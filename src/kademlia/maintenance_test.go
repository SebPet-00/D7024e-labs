package kademlia

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

type refreshRecorder struct {
	Transport
	mu      sync.Mutex
	targets []KademliaID
}

func (recorder *refreshRecorder) Send(to string, data []byte) error {
	var message rpcMessage
	if json.Unmarshal(data, &message) == nil && message.Type == rpcFindNode && !message.Response {
		var request findNodeRequest
		if json.Unmarshal(message.Payload, &request) == nil {
			if id, err := NewKademliaID(request.Target); err == nil {
				recorder.mu.Lock()
				recorder.targets = append(recorder.targets, *id)
				recorder.mu.Unlock()
			}
		}
	}
	return recorder.Transport.Send(to, data)
}
func (recorder *refreshRecorder) snapshot() []KademliaID {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]KademliaID(nil), recorder.targets...)
}

func TestJoinRefreshesFartherBuckets(t *testing.T) {
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	recorder := &refreshRecorder{Transport: testEndpoint(t, simulation, "127.0.0.1:8000")}
	joining := testRPCNode(t, recorder, DefaultConfig())
	contact := sameBucketContacts(t, joining.routingTable, 4, 1)[0]
	bootstrap := testRPCNode(t, testEndpoint(t, simulation, contact.Address), DefaultConfig())
	if err := joining.Join(context.Background(), bootstrap.me.Address); err != nil {
		t.Fatal(err)
	}
	targets := recorder.snapshot()
	if len(targets) != 5 || targets[0] != *joining.me.ID {
		t.Fatalf("join must perform own-ID lookup plus four refreshes, got %d lookups", len(targets))
	}
	for i, target := range targets[1:] {
		if got, want := joining.routingTable.getBucketIndex(&target), 3-i; got != want {
			t.Fatalf("refresh %d targeted bucket %d, want %d", i, got, want)
		}
	}
	if !hasContact(bootstrap.routingTable, joining.Contact()) || !hasContact(joining.routingTable, bootstrap.Contact()) {
		t.Fatal("joining did not establish mutual contacts")
	}
}

func TestJoinNetworkWithoutManualSeeding(t *testing.T) {
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	config := DefaultConfig()
	config.K, config.Alpha = 4, 2
	nodes := make([]*Kademlia, 24)
	for i := range nodes {
		nodes[i] = testRPCNode(t, testEndpoint(t, simulation, fmt.Sprintf("127.0.0.1:%d", 22000+i)), config)
	}
	// Start with a single bootstrap, then exercise ordinary and simultaneous joins.
	for i := 1; i < 12; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := nodes[i].Join(ctx, nodes[0].me.Address)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	var workers sync.WaitGroup
	for i := 12; i < len(nodes); i++ {
		workers.Add(1)
		go func(node *Kademlia) {
			defer workers.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := node.Join(ctx, nodes[0].me.Address); err != nil {
				t.Error(err)
			}
		}(nodes[i])
	}
	workers.Wait()
	for _, pair := range [][2]int{{0, 23}, {15, 2}, {23, 12}} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		got, err := nodes[pair[0]].LookupContact(ctx, nodes[pair[1]].me.ID)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if !got[0].ID.Equals(nodes[pair[1]].me.ID) {
			t.Fatalf("joined node %d was not discoverable", pair[1])
		}
	}
}

func TestJoinUDP(t *testing.T) {
	bootstrap := testRPCNode(t, testUDP(t), DefaultConfig())
	joining := testRPCNode(t, testUDP(t), DefaultConfig())
	if err := joining.Join(context.Background(), bootstrap.me.Address); err != nil {
		t.Fatal(err)
	}
	got, err := bootstrap.LookupContact(context.Background(), joining.me.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got[0].ID.Equals(joining.me.ID) {
		t.Fatal("UDP joiner was not discoverable")
	}
}

func TestJoinErrors(t *testing.T) {
	config := DefaultConfig()
	config.RPCTimeout, config.RPCRetries = 20*time.Millisecond, 0
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	node := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), config)
	for _, address := range []string{"invalid", node.me.Address} {
		if err := node.Join(context.Background(), address); err == nil {
			t.Fatal("invalid bootstrap accepted")
		}
	}
	missing := rpcContact("127.0.0.1:8100")
	if err := node.Join(context.Background(), missing.Address); !errors.Is(err, ErrRPCTimeout) {
		t.Fatal(err)
	}
	if hasContact(node.routingTable, missing) {
		t.Fatal("failed bootstrap left a stale seed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := node.Join(ctx, missing.Address); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	node.Close()
	if err := node.Join(context.Background(), missing.Address); !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	offline, err := NewKademlia("127.0.0.1:9000", config)
	if err != nil {
		t.Fatal(err)
	}
	defer offline.Close()
	if err := offline.Join(context.Background(), missing.Address); err == nil {
		t.Fatal("offline join accepted")
	}
}

func TestRefreshOnlyStaleRanges(t *testing.T) {
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	recorder := &refreshRecorder{Transport: testEndpoint(t, simulation, "127.0.0.1:8000")}
	node := testRPCNode(t, recorder, DefaultConfig())
	remote := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8100"), DefaultConfig())
	seedContact(t, node, remote.Contact())
	now := time.Now()
	node.routingTable.mu.Lock()
	for _, bucket := range node.routingTable.buckets {
		bucket.lastLookup = now
	}
	node.routingTable.buckets[5].lastLookup = now.Add(-2 * time.Hour)
	node.routingTable.buckets[18].lastLookup = now.Add(-2 * time.Hour)
	node.routingTable.mu.Unlock()
	if err := node.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	targets := recorder.snapshot()
	if len(targets) != 2 {
		t.Fatalf("got %d refresh lookups, want 2", len(targets))
	}
	for i, index := range []int{5, 18} {
		if node.routingTable.getBucketIndex(&targets[i]) != index {
			t.Fatal("wrong refresh range")
		}
	}
	if err := node.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(recorder.snapshot()) != 2 {
		t.Fatal("fresh buckets were unnecessarily refreshed")
	}
}

func TestPeriodicRefreshAndShutdown(t *testing.T) {
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	config := DefaultConfig()
	config.RefreshPeriod = 30 * time.Millisecond
	recorder := &refreshRecorder{Transport: testEndpoint(t, simulation, "127.0.0.1:8000")}
	node := testRPCNode(t, recorder, config)
	// A raw peer keeps refresh waiting for its first response, so shutdown
	// must interrupt an in-progress lookup rather than just a sleeping ticker.
	peer := testEndpoint(t, simulation, "127.0.0.1:8100")
	seedContact(t, node, rpcContact(peer.LocalAddr()))
	request := readRPC(t, peer)
	if request.Type != rpcFindNode {
		t.Fatal("periodic refresh did not start lookup")
	}
	closed := make(chan struct{})
	go func() { node.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("shutdown waited for RPC timeout")
	}
	select {
	case <-node.refreshStopped:
	default:
		t.Fatal("refresh worker still running")
	}
	requireNoPending(t, node.network)
}

func TestRefreshEmptyFailureAndCancellation(t *testing.T) {
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	config := DefaultConfig()
	config.RPCTimeout, config.RPCRetries = 20*time.Millisecond, 0
	node := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), config)
	if err := node.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	seedContact(t, node, rpcContact("127.0.0.1:8100"))
	node.routingTable.mu.Lock()
	node.routingTable.buckets[0].lastLookup = time.Time{}
	node.routingTable.mu.Unlock()
	if err := node.Refresh(context.Background()); !errors.Is(err, ErrNoReachableContacts) {
		t.Fatal(err)
	}
	// Waiting behind another maintenance operation must remain cancellable.
	node.maintenanceGate <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := node.Refresh(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	<-node.maintenanceGate
	if err := node.refreshBucket(context.Background(), -1); err == nil {
		t.Fatal("invalid refresh range accepted")
	}
}
