package kademlia

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestReplicationRepairsAfterChurn(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	config := DefaultConfig()
	config.K = 2
	config.RPCTimeout = 10 * time.Millisecond
	config.RPCRetries = 0
	a := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9700"), config)
	b := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9701"), config)
	seedContact(t, a, b.Contact())
	data := []byte("survives churn")
	key, err := a.Store(context.Background(), data)
	if err != nil {
		t.Fatal(err)
	}
	a.Close()
	c := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9702"), config)
	seedContact(t, b, c.Contact())
	if err := b.Replicate(context.Background()); err != nil {
		t.Fatal(err)
	}
	b.Close()
	result, err := c.LookupData(context.Background(), key.String())
	if err != nil || !bytes.Equal(result.Data, data) {
		t.Fatalf("replacement lost value: %v", err)
	}
}

func TestReplicationRetainsOldCopy(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	config := DefaultConfig()
	config.K = 1
	a := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9710"), config)
	b := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9711"), config)
	var data []byte
	var key KademliaID
	for i := 0; ; i++ {
		data = []byte(fmt.Sprintf("closer replacement %d", i))
		key = KademliaID(sha256.Sum256(data))
		if b.me.ID.CalcDistance(&key).Less(a.me.ID.CalcDistance(&key)) {
			break
		}
	}
	if _, err := a.Store(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	seedContact(t, a, b.Contact())
	if err := a.Replicate(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, node := range []*Kademlia{a, b} {
		got, found := node.dataStore.get(key)
		if !found || !bytes.Equal(got, data) {
			t.Fatal("new or existing copy missing")
		}
	}
}

func TestReplicationContinuesAfterFailure(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	config := DefaultConfig()
	a := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9720"), config)
	config.MaxValueSize = 255
	b := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9721"), config)
	seedContact(t, a, b.Contact())
	small := []byte("valid")
	smallKey := KademliaID(sha256.Sum256(small))
	large := make([]byte, 256)
	largeKey := KademliaID(sha256.Sum256(large))
	corruptKey := KademliaID(sha256.Sum256([]byte("original")))
	if err := a.dataStore.put(smallKey, small); err != nil {
		t.Fatal(err)
	}
	if err := a.dataStore.put(largeKey, large); err != nil {
		t.Fatal(err)
	}
	a.dataStore.mu.Lock()
	a.dataStore.values[corruptKey] = []byte("corrupt")
	a.dataStore.mu.Unlock()
	err := a.Replicate(context.Background())
	if !errors.Is(err, ErrValueTooLarge) || !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("missing failures: %v", err)
	}
	if got, found := b.dataStore.get(smallKey); !found || !bytes.Equal(got, small) {
		t.Fatal("valid value skipped")
	}
	wrongKey := KademliaID(sha256.Sum256([]byte("corrupt")))
	if _, found := b.dataStore.get(wrongKey); found {
		t.Fatal("corrupt data republished under a new key")
	}
	if _, found := a.dataStore.get(largeKey); !found {
		t.Fatal("failed write removed original")
	}
}

func TestPeriodicReplicationAndClose(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	config := DefaultConfig()
	config.ReplicationPeriod = 10 * time.Millisecond
	a := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9730"), config)
	b := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9731"), DefaultConfig())
	key, err := a.Store(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	seedContact(t, a, b.Contact())
	waitUntil(t, func() bool { _, found := b.dataStore.get(*key); return found })
	a.Close()
	select {
	case <-a.replicationStopped:
	default:
		t.Fatal("replication worker still running")
	}
	if err := a.Replicate(context.Background()); !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
}

func TestReplicationShutdownDuringLookup(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	config := DefaultConfig()
	config.ReplicationPeriod = 10 * time.Millisecond
	config.RPCTimeout = time.Hour
	a := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9740"), config)
	if _, err := a.Store(context.Background(), []byte("pending")); err != nil {
		t.Fatal(err)
	}
	peer := testEndpoint(t, sim, "127.0.0.1:9741")
	seedContact(t, a, rpcContact(peer.LocalAddr()))
	readRPC(t, peer) // A periodic pass is now waiting for this peer.
	// The pass must not retain the data-store lock during network I/O.
	wrote := make(chan error, 1)
	go func() {
		data := []byte("concurrent")
		key := KademliaID(sha256.Sum256(data))
		wrote <- a.dataStore.put(key, data)
	}()
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("replication held storage lock")
	}
	closed := make(chan struct{})
	go func() { a.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("shutdown waited for RPC timeout")
	}
	requireNoPending(t, a.network)
}

func TestReplicationSnapshotAndCancellation(t *testing.T) {
	store := newValueStore(1024)
	data := []byte("snapshot")
	key := KademliaID(sha256.Sum256(data))
	if err := store.put(key, data); err != nil {
		t.Fatal(err)
	}
	snapshot := store.snapshot()
	snapshot[key][0] = 'X'
	delete(snapshot, key)
	if got, found := store.get(key); !found || !bytes.Equal(got, data) {
		t.Fatal("snapshot exposed storage")
	}
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	node := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9750"), DefaultConfig())
	if err := node.Replicate(context.Background()); err != nil {
		t.Fatal(err)
	}
	node.maintenanceGate <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := node.Replicate(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	<-node.maintenanceGate
	offline, err := NewKademlia("127.0.0.1:9751", DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer offline.Close()
	if err := offline.Replicate(context.Background()); err == nil {
		t.Fatal("offline replication accepted")
	}
}
