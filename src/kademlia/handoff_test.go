package kademlia

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"
)

func handoffValue(near, far Contact) ([]byte, KademliaID) {
	for i := 0; ; i++ {
		data := []byte(fmt.Sprintf("join handoff %d", i))
		key := KademliaID(sha256.Sum256(data))
		if near.ID.CalcDistance(&key).Less(far.ID.CalcDistance(&key)) {
			return data, key
		}
	}
}

func TestJoinTransfersExistingValue(t *testing.T) {
	for _, k := range []int{1, 2} {
		t.Run(fmt.Sprintf("k%d", k), func(t *testing.T) {
			sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
			config := DefaultConfig()
			config.K = k
			// No explicit Replicate and no periodic pass can repair the regression.
			config.ReplicationPeriod = 24 * time.Hour
			holder := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:31000"), config)
			joining := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:31001"), config)
			data, key := handoffValue(joining.Contact(), holder.Contact())
			if _, err := holder.Store(context.Background(), data); err != nil {
				t.Fatal(err)
			}
			if err := joining.Join(context.Background(), holder.Contact().Address); err != nil {
				t.Fatal(err)
			}
			waitUntil(t, func() bool { got, found := joining.dataStore.get(key); return found && bytes.Equal(got, data) })
			if got, found := holder.dataStore.get(key); !found || !bytes.Equal(got, data) {
				t.Fatal("original copy removed")
			}
			// A separate reader must retrieve through routing, not just local storage.
			var reader *Kademlia
			for port := 34000; port < 35000; port++ {
				address := fmt.Sprintf("127.0.0.1:%d", port)
				candidate := rpcContact(address)
				if joining.me.ID.CalcDistance(&key).Less(candidate.ID.CalcDistance(&key)) {
					reader = testRPCNode(t, testEndpoint(t, sim, address), config)
					break
				}
			}
			if reader == nil {
				t.Fatal("could not select reader")
			}
			if err := reader.Join(context.Background(), joining.Contact().Address); err != nil {
				t.Fatal(err)
			}
			holder.Close()
			remoteResult, err := reader.LookupData(context.Background(), key.String())
			if err != nil || !bytes.Equal(remoteResult.Data, data) {
				t.Fatalf("reader could not retrieve after join: %v", err)
			}
			result, err := joining.LookupData(context.Background(), key.String())
			if err != nil || !bytes.Equal(result.Data, data) {
				t.Fatalf("joiner lost value after original holder left: %v", err)
			}
		})
	}
}

func TestJoinTransfersOverUDP(t *testing.T) {
	config := DefaultConfig()
	config.K = 1
	holder := testRPCNode(t, testUDP(t), config)
	joining := testRPCNode(t, testUDP(t), config)
	data, key := handoffValue(joining.Contact(), holder.Contact())
	if _, err := holder.Store(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	if err := joining.Join(context.Background(), holder.Contact().Address); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { got, found := joining.dataStore.get(key); return found && bytes.Equal(got, data) })
}

func TestHandoffResponsibilityAndSenderElection(t *testing.T) {
	contact := func(last byte) Contact { id := KademliaID{}; id[IDLength-1] = last; return NewContact(&id, "unused") }
	key := KademliaID{}
	self, peer, other := contact(2), contact(1), contact(3)
	if !handoffEligible(self, peer, &key, []Contact{peer, other}, 1) {
		t.Fatal("closest joiner skipped")
	}
	if handoffEligible(self, other, &key, []Contact{other}, 1) {
		t.Fatal("non-responsible peer selected")
	}
	if !handoffEligible(self, other, &key, []Contact{other, other}, 2) {
		t.Fatal("second holder or duplicate handling wrong")
	}
	if handoffEligible(other, peer, &key, []Contact{self, peer}, 2) {
		t.Fatal("farther existing holder sent redundant copy")
	}
}

func TestHandoffQueueDeduplicatesAndBoundsHistory(t *testing.T) {
	queue := newHandoffQueue()
	self := &KademliaID{}
	queue.observe(Contact{}, self)
	queue.observe(NewContact(self, "self"), self)
	peer := rpcContact("127.0.0.1:31001")
	for i := 0; i < 10; i++ {
		queue.observe(peer, self)
	}
	if len(queue.inbox) != 1 {
		t.Fatal("repeated observation queued duplicate")
	}
	<-queue.inbox
	delete(queue.pending, *peer.ID)
	for i := 1; i <= handoffHistorySize+1; i++ {
		id := KademliaID(sha256.Sum256([]byte(fmt.Sprint(i))))
		queue.observe(NewContact(&id, "unused"), self)
		queued := <-queue.inbox
		delete(queue.pending, *queued.ID)
	}
	if len(queue.recent) != handoffHistorySize || queue.order.Len() != handoffHistorySize {
		t.Fatal("unbounded history")
	}
	for i := 0; i < handoffQueueSize; i++ {
		queue.observe(rpcContact(fmt.Sprintf("127.0.0.1:%d", 40000+i)), self)
	}
	dropped := rpcContact("127.0.0.1:45000")
	queue.observe(dropped, self)
	if queue.recent[*dropped.ID] != nil {
		t.Fatal("full queue marked peer as handled")
	}
	<-queue.inbox
	queue.observe(dropped, self)
	if queue.recent[*dropped.ID] == nil {
		t.Fatal("later observation could not retry queue admission")
	}
}

func TestHandoffShutdownDuringStore(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	config := DefaultConfig()
	config.K = 1
	config.RPCTimeout = time.Hour
	holder := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:32000"), config)
	remote := testEndpoint(t, sim, "127.0.0.1:32001")
	peer := rpcContact(remote.LocalAddr())
	data, _ := handoffValue(peer, holder.Contact())
	if _, err := holder.Store(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	holder.network.observeContact(peer)
	if request := readRPC(t, remote); request.Type != rpcStore {
		t.Fatal("no join transfer")
	}
	// A blocked transfer must not keep storage locked.
	done := make(chan struct{})
	go func() { holder.DataSnapshot(); holder.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handoff blocked shutdown or held storage lock")
	}
	select {
	case <-holder.network.handoffs.stopped:
	default:
		t.Fatal("transfer worker still running")
	}
	requireNoPending(t, holder.network)
}

func TestHandoffFailureRetainsValue(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	config := DefaultConfig()
	config.K = 1
	config.RPCTimeout = 10 * time.Millisecond
	config.RPCRetries = 0
	holder := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:33000"), config)
	peer := rpcContact("127.0.0.1:33001")
	data, key := handoffValue(peer, holder.Contact())
	if _, err := holder.Store(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	holder.network.observeContact(peer)
	waitUntil(t, func() bool {
		holder.network.handoffs.mu.Lock()
		defer holder.network.handoffs.mu.Unlock()
		return !holder.network.handoffs.pending[*peer.ID]
	})
	if got, found := holder.dataStore.get(key); !found || !bytes.Equal(got, data) {
		t.Fatal("failed transfer deleted value")
	}
	// Periodic replication can recover even though observation was deduplicated.
	joining := testRPCNode(t, testEndpoint(t, sim, peer.Address), config)
	seedContact(t, holder, joining.Contact())
	if err := holder.Replicate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, found := joining.dataStore.get(key); !found || !bytes.Equal(got, data) {
		t.Fatal("replication did not repair failed handoff")
	}
}

func TestHandoffSequentialJoins(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	config := DefaultConfig()
	config.K = 2
	config.ReplicationPeriod = 24 * time.Hour
	holder := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:35000"), config)
	first := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:35001"), config)
	data, key := handoffValue(first.Contact(), holder.Contact())
	if _, err := holder.Store(context.Background(), data); err != nil {
		t.Fatal(err)
	}
	if err := first.Join(context.Background(), holder.Contact().Address); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { _, found := first.dataStore.get(key); return found })
	var next *Kademlia
	for port := 35002; port < 36000; port++ {
		address := fmt.Sprintf("127.0.0.1:%d", port)
		candidate := rpcContact(address)
		if candidate.ID.CalcDistance(&key).Less(first.me.ID.CalcDistance(&key)) {
			next = testRPCNode(t, testEndpoint(t, sim, address), config)
			break
		}
	}
	if next == nil {
		t.Fatal("could not select closer joiner")
	}
	if err := next.Join(context.Background(), first.Contact().Address); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { got, found := next.dataStore.get(key); return found && bytes.Equal(got, data) })
	holder.Close()
	first.Close()
	got, err := next.LookupData(context.Background(), key.String())
	if err != nil || !bytes.Equal(got.Data, data) {
		t.Fatalf("second joiner lost value: %v", err)
	}
}
