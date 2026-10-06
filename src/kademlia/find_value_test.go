package kademlia

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func TestLookupDataMultihopAndMissing(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	config := DefaultConfig()
	a := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9600"), config)
	b := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9601"), config)
	c := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9602"), config)
	seedContact(t, a, b.Contact())
	seedContact(t, b, c.Contact())
	// This test isolates lookup caching from the separate new-peer handoff.
	// Observe peers while stores are empty, then wait for those transfers to finish.
	for _, holder := range []*Kademlia{a, b, c} {
		for _, peer := range []*Kademlia{a, b, c} {
			holder.network.handoffs.observe(peer.Contact(), holder.me.ID)
		}
		waitUntil(t, func() bool {
			holder.network.handoffs.mu.Lock()
			defer holder.network.handoffs.mu.Unlock()
			return len(holder.network.handoffs.pending) == 0
		})
	}
	for _, data := range [][]byte{nil, {0, 1, 255, 2}} {
		key := KademliaID(sha256.Sum256(data))
		if err := c.dataStore.put(key, data); err != nil {
			t.Fatal(err)
		}
		result, err := a.LookupData(context.Background(), key.String())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(result.Data, data) || !result.Source.ID.Equals(c.me.ID) {
			t.Fatal("incorrect value or source")
		}
		if _, found := a.dataStore.get(key); found {
			t.Fatal("unexpected caching")
		}
	}
	missing := KademliaID(sha256.Sum256([]byte("absent")))
	if _, err := a.LookupData(context.Background(), missing.String()); !errors.Is(err, ErrValueNotFound) {
		t.Fatal(err)
	}
}

func TestLookupDataLocalAndLifecycle(t *testing.T) {
	node, err := NewKademlia("127.0.0.1:9610", DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	data := []byte("local")
	key := KademliaID(sha256.Sum256(data))
	if err := node.dataStore.put(key, data); err != nil {
		t.Fatal(err)
	}
	got, err := node.LookupData(context.Background(), key.String())
	if err != nil || !got.Source.ID.Equals(node.me.ID) {
		t.Fatal(err)
	}
	got.Data[0] = 'X'
	got, err = node.LookupData(context.Background(), key.String())
	if err != nil || !bytes.Equal(got.Data, data) {
		t.Fatal("local data exposed")
	}
	if _, err := node.LookupData(context.Background(), "invalid"); err == nil {
		t.Fatal("accepted malformed key")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := node.LookupData(ctx, key.String()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	missing := KademliaID(sha256.Sum256(nil))
	if _, err := node.LookupData(context.Background(), missing.String()); err == nil {
		t.Fatal("offline miss succeeded")
	}
	// Simulate corruption bypassing the normal, hash-checked write path.
	node.dataStore.mu.Lock()
	node.dataStore.values[key] = []byte("corrupt")
	node.dataStore.mu.Unlock()
	if _, err := node.LookupData(context.Background(), key.String()); !errors.Is(err, ErrHashMismatch) {
		t.Fatal(err)
	}
	node.Close()
	if _, err := node.LookupData(context.Background(), key.String()); !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
}

func TestFindValueRejectsCorruptionAndLookupContinues(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	config := DefaultConfig()
	a := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9620"), config)
	bad := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9621"), config)
	good := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9622"), config)
	data := []byte("verified")
	key := KademliaID(sha256.Sum256(data))
	bad.dataStore.mu.Lock()
	bad.dataStore.values[key] = []byte("forged")
	bad.dataStore.mu.Unlock()
	peer := bad.Contact()
	if _, _, _, err := a.network.SendFindDataMessage(context.Background(), &peer, &key); !errors.Is(err, ErrHashMismatch) {
		t.Fatal(err)
	}
	if err := good.dataStore.put(key, data); err != nil {
		t.Fatal(err)
	}
	seedContact(t, a, good.Contact())
	result, err := a.LookupData(context.Background(), key.String())
	if err != nil || !bytes.Equal(result.Data, data) || !result.Source.ID.Equals(good.me.ID) {
		t.Fatalf("failed to recover: %v", err)
	}
	requireNoPending(t, a.network)
}

func TestLookupDataStopsOutstandingProbes(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	config := DefaultConfig()
	config.RPCTimeout = time.Hour
	a := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9630"), config)
	slow := testEndpoint(t, sim, "127.0.0.1:9631")
	fast := testEndpoint(t, sim, "127.0.0.1:9632")
	seedContact(t, a, rpcContact(slow.LocalAddr()))
	seedContact(t, a, rpcContact(fast.LocalAddr()))
	data := []byte("fast reply")
	key := KademliaID(sha256.Sum256(data))
	done := make(chan error, 1)
	go func() { _, err := a.LookupData(context.Background(), key.String()); done <- err }()
	readRPC(t, slow) // Both requests must be in flight.
	request := readRPC(t, fast)
	found := true
	request.Payload, _ = json.Marshal(findValueResponse{Found: &found, Data: data})
	request.Response = true
	request.Sender = rpcContact(fast.LocalAddr())
	sendRPC(t, fast, a.me.Address, request)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("waited for slow peer after finding value")
	}
	requireNoPending(t, a.network)
	// Cancellation also interrupts an unsuccessful lookup.
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _, err := a.LookupData(ctx, key.String()); done <- err }()
	readRPC(t, slow)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("lookup ignored cancellation")
	}
	requireNoPending(t, a.network)
}

func TestStoreThenLookupDataUDPConcurrent(t *testing.T) {
	config := DefaultConfig()
	config.MaxValueSize = MaxSupportedValueSize
	publisher := testRPCNode(t, testUDP(t), config)
	reader := testRPCNode(t, testUDP(t), config)
	seedContact(t, reader, publisher.Contact())
	data := bytes.Repeat([]byte{0, 255}, MaxSupportedValueSize/2)
	key, err := publisher.Store(context.Background(), data)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := reader.LookupData(context.Background(), key.String())
			if err != nil {
				t.Error(err)
				return
			}
			if !bytes.Equal(result.Data, data) || !result.Source.ID.Equals(publisher.me.ID) {
				t.Error("incorrect UDP result")
			}
		}()
	}
	wg.Wait()
	requireNoPending(t, reader.network)
}

func TestFindValueMalformedResponses(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	a := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9640"), DefaultConfig())
	endpoint := testEndpoint(t, sim, "127.0.0.1:9641")
	peer := rpcContact(endpoint.LocalAddr())
	key := KademliaID(sha256.Sum256(nil))
	if _, _, _, err := a.network.SendFindDataMessage(context.Background(), &peer, nil); err == nil {
		t.Fatal("accepted nil key")
	}
	for _, payload := range []string{
		`"bad"`,
		`{}`,
		`{"found":true,"data":null}`,
		`{"found":true,"data":"","contacts":[]}`,
		`{"found":false,"contacts":null}`,
		`{"found":false,"data":"","contacts":[]}`,
		`{"found":false,"contacts":[{"ID":null,"Address":"127.0.0.1:1"}]}`,
	} {
		done := make(chan error, 1)
		go func() { _, _, _, err := a.network.SendFindDataMessage(context.Background(), &peer, &key); done <- err }()
		request := readRPC(t, endpoint)
		request.Response = true
		request.Sender = peer
		request.Payload = json.RawMessage(payload)
		sendRPC(t, endpoint, a.me.Address, request)
		if err := <-done; err == nil {
			t.Fatalf("accepted malformed response %s", payload)
		}
	}
	for _, payload := range []string{`"bad"`, `{}`, `{"target":"bad"}`} {
		if _, err := a.network.findValuePayload(json.RawMessage(payload), peer.ID); err == nil {
			t.Fatal("accepted malformed request")
		}
	}
	// A sender may have a larger configured value limit than its client.
	config := DefaultConfig()
	config.MaxValueSize = 2048
	large := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9642"), config)
	data := make([]byte, 1025)
	key = KademliaID(sha256.Sum256(data))
	if err := large.dataStore.put(key, data); err != nil {
		t.Fatal(err)
	}
	peer = large.Contact()
	if _, _, _, err := a.network.SendFindDataMessage(context.Background(), &peer, &key); !errors.Is(err, ErrValueTooLarge) {
		t.Fatal(err)
	}
}
