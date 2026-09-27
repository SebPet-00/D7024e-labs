package kademlia

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestValueStoreOwnershipAndValidation(t *testing.T) {
	s := newValueStore(255)
	data := bytes.Repeat([]byte{7}, 255)
	key := KademliaID(sha256.Sum256(data))
	if err := s.put(key, data); err != nil {
		t.Fatal(err)
	}
	data[0] = 8
	got, found := s.get(key)
	if !found || got[0] != 7 {
		t.Fatal("store retained caller's slice")
	}
	got[0] = 9
	got, _ = s.get(key)
	if got[0] != 7 {
		t.Fatal("get exposed stored slice")
	}
	if err := s.put(key, data); !errors.Is(err, ErrHashMismatch) {
		t.Fatal(err)
	}
	if err := s.put(key, make([]byte, 256)); !errors.Is(err, ErrValueTooLarge) {
		t.Fatal(err)
	}
	empty := KademliaID(sha256.Sum256(nil))
	if err := s.put(empty, nil); err != nil {
		t.Fatal(err)
	}
	if value, ok := s.get(empty); !ok || len(value) != 0 {
		t.Fatal("empty value missing")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if err := s.put(empty, nil); err != nil {
					t.Error(err)
				}
				s.get(key)
			}
		}()
	}
	wg.Wait()
	if len(s.values) != 2 {
		t.Fatal("repeated STORE created extra entries")
	}
}

func TestStoreClosestPlacement(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	config := DefaultConfig()
	config.K = 2
	nodes := make([]*Kademlia, 3)
	contacts := make([]Contact, 3)
	for i := range nodes {
		nodes[i] = testRPCNode(t, testEndpoint(t, sim, fmt.Sprintf("127.0.0.1:%d", 9200+i)), config)
		contacts[i] = nodes[i].Contact()
	}
	for _, node := range nodes {
		for _, peer := range contacts {
			seedContact(t, node, peer)
		}
	}
	// All three are discoverable; select only the two closest, exercising both
	// local inclusion and exclusion over several different content hashes.

	included, excluded := false, false
	for i := 0; i < 20; i++ {
		data := []byte(fmt.Sprintf("placement %d", i))
		key, err := nodes[0].Store(context.Background(), data)
		if err != nil {
			t.Fatal(err)
		}
		want := contactsByDistance(contacts, key)[:2]
		for _, node := range nodes {
			expected := node.me.ID.Equals(want[0].ID) || node.me.ID.Equals(want[1].ID)
			got, found := node.dataStore.get(*key)
			if found != expected || (found && !bytes.Equal(got, data)) {
				t.Fatalf("incorrect placement at %s", node.me.Address)
			}
			if node == nodes[0] {
				included = included || found
				excluded = excluded || !found
			}
		}
	}
	if !included || !excluded {
		t.Fatal("fixture did not exercise both publisher placements")
	}
}

func TestStorePartialFailureAndSingleton(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	config := DefaultConfig()
	a := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9300"), config)
	key, err := a.Store(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := a.dataStore.get(*key); !ok {
		t.Fatal("isolated node did not store")
	}
	config.MaxValueSize = 255
	b := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9301"), config)
	seedContact(t, a, b.Contact())
	data := make([]byte, 256)
	key, err = a.Store(context.Background(), data)
	if key == nil || !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("expected partial size failure, got %v", err)
	}
	if _, ok := a.dataStore.get(*key); !ok {
		t.Fatal("successful local write missing")
	}
	if _, ok := b.dataStore.get(*key); ok {
		t.Fatal("oversized value stored")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.Store(ctx, data); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := a.Store(context.Background(), make([]byte, 1025)); !errors.Is(err, ErrValueTooLarge) {
		t.Fatal(err)
	}
	a.Close()
	if _, err := a.Store(context.Background(), data); err == nil {
		t.Fatal("closed node accepted STORE")
	}
}

func TestStoreReceiverValidation(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	a := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9400"), DefaultConfig())
	b := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9401"), DefaultConfig())
	peer := b.Contact()
	key := KademliaID(sha256.Sum256([]byte("right")))
	payload, _ := json.Marshal(storeRequest{Key: key.String(), Data: []byte("wrong")})
	response, err := a.network.call(context.Background(), &peer, rpcStore, payload)
	if err != nil {
		t.Fatal(err)
	}
	var ack storeResponse
	if err := json.Unmarshal(response.Payload, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Stored || ack.Error != storeHashMismatch {
		t.Fatalf("invalid rejection: %+v", ack)
	}
	if _, ok := b.dataStore.get(key); ok {
		t.Fatal("invalid hash stored")
	}
	for _, payload := range []string{
		"{}",
		`{"key":"bad","data":""}`,
		fmt.Sprintf(`{"key":%q}`, key.String()),
		fmt.Sprintf(`{"key":%q,"data":null}`, key.String()),
		fmt.Sprintf(`{"key":%q,"data":"!"}`, key.String()),
	} {
		if _, err := b.network.storePayload(json.RawMessage(payload)); err == nil {
			t.Fatalf("accepted malformed payload %s", payload)
		}
	}
	if err := a.network.SendStoreMessage(context.Background(), &peer, &key, []byte("wrong")); !errors.Is(err, ErrHashMismatch) {
		t.Fatal(err)
	}
	if err := a.network.SendStoreMessage(context.Background(), &peer, nil, nil); err == nil {
		t.Fatal("accepted nil key")
	}
}

func TestStoreUDPMaximumAndEmpty(t *testing.T) {
	config := DefaultConfig()
	config.MaxValueSize = MaxSupportedValueSize
	a := testRPCNode(t, testUDP(t), config)
	b := testRPCNode(t, testUDP(t), config)
	peer := b.Contact()
	for _, data := range [][]byte{{}, bytes.Repeat([]byte{0, 255}, MaxSupportedValueSize/2)} {
		key := KademliaID(sha256.Sum256(data))
		for attempt := 0; attempt < 2; attempt++ {
			if err := a.network.SendStoreMessage(context.Background(), &peer, &key, data); err != nil {
				t.Fatal(err)
			}
		}
		got, found := b.dataStore.get(key)
		if !found || !bytes.Equal(got, data) {
			t.Fatal("UDP value differs")
		}
	}
	config.MaxValueSize++
	if err := config.validate(); err == nil {
		t.Fatal("accepted value limit beyond UDP ceiling")
	}
}

func TestStoreAcknowledgmentValidationAndCancellation(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	a := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9500"), DefaultConfig())
	endpoint := testEndpoint(t, sim, "127.0.0.1:9501")
	peer := rpcContact(endpoint.LocalAddr())
	data := []byte("acknowledge")
	key := KademliaID(sha256.Sum256(data))
	payloads := []string{
		"{",
		`{"key":"wrong","stored":true}`,
		fmt.Sprintf(`{"key":%q,"stored":true,"error":"unexpected"}`, key.String()),
		fmt.Sprintf(`{"key":%q,"stored":false}`, key.String()),
		fmt.Sprintf(`{"key":%q,"stored":false,"error":"unknown"}`, key.String()),
	}
	for _, payload := range payloads {
		result := make(chan error, 1)
		go func() { result <- a.network.SendStoreMessage(context.Background(), &peer, &key, data) }()
		request := readRPC(t, endpoint)
		request.Response = true
		request.Sender = peer
		// Invalid JSON must still be a valid envelope; use a JSON string here.
		if payload == "{" {
			request.Payload = json.RawMessage(`"invalid"`)
		} else {
			request.Payload = json.RawMessage(payload)
		}
		sendRPC(t, endpoint, a.me.Address, request)
		if err := <-result; err == nil {
			t.Fatalf("accepted invalid acknowledgment %s", payload)
		}
		requireNoPending(t, a.network)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- a.network.SendStoreMessage(ctx, &peer, &key, data) }()
	readRPC(t, endpoint)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	requireNoPending(t, a.network)
}
