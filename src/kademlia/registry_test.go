package kademlia

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func registryConfig() (Config, ed25519.PrivateKey) {
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, 32))
	c := DefaultConfig()
	c.Owners = Ownership{"example.test": key.Public().(ed25519.PublicKey)}
	c.RPCTimeout = 200 * time.Millisecond
	c.RPCRetries = 1
	return c, key
}
func registryNode(t *testing.T, sim *SimulatedNetwork, port int, c Config) *Kademlia {
	return testRPCNode(t, testEndpoint(t, sim, fmt.Sprintf("127.0.0.1:%d", port)), c)
}
func mustPublish(t *testing.T, n *Kademlia, key ed25519.PrivateKey, v uint64) {
	t.Helper()
	if e := n.Registry().Publish(context.Background(), "example.test", "demo", v, []byte(fmt.Sprintf("payload %d", v)), key, PublishOptions{}); e != nil {
		t.Fatal(e)
	}
}
func localHead(t *testing.T, n *Kademlia) PackageRecord {
	t.Helper()
	for _, h := range n.registry.snapshot() {
		if h.Package == "demo" {
			return h
		}
	}
	t.Fatal("missing head")
	return PackageRecord{}
}
func makeVersion(t *testing.T, n *Kademlia, key ed25519.PrivateKey, v uint64, prev string) PackageRecord {
	t.Helper()
	blob, e := n.Store(context.Background(), []byte(fmt.Sprint(v)))
	if e != nil {
		t.Fatal(e)
	}
	record := signRecord(PackageRecord{Tag: "version-record", Domain: "example.test", Package: "demo", Version: v, BlobHash: blob.String(), Previous: prev}, key)
	hash, e := n.Store(context.Background(), recordBytes(record))
	if e != nil {
		t.Fatal(e)
	}
	return signRecord(PackageRecord{Tag: "latest-pointer", Domain: record.Domain, Package: record.Package, Version: v, RecordHash: hash.String()}, key)
}
func TestRegistryPublishInstallAndRejections(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	c, key := registryConfig()
	a := registryNode(t, sim, 34000, c)
	b := registryNode(t, sim, 34001, c)
	if e := b.Join(context.Background(), a.Contact().Address); e != nil {
		t.Fatal(e)
	}
	mustPublish(t, a, key, 1)
	mustPublish(t, a, key, 2)
	for _, version := range []string{"1", "2", "latest"} {
		got, e := b.registry.Install(context.Background(), "example.test", "demo", version)
		want := "payload " + version
		if version == "latest" {
			want = "payload 2"
		}
		if e != nil || string(got.Data) != want {
			t.Fatalf("%s: %v %v", version, got, e)
		}
	}
	history, e := b.registry.History(context.Background(), "example.test", "demo")
	if e != nil || len(history) != 2 || history[1].Previous != zeroHash {
		t.Fatal(history, e)
	}
	old := uint64(0)
	attempts := []struct {
		v uint64
		k ed25519.PrivateKey
		o PublishOptions
	}{
		{2, key, PublishOptions{}},
		{3, key, PublishOptions{Previous: &old}},
		{3, key, PublishOptions{Force: true, Previous: &old}},
		{1, key, PublishOptions{Force: true}},
		{3, ed25519.NewKeyFromSeed(bytes.Repeat([]byte{8}, 32)), PublishOptions{}},
		{3, ed25519.NewKeyFromSeed(bytes.Repeat([]byte{8}, 32)), PublishOptions{Force: true}},
	}
	for _, test := range attempts {
		if e := a.registry.Publish(context.Background(), "example.test", "demo", test.v, []byte("invalid"), test.k, test.o); e == nil {
			t.Fatal("invalid update accepted")
		}
	}
	for _, n := range []*Kademlia{a, b} {
		if localHead(t, n).Version != 2 {
			t.Fatal("head changed")
		}
	}
	if _, e := a.registry.Install(context.Background(), "example.test", "demo", "99"); e == nil {
		t.Fatal("unknown version accepted")
	}
	if _, e := a.registry.Latest(context.Background(), "example.test", "missing"); !errors.Is(e, ErrPackageNotFound) {
		t.Fatal(e)
	}
	if len(b.registry.Describe()) < 3 {
		t.Fatal("missing structured inspection")
	}
	// Domain key and snapshot ownership must not permit caller mutation.
	owner := a.registry.Owner("example.test")
	owner[0] ^= 1
	head := localHead(t, a)
	head.Signature[0] ^= 1
	if e := a.registry.verify(localHead(t, a), "latest-pointer"); e != nil {
		t.Fatal(e)
	}
}
func TestRegistryCatchupFetchesHistoryWithoutBlockingRPC(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	c, key := registryConfig()
	a := registryNode(t, sim, 34100, c)
	mustPublish(t, a, key, 1)
	first := localHead(t, a)
	mustPublish(t, a, key, 2)
	mustPublish(t, a, key, 3)
	latest := localHead(t, a)
	b := registryNode(t, sim, 34101, c)
	// Give B only the first record and pointer, not the missing versions.
	recordID, _ := NewKademliaID(first.RecordHash)
	data, _ := a.dataStore.get(*recordID)
	if e := b.dataStore.put(*recordID, data); e != nil {
		t.Fatal(e)
	}
	if e := b.registry.accept(context.Background(), first); e != nil {
		t.Fatal(e)
	}
	seedContact(t, b, a.Contact())
	seedContact(t, a, b.Contact())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if e := a.registry.send(ctx, b.Contact(), latest); e != nil {
		t.Fatal(e)
	}
	if localHead(t, b).Version != 3 {
		t.Fatal("did not catch up")
	}
	// Idempotent retransmission is successful.
	if e := a.registry.send(ctx, b.Contact(), latest); e != nil {
		t.Fatal(e)
	}
	// Older heads must be rejected, not replace the accepted head.
	if e := a.registry.send(ctx, b.Contact(), first); e == nil {
		t.Fatal("rollback accepted")
	}
	history, e := b.registry.History(ctx, "example.test", "demo")
	if e != nil || len(history) != 3 {
		t.Fatal(history, e)
	}
}
func TestRegistryConcurrentForkAndBadRecords(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	c, key := registryConfig()
	a := registryNode(t, sim, 34200, c)
	mustPublish(t, a, key, 1)
	first := localHead(t, a)
	left := makeVersion(t, a, key, 2, first.RecordHash)
	right := makeVersion(t, a, key, 3, first.RecordHash)
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, h := range []PackageRecord{left, right} {
		go func(h PackageRecord) { <-start; results <- a.registry.accept(context.Background(), h) }(h)
	}
	close(start)
	success := 0
	for i := 0; i < 2; i++ {
		if <-results == nil {
			success++
		}
	}
	if success != 1 {
		t.Fatalf("accepted %d competing heads", success)
	}
	current := localHead(t, a)
	invalid := []PackageRecord{current, current, current, current, current}
	invalid[0].Version++
	invalid[1].Domain = "other.test"
	invalid[2].RecordHash = zeroHash
	invalid[3].BlobHash = zeroHash
	invalid[4].Tag = "version-record"
	for _, h := range invalid {
		h = signRecord(h, key)
		if e := a.registry.accept(context.Background(), h); e == nil {
			t.Fatal("invalid head accepted")
		}
	}
	// A signed record with non-increasing predecessor versions cannot extend history.
	lower := makeVersion(t, a, key, 1, current.RecordHash)
	if e := a.registry.accept(context.Background(), lower); e == nil {
		t.Fatal("decreasing chain accepted")
	}
	wrongPackage := signRecord(PackageRecord{Tag: "latest-pointer", Domain: "example.test", Package: "other", Version: current.Version, RecordHash: current.RecordHash}, key)
	if e := a.registry.accept(context.Background(), wrongPackage); e == nil {
		t.Fatal("cross-package chain accepted")
	}
}
func TestRegistryFreshnessAndReplicationCatchup(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	c, key := registryConfig()
	a := registryNode(t, sim, 34300, c)
	mustPublish(t, a, key, 1)
	first := localHead(t, a)
	mustPublish(t, a, key, 2)
	newest := localHead(t, a)
	b := registryNode(t, sim, 34301, c)
	for hash, data := range a.dataStore.snapshot() {
		if e := b.dataStore.put(hash, data); e != nil {
			t.Fatal(e)
		}
	}
	if e := b.registry.accept(context.Background(), first); e != nil {
		t.Fatal(e)
	}
	seedContact(t, b, a.Contact())
	// A stale replica sending its old head learns the new head from the rejection.
	if e := b.registry.send(context.Background(), a.Contact(), first); e == nil {
		t.Fatal("old update accepted")
	}
	if localHead(t, b).Version != 2 {
		t.Fatal("rejected sender did not catch up")
	}
	// Recreate a stale local head to exercise latest lookup freshness.
	b.registry.mu.Lock()
	b.registry.heads[latestKey("example.test", "demo")] = first
	b.registry.mu.Unlock()
	got, e := b.registry.Latest(context.Background(), "example.test", "demo")
	if e != nil || got.RecordHash != newest.RecordHash {
		t.Fatal(got, e)
	}
	if e := b.Replicate(context.Background()); e != nil {
		t.Fatal(e)
	}
}
func TestRegistryUDPAndJoinHandoff(t *testing.T) {
	c, key := registryConfig()
	makeNode := func() *Kademlia {
		tr, e := Listen("127.0.0.1", 0)
		if e != nil {
			t.Fatal(e)
		}
		return testRPCNode(t, tr, c)
	}
	a := makeNode()
	mustPublish(t, a, key, 1)
	b := makeNode()
	if e := b.Join(context.Background(), a.Contact().Address); e != nil {
		t.Fatal(e)
	}
	waitUntil(t, func() bool { return len(b.registry.snapshot()) == 1 })
	// Replication and handoff preserve package availability after the publisher exits.
	if e := a.Replicate(context.Background()); e != nil {
		t.Fatal(e)
	}
	a.Close()
	result, e := b.registry.Install(context.Background(), "example.test", "demo", "latest")
	if e != nil || string(result.Data) != "payload 1" {
		t.Fatal(result, e)
	}
}
func TestRegistryValidationAndCancellation(t *testing.T) {
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	c, key := registryConfig()
	a := registryNode(t, sim, 34400, c)
	for _, s := range []string{"", ":x", "x:", "a:b:c", "UPPER:x"} {
		if _, e := ParsePackage(s, false); e == nil {
			t.Fatal(s)
		}
	}
	if _, e := ParsePackage("example.test:demo:1", true); e != nil {
		t.Fatal(e)
	}
	if _, e := a.registry.Latest(context.Background(), "BAD", "demo"); e == nil {
		t.Fatal("invalid name")
	}
	if e := a.registry.Publish(context.Background(), "example.test", "demo", 1, nil, nil, PublishOptions{}); e == nil {
		t.Fatal("missing key")
	}
	if e := a.registry.Publish(context.Background(), "example.test", "demo", 1, make([]byte, 1025), key, PublishOptions{}); !errors.Is(e, ErrValueTooLarge) {
		t.Fatal(e)
	}
	mustPublish(t, a, key, 1)
	head := localHead(t, a)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e := a.registry.accept(ctx, head); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = a.registry.accept(context.Background(), head) }()
	}
	wg.Wait()
	req := registryRequest{Key: zeroHash, Head: &head}
	payload, _ := json.Marshal(req)
	response := a.network.registryPayload(rpcMessage{Type: "REGISTRY_UPDATE", Payload: payload})
	if !strings.Contains(string(response), "wrong latest-pointer key") {
		t.Fatal(string(response))
	}
	for _, raw := range []string{"{", `{"key":"bad"}`} {
		if a.network.registryPayload(rpcMessage{Type: "REGISTRY_GET", Payload: []byte(raw)}) != nil {
			t.Fatal("malformed accepted")
		}
	}
	// Part 1 still rejects any pointer under its non-content-addressed key.
	hash := latestKey(head.Domain, head.Package)
	if e := a.dataStore.put(hash, recordBytes(head)); !errors.Is(e, ErrHashMismatch) {
		t.Fatal(e)
	}
	sum := sha256.Sum256(recordBytes(head))
	if KademliaID(sum) == hash {
		t.Fatal("unexpected pointer hash")
	}
}
