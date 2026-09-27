package kademlia

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func hasContact(table *RoutingTable, contact Contact) bool {
	return table.contactVersion(contact.ID) != 0
}

func waitUntil(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(time.Millisecond)
	}
}

func sameBucketContacts(t *testing.T, table *RoutingTable, index, count int) []Contact {
	t.Helper()
	var contacts []Contact
	for port := 10000; port < 60000 && len(contacts) < count; port++ {
		contact := rpcContact(fmt.Sprintf("127.0.0.1:%d", port))
		if table.getBucketIndex(contact.ID) == index {
			contacts = append(contacts, contact)
		}
	}
	if len(contacts) != count {
		t.Fatal("could not generate bucket contacts")
	}
	return contacts
}

func bucketProbeFinished(table *RoutingTable, index int) bool {
	table.mu.RLock()
	defer table.mu.RUnlock()
	return !table.buckets[index].probing
}

func TestRPCCommunicationUpdatesRouting(t *testing.T) {
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	a := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), DefaultConfig())
	b := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8100"), DefaultConfig())
	peer := b.Contact()
	if _, err := a.Ping(context.Background(), &peer); err != nil {
		t.Fatal(err)
	}
	if !hasContact(a.routingTable, peer) || !hasContact(b.routingTable, a.Contact()) {
		t.Fatal("PING did not update both routing tables")
	}
	oldVersion := a.routingTable.contactVersion(peer.ID)
	if _, err := a.Ping(context.Background(), &peer); err != nil {
		t.Fatal(err)
	}
	if a.routingTable.contactVersion(peer.ID) <= oldVersion {
		t.Fatal("existing peer was not refreshed")
	}

	c := testEndpoint(t, simulation, "127.0.0.1:8200")
	unexpected := rpcMessage{RequestID: strings.Repeat("a", 32), Type: rpcPing, Response: true, Sender: rpcContact(c.LocalAddr())}
	sendRPC(t, c, a.me.Address, unexpected)
	unexpected.Response = false
	unexpected.Type = "UNKNOWN"
	sendRPC(t, c, a.me.Address, unexpected)
	// A legitimate PING acts as a barrier for the previous queued datagrams.
	if _, err := a.Ping(context.Background(), &peer); err != nil {
		t.Fatal(err)
	}
	if hasContact(a.routingTable, rpcContact(c.LocalAddr())) {
		t.Fatal("unsolicited/unknown RPC added a peer")
	}
}

func TestFullBucketEviction(t *testing.T) {
	for _, outcome := range []string{"alive", "dead", "recent request", "shutdown"} {
		t.Run(outcome, func(t *testing.T) {
			config := DefaultConfig()
			config.K, config.RPCRetries = 2, 0
			config.RPCTimeout = 150 * time.Millisecond
			simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
			origin := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), config)
			contacts := sameBucketContacts(t, origin.routingTable, 0, 3)
			old := testEndpoint(t, simulation, contacts[0].Address)
			seedContact(t, origin, contacts[0]) // Least recently seen.
			seedContact(t, origin, contacts[1])
			replacement := testRPCNode(t, testEndpoint(t, simulation, contacts[2].Address), config)
			peer := origin.Contact()
			// If the receiver blocks waiting for eviction PING, this call hangs.
			if _, err := replacement.Ping(context.Background(), &peer); err != nil {
				t.Fatal(err)
			}
			probe := readRPC(t, old)
			if probe.Type != rpcPing || probe.Response {
				t.Fatal("oldest contact was not pinged")
			}
			switch outcome {
			case "alive":
				probe.Response = true
				probe.Sender = contacts[0]
				sendRPC(t, old, origin.me.Address, probe)
			case "recent request":
				// The old PING reply is lost, but a fresh incoming request
				// must protect this node from the outstanding timeout.
				request := rpcMessage{RequestID: strings.Repeat("b", 32), Type: rpcPing, Sender: contacts[0]}
				sendRPC(t, old, origin.me.Address, request)
				if response := readRPC(t, old); !response.Response {
					t.Fatal("missing response to fresh PING")
				}
			case "shutdown":
				origin.Close()
			}
			waitUntil(t, func() bool { return bucketProbeFinished(origin.routingTable, 0) })
			if outcome == "dead" {
				if hasContact(origin.routingTable, contacts[0]) || !hasContact(origin.routingTable, contacts[2]) {
					t.Fatal("dead oldest peer was not replaced")
				}
			} else {
				if !hasContact(origin.routingTable, contacts[0]) || hasContact(origin.routingTable, contacts[2]) {
					t.Fatal("live/recently seen peer was incorrectly replaced")
				}
			}
			if !hasContact(origin.routingTable, contacts[1]) {
				t.Fatal("unrelated peer was evicted")
			}
			origin.routingTable.mu.RLock()
			size := origin.routingTable.buckets[0].Len()
			origin.routingTable.mu.RUnlock()
			if size != config.K {
				t.Fatal("bucket capacity changed")
			}
		})
	}
}

func TestProbeReservationsAndStaleDecisions(t *testing.T) {
	self := KademliaID{}
	table := newRoutingTable(NewContact(&self, "self"), 1)
	oldID, newID, otherID := KademliaID{0x80}, KademliaID{0x81}, KademliaID{0x82}
	old := NewContact(&oldID, "old")
	newContact := NewContact(&newID, "new")
	other := NewContact(&otherID, "other")
	table.AddContact(old)
	probe := table.observe(newContact)
	if probe == nil || !table.probeCurrent(probe) {
		t.Fatal("missing probe reservation")
	}
	if table.observe(other) != nil {
		t.Fatal("more than one pending check for a bucket")
	}
	table.observe(old)
	if table.probeCurrent(probe) {
		t.Fatal("recent activity did not invalidate old decision")
	}
	table.finishProbe(probe, true)
	if !hasContact(table, old) {
		t.Fatal("stale timeout removed live contact")
	}

	probe = table.observe(newContact)
	table.removeUnresponsive(old.ID, probe.version)
	table.AddContact(other)
	table.finishProbe(probe, true)
	if !hasContact(table, other) || hasContact(table, newContact) {
		t.Fatal("probe evicted an unrelated replacement")
	}
	if table.observe(NewContact(&self, "self")) != nil || table.observe(Contact{}) != nil {
		t.Fatal("self/nil contact triggered a probe")
	}
}

func TestRPCTimeoutRemovalAndCancellation(t *testing.T) {
	for _, cancelCall := range []bool{false, true} {
		config := DefaultConfig()
		config.RPCTimeout, config.RPCRetries = 100*time.Millisecond, 0
		simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
		origin := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), config)
		remote := testEndpoint(t, simulation, "127.0.0.1:8100")
		peer := rpcContact(remote.LocalAddr())
		seedContact(t, origin, peer)
		ctx, cancel := context.WithCancel(context.Background())
		result := startPing(origin, ctx, peer)
		readRPC(t, remote)
		want := error(ErrRPCTimeout)
		if cancelCall {
			cancel()
			want = context.Canceled
		}
		got := awaitPing(t, result)
		cancel()
		if !errors.Is(got.err, want) {
			t.Fatalf("got %v, want %v", got.err, want)
		}
		if hasContact(origin.routingTable, peer) != cancelCall {
			t.Fatal("incorrect removal policy")
		}
	}
}

func TestRandomRefreshTargets(t *testing.T) {
	self := KademliaID{0xa5, 0x19, 0xff}
	table := NewRoutingTable(NewContact(&self, "self"))
	for index := 0; index < IDLength*8; index++ {
		for repeat := 0; repeat < 3; repeat++ {
			target, err := randomBucketTarget(&self, index)
			if err != nil {
				t.Fatal(err)
			}
			if target.Equals(&self) || table.getBucketIndex(target) != index {
				t.Fatalf("refresh target belongs to wrong range for bucket %d", index)
			}
		}
	}
	for _, index := range []int{-1, IDLength * 8} {
		if _, err := randomBucketTarget(&self, index); err == nil {
			t.Fatal("invalid bucket accepted")
		}
	}
	if _, err := randomBucketTarget(nil, 0); err == nil {
		t.Fatal("nil ID accepted")
	}
}

func TestBucketLookupActivity(t *testing.T) {
	self := KademliaID{}
	table := NewRoutingTable(NewContact(&self, "self"))
	now := time.Now()
	table.mu.Lock()
	for _, bucket := range table.buckets {
		bucket.lastLookup = now
	}
	table.buckets[0].lastLookup = now.Add(-2 * time.Hour)
	table.buckets[17].lastLookup = now.Add(-time.Hour)
	table.mu.Unlock()
	stale := table.staleBuckets(now, time.Hour)
	if len(stale) != 2 || stale[0] != 0 || stale[1] != 17 {
		t.Fatal("wrong stale buckets")
	}
	target, err := randomBucketTarget(&self, 17)
	if err != nil {
		t.Fatal(err)
	}
	table.markLookup(target, now)
	stale = table.staleBuckets(now, time.Hour)
	if len(stale) != 1 || stale[0] != 0 {
		t.Fatal("lookup did not reset its bucket timer")
	}
}
