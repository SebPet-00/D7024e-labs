package kademlia

import (
	"math/rand"
	"sort"
	"sync"
	"testing"
)

func TestRoutingTable(t *testing.T) {
	me := NewContact(mustKademliaID(t, "FFFFFFFF00000000000000000000000000000000000000000000000000000000"), "localhost:8000")
	rt := NewRoutingTable(me)
	rt.AddContact(me) // A node must not appear in its own buckets.
	for _, prefix := range []string{"11111111", "11111112", "11111113", "11111114", "21111114"} {
		rt.AddContact(NewContact(mustKademliaID(t, prefix+"00000000000000000000000000000000000000000000000000000000"), "localhost:8002"))
	}
	target := mustKademliaID(t, "2111111400000000000000000000000000000000000000000000000000000000")
	contacts := rt.FindClosestContacts(target, 20)
	if len(contacts) != 5 {
		t.Fatalf("got %d contacts, want 5 excluding self", len(contacts))
	}
	if !contacts[0].ID.Equals(target) {
		t.Fatal("exact match must come first")
	}
	for i, contact := range contacts {
		if contact.ID.Equals(me.ID) {
			t.Fatal("self included")
		}
		if i > 0 && contact.Less(&contacts[i-1]) {
			t.Fatal("contacts are not sorted")
		}
	}
}

func TestClosestContactsSearchesAllBuckets(t *testing.T) {
	me, target := KademliaID{}, KademliaID{0x40}
	far, near := KademliaID{0x80}, KademliaID{0x10}
	rt := NewRoutingTable(NewContact(&me, "self"))
	rt.AddContact(NewContact(&far, "far"))
	rt.AddContact(NewContact(&near, "near"))
	// Bucket 0 is adjacent to target bucket 1, but bucket 3 contains the
	// closer contact: 0x40 XOR 0x10 = 0x50 < 0x40 XOR 0x80 = 0xc0.
	if got := rt.FindClosestContacts(&target, 1); len(got) != 1 || !got[0].ID.Equals(&near) {
		t.Fatal("missed the nearer contact in a non-adjacent bucket")
	}
}

func TestClosestContactsMatchesExhaustiveSort(t *testing.T) {
	random := rand.New(rand.NewSource(7))
	me := KademliaID{}
	rt := newRoutingTable(NewContact(&me, "self"), 200)
	var all []Contact
	for i := 0; i < 200; i++ {
		id := KademliaID{}
		_, _ = random.Read(id[:])
		contact := NewContact(&id, "peer")
		all = append(all, contact)
		rt.AddContact(contact)
	}
	for i := 0; i < 30; i++ {
		target := KademliaID{}
		_, _ = random.Read(target[:])
		want := append([]Contact(nil), all...)
		sort.Slice(want, func(i, j int) bool {
			return want[i].ID.CalcDistance(&target).Less(want[j].ID.CalcDistance(&target))
		})
		got := rt.FindClosestContacts(&target, 10)
		if len(got) != 10 {
			t.Fatal("wrong result count")
		}
		for j := range got {
			if !got[j].ID.Equals(want[j].ID) {
				t.Fatalf("wrong nearest contact at %d", j)
			}
		}
	}
}

func TestRoutingTableOwnsContactCopies(t *testing.T) {
	me, peer := KademliaID{}, KademliaID{0x80}
	rt := NewRoutingTable(NewContact(&me, "self"))
	me[0] = 0xff             // Must not change the table's own ID.
	rt.AddContact(Contact{}) // Ignore missing ID.
	rt.AddContact(NewContact(&peer, "peer"))
	peer[0] = 0x40
	target := KademliaID{0x80}
	got := rt.FindClosestContacts(&target, 1)
	if len(got) != 1 || !got[0].ID.Equals(&target) {
		t.Fatal("insertion retained caller ID pointer")
	}
	got[0].ID[0] = 0
	got[0].distance[0] = 0xff
	again := rt.FindClosestContacts(&target, 1)
	if !again[0].ID.Equals(&target) || again[0].distance[0] != 0 {
		t.Fatal("result exposes table state")
	}
	for _, count := range []int{-1, 0} {
		if len(rt.FindClosestContacts(&target, count)) != 0 {
			t.Fatal("nonpositive count accepted")
		}
	}
	if len(rt.FindClosestContacts(nil, 1)) != 0 {
		t.Fatal("nil target accepted")
	}
}

func TestRoutingTableConcurrentAccess(t *testing.T) {
	me := KademliaID{}
	rt := NewRoutingTable(NewContact(&me, "self"))
	var workers sync.WaitGroup
	for i := 0; i < 20; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			for j := 0; j < 100; j++ {
				id := KademliaID{byte(i + 1), byte(j)}
				rt.AddContact(NewContact(&id, "peer"))
				_ = rt.FindClosestContacts(&id, 10)
			}
		}(i)
	}
	workers.Wait()
}
