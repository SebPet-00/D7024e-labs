package kademlia

import (
	"crypto/sha256"
	"testing"
)

func TestInspectionSnapshots(t *testing.T) {
	node, err := NewKademlia("127.0.0.1:9800", DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	peer := rpcContact("127.0.0.1:9801")
	if err := node.AddContact(peer); err != nil {
		t.Fatal(err)
	}
	snapshot := node.RoutingSnapshot()
	if len(snapshot) != 1 || len(snapshot[0].Contacts) != 1 {
		t.Fatal("incorrect bucket snapshot")
	}
	snapshot[0].Contacts[0].ID[0] ^= 255
	if !node.RoutingSnapshot()[0].Contacts[0].ID.Equals(peer.ID) {
		t.Fatal("snapshot exposed contact ID")
	}
	for _, value := range []string{"one", "two", ""} {
		data := []byte(value)
		key := KademliaID(sha256.Sum256(data))
		if err := node.dataStore.put(key, data); err != nil {
			t.Fatal(err)
		}
	}
	entries := node.DataSnapshot()
	if len(entries) != 3 {
		t.Fatal("missing keys")
	}
	for i, entry := range entries {
		value, found := node.dataStore.get(entry.Key)
		if !found || len(value) != entry.Size {
			t.Fatal("incorrect size")
		}
		if i > 0 && !entries[i-1].Key.Less(&entry.Key) {
			t.Fatal("unsorted keys")
		}
	}
}
