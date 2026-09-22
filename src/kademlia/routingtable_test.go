package kademlia

import (
	"fmt"
	"testing"
)

// FIXME: This test doesn't actually test anything. There is only one assertion
// that is included as an example.

func TestRoutingTable(t *testing.T) {
	rt := NewRoutingTable(NewContact(mustKademliaID(t, "FFFFFFFF00000000000000000000000000000000000000000000000000000000"), "localhost:8000"))

	rt.AddContact(NewContact(mustKademliaID(t, "FFFFFFFF00000000000000000000000000000000000000000000000000000000"), "localhost:8001"))
	rt.AddContact(NewContact(mustKademliaID(t, "1111111100000000000000000000000000000000000000000000000000000000"), "localhost:8002"))
	rt.AddContact(NewContact(mustKademliaID(t, "1111111200000000000000000000000000000000000000000000000000000000"), "localhost:8002"))
	rt.AddContact(NewContact(mustKademliaID(t, "1111111300000000000000000000000000000000000000000000000000000000"), "localhost:8002"))
	rt.AddContact(NewContact(mustKademliaID(t, "1111111400000000000000000000000000000000000000000000000000000000"), "localhost:8002"))
	rt.AddContact(NewContact(mustKademliaID(t, "2111111400000000000000000000000000000000000000000000000000000000"), "localhost:8002"))

	contacts := rt.FindClosestContacts(mustKademliaID(t, "2111111400000000000000000000000000000000000000000000000000000000"), 20)
	for i := range contacts {
		fmt.Println(contacts[i].String())
	}

	// TODO: This is just an example. Make more meaningful assertions.
	if len(contacts) != 6 {
		t.Fatalf("Expected 6 contacts but instead got %d", len(contacts))
	}
}
