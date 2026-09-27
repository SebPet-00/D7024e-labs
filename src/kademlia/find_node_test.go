package kademlia

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestFindNodeReturnsNearestAndExcludesRequester(t *testing.T) {
	config := DefaultConfig()
	config.K = 3
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	a := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), config)
	b := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8100"), config)
	seedContact(t, b, a.Contact())
	for i := 0; i < 20; i++ {
		seedContact(t, b, rpcContact(fmt.Sprintf("127.0.0.1:%d", 9000+i)))
	}
	target := a.Contact().ID // Requester would be the closest if it were returned.
	var expected []Contact
	for _, contact := range b.routingTable.FindClosestContacts(target, 100) {
		if !contact.ID.Equals(a.me.ID) {
			expected = append(expected, contact)
		}
	}
	if len(expected) < config.K {
		t.Fatal("not enough contacts in fixture")
	}
	peer := b.Contact()
	got, err := a.network.SendFindContactMessage(context.Background(), &peer, target)
	if err != nil {
		t.Fatal(err)
	}
	expectContacts(t, got, expected[:config.K])
}

func TestFindNodeResponseValidation(t *testing.T) {
	valid := rpcContact("127.0.0.1:8100")
	origin := rpcContact("127.0.0.1:8000")
	badID := cloneContact(valid)
	badID.ID[0] ^= 0xff
	badAddress := cloneContact(valid)
	badAddress.Address = "invalid"
	encode := func(contacts []Contact) json.RawMessage {
		data, err := json.Marshal(findNodeResponse{Contacts: contacts})
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	for _, test := range []struct {
		name      string
		payload   json.RawMessage
		want      []Contact
		wantError bool
	}{
		{"empty", encode([]Contact{}), []Contact{}, false},
		{"duplicates and self", encode([]Contact{valid, valid, origin}), []Contact{valid}, false},
		{"wrong type", json.RawMessage("\"wrong\""), nil, true},
		{"null", json.RawMessage("null"), nil, true},
		{"missing list", json.RawMessage("{}"), nil, true},
		{"null list", encode(nil), nil, true},
		{"too many", encode([]Contact{valid, valid, valid, valid}), nil, true},
		{"nil ID", encode([]Contact{{Address: valid.Address}}), nil, true},
		{"wrong ID", encode([]Contact{badID}), nil, true},
		{"bad address", encode([]Contact{badAddress}), nil, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := DefaultConfig()
			config.K = 3
			simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
			a := testRPCNode(t, testEndpoint(t, simulation, origin.Address), config)
			b := testEndpoint(t, simulation, valid.Address)
			type result struct {
				contacts []Contact
				err      error
			}
			done := make(chan result, 1)
			go func() {
				target := KademliaID{}
				contacts, err := a.network.SendFindContactMessage(context.Background(), &valid, &target)
				done <- result{contacts, err}
			}()
			request := readRPC(t, b)
			request.Response = true
			request.Sender = valid
			request.Payload = test.payload
			sendRPC(t, b, origin.Address, request)
			select {
			case got := <-done:
				if (got.err != nil) != test.wantError {
					t.Fatalf("unexpected error: %v", got.err)
				}
				if !test.wantError {
					expectContacts(t, got.contacts, test.want)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("FIND_NODE did not finish")
			}
			requireNoPending(t, a.network)
		})
	}
}

func TestFindNodeRejectsMalformedTargets(t *testing.T) {
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	a := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), DefaultConfig())
	b := testEndpoint(t, simulation, "127.0.0.1:8100")
	for _, payload := range []json.RawMessage{
		json.RawMessage("null"), json.RawMessage("{}"), json.RawMessage("{\"target\":7}"),
		json.RawMessage("{\"target\":\"short\"}"),
	} {
		request := rpcMessage{RequestID: strings.Repeat("a", 32), Type: rpcFindNode, Sender: rpcContact(b.LocalAddr()), Payload: payload}
		sendRPC(t, b, a.me.Address, request)
	}
	ping := rpcMessage{RequestID: strings.Repeat("b", 32), Type: rpcPing, Sender: rpcContact(b.LocalAddr())}
	sendRPC(t, b, a.me.Address, ping)
	if got := readRPC(t, b); got.Type != rpcPing || !got.Response {
		t.Fatal("malformed FIND_NODE target produced a response")
	}
	peer := rpcContact(b.LocalAddr())
	if _, err := a.network.SendFindContactMessage(context.Background(), &peer, nil); err == nil {
		t.Fatal("nil target accepted")
	}
}

func TestLookup1000Nodes(t *testing.T) {
	// Seed routing tables directly: joining is step 6. Each table keeps at
	// most K contacts per bucket, so lookups must discover additional peers.
	const count = 1000
	config := DefaultConfig()
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{Seed: 17})
	nodes := make([]*Kademlia, count)
	contacts := make([]Contact, count)
	for i := range nodes {
		endpoint := testEndpoint(t, simulation, fmt.Sprintf("127.0.0.1:%d", 20000+i))
		nodes[i] = testRPCNode(t, endpoint, config)
		contacts[i] = nodes[i].Contact()
	}
	for _, node := range nodes {
		for _, contact := range contacts {
			seedContact(t, node, contact)
		}
	}
	for _, pair := range [][2]int{{0, 999}, {423, 812}, {999, 127}} {
		origin, target := nodes[pair[0]], contacts[pair[1]].ID
		ordered := contactsByDistance(contacts, target)
		var want []Contact
		for _, contact := range ordered {
			if !contact.ID.Equals(origin.me.ID) {
				want = append(want, contact)
			}
			if len(want) == config.K {
				break
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		got, err := origin.LookupContact(ctx, target)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		expectContacts(t, got, want)
	}
}
