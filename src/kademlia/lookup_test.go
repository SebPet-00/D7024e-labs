package kademlia

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"
	"time"
)

func seedContact(t *testing.T, node *Kademlia, contact Contact) {
	t.Helper()
	if err := node.AddContact(contact); err != nil {
		t.Fatal(err)
	}
}

func contactsByDistance(contacts []Contact, target *KademliaID) []Contact {
	result := append([]Contact(nil), contacts...)
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID.CalcDistance(target).Less(result[j].ID.CalcDistance(target))
	})
	return result
}

func expectContacts(t *testing.T, got, want []Contact) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d contacts, want %d", len(got), len(want))
	}
	for i := range want {
		if !got[i].ID.Equals(want[i].ID) || got[i].Address != want[i].Address {
			t.Fatalf("contact %d: got %s, want %s", i, got[i].String(), want[i].String())
		}
	}
}

type lookupResult struct {
	contacts []Contact
	err      error
}

func startLookup(node *Kademlia, ctx context.Context, target *KademliaID) <-chan lookupResult {
	result := make(chan lookupResult, 1)
	go func() {
		contacts, err := node.LookupContact(ctx, target)
		result <- lookupResult{contacts, err}
	}()
	return result
}
func awaitLookup(t *testing.T, result <-chan lookupResult) lookupResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("lookup did not finish")
		return lookupResult{}
	}
}
func replyFindNode(t *testing.T, endpoint Transport, request rpcMessage, contacts []Contact) {
	t.Helper()
	if contacts == nil {
		contacts = []Contact{}
	}
	payload, err := json.Marshal(findNodeResponse{Contacts: contacts})
	if err != nil {
		t.Fatal(err)
	}
	to := request.Sender.Address
	request.Response = true
	request.Sender = rpcContact(endpoint.LocalAddr())
	request.Payload = payload
	sendRPC(t, endpoint, to, request)
}

type lookupRecorder struct {
	Transport
	mu     sync.Mutex
	probes map[string]int
}

func (recorder *lookupRecorder) Send(to string, data []byte) error {
	var message rpcMessage
	if json.Unmarshal(data, &message) == nil && message.Type == rpcFindNode && !message.Response {
		recorder.mu.Lock()
		recorder.probes[to]++
		recorder.mu.Unlock()
	}
	return recorder.Transport.Send(to, data)
}

func TestLookupMultiHop(t *testing.T) {
	config := DefaultConfig()
	config.K, config.Alpha, config.RPCRetries = 3, 2, 0
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	record := &lookupRecorder{Transport: testEndpoint(t, simulation, "127.0.0.1:8000"), probes: make(map[string]int)}
	origin := testRPCNode(t, record, config)
	target := KademliaID{}
	var peers []*Kademlia
	for i := 0; i < 6; i++ {
		peers = append(peers, testRPCNode(t, testEndpoint(t, simulation, fmt.Sprintf("127.0.0.1:%d", 8100+i)), config))
	}
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].me.ID.CalcDistance(&target).Less(peers[j].me.ID.CalcDistance(&target))
	})
	// Start at the farthest peer and discover successively closer peers.
	seedContact(t, origin, peers[5].Contact())
	for i := 5; i > 0; i-- {
		seedContact(t, peers[i], peers[i-1].Contact())
		seedContact(t, peers[i-1], peers[i].Contact()) // Cycles must not cause repeated probes.
	}
	got, err := origin.LookupContact(context.Background(), &target)
	if err != nil {
		t.Fatal(err)
	}
	expectContacts(t, got, []Contact{peers[0].Contact(), peers[1].Contact(), peers[2].Contact()})
	record.mu.Lock()
	defer record.mu.Unlock()
	if len(record.probes) != 6 {
		t.Fatalf("discovered %d peers, want 6", len(record.probes))
	}
	for address, count := range record.probes {
		if count != 1 {
			t.Fatalf("queried %s %d times", address, count)
		}
	}
	requireNoPending(t, origin.network)
}

func TestLookupStrictParallelBatches(t *testing.T) {
	config := DefaultConfig()
	config.K, config.Alpha, config.RPCRetries = 4, 2, 0
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	origin := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), config)
	endpoints := make(map[string]*SimulatedTransport)
	var contacts []Contact
	for i := 0; i < 4; i++ {
		endpoint := testEndpoint(t, simulation, fmt.Sprintf("127.0.0.1:%d", 8100+i))
		endpoints[endpoint.LocalAddr()] = endpoint
		contact := rpcContact(endpoint.LocalAddr())
		contacts = append(contacts, contact)
		seedContact(t, origin, contact)
	}
	target := KademliaID{}
	contacts = contactsByDistance(contacts, &target)
	result := startLookup(origin, context.Background(), &target)
	// Both first-batch requests must arrive before either peer replies.
	first := readRPC(t, endpoints[contacts[0].Address])
	second := readRPC(t, endpoints[contacts[1].Address])
	for _, request := range []rpcMessage{first, second} {
		var payload findNodeRequest
		if request.Type != rpcFindNode || json.Unmarshal(request.Payload, &payload) != nil || payload.Target != target.String() {
			t.Fatal("wrong FIND_NODE request")
		}
	}
	replyFindNode(t, endpoints[contacts[0].Address], first, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := endpoints[contacts[2].Address].Receive(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("next batch started before the previous one completed")
	}
	replyFindNode(t, endpoints[contacts[1].Address], second, nil)
	third := readRPC(t, endpoints[contacts[2].Address])
	fourth := readRPC(t, endpoints[contacts[3].Address])
	replyFindNode(t, endpoints[contacts[2].Address], third, nil)
	replyFindNode(t, endpoints[contacts[3].Address], fourth, nil)
	got := awaitLookup(t, result)
	if got.err != nil {
		t.Fatal(got.err)
	}
	expectContacts(t, got.contacts, contacts)
}

func TestLookupContinuesAfterNoImprovement(t *testing.T) {
	config := DefaultConfig()
	config.K, config.Alpha = 2, 1
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	origin := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), config)
	var peers []*Kademlia
	target := KademliaID{}
	for i := 0; i < 3; i++ {
		peers = append(peers, testRPCNode(t, testEndpoint(t, simulation, fmt.Sprintf("127.0.0.1:%d", 8100+i)), config))
	}
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].me.ID.CalcDistance(&target).Less(peers[j].me.ID.CalcDistance(&target))
	})
	seedContact(t, origin, peers[1].Contact())
	seedContact(t, origin, peers[2].Contact())
	// The first queried peer has an empty table. The second reveals a closer one.
	seedContact(t, peers[2], peers[0].Contact())
	got, err := origin.LookupContact(context.Background(), &target)
	if err != nil {
		t.Fatal(err)
	}
	expectContacts(t, got, []Contact{peers[0].Contact(), peers[1].Contact()})
}

func TestLookupFallsBackAfterFailure(t *testing.T) {
	config := DefaultConfig()
	config.K, config.Alpha, config.RPCRetries = 2, 2, 0
	config.RPCTimeout = 20 * time.Millisecond
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	origin := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), config)
	var peers []*Kademlia
	for i := 0; i < 3; i++ {
		peer := testRPCNode(t, testEndpoint(t, simulation, fmt.Sprintf("127.0.0.1:%d", 8100+i)), config)
		peers = append(peers, peer)
		seedContact(t, origin, peer.Contact())
	}
	// Keep contacts in different buckets if necessary: the table can hold only
	// K per bucket. Explicitly verify all three seeds were retained.
	if len(origin.routingTable.FindClosestContacts(peers[0].me.ID, 10)) != 3 {
		t.Fatal("fixture needs three stored contacts")
	}
	target := *peers[0].me.ID
	peers[0].Close()
	got, err := origin.LookupContact(context.Background(), &target)
	if err != nil {
		t.Fatal(err)
	}
	expectContacts(t, got, contactsByDistance([]Contact{peers[1].Contact(), peers[2].Contact()}, &target))
}

func TestLookupExactTargetStillReturnsK(t *testing.T) {
	config := DefaultConfig()
	config.K = 2
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	origin := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), config)
	b := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8100"), config)
	c := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8101"), config)
	seedContact(t, origin, b.Contact())
	seedContact(t, b, c.Contact())
	got, err := origin.LookupContact(context.Background(), b.me.ID)
	if err != nil {
		t.Fatal(err)
	}
	expectContacts(t, got, []Contact{b.Contact(), c.Contact()})
}

func TestLookupErrorsAndCancellation(t *testing.T) {
	config := DefaultConfig()
	config.RPCTimeout, config.RPCRetries = 20*time.Millisecond, 0
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	origin := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), config)
	target := KademliaID{}
	if _, err := origin.LookupContact(context.Background(), nil); err == nil {
		t.Fatal("nil target accepted")
	}
	if _, err := origin.LookupContact(context.Background(), &target); !errors.Is(err, ErrNoReachableContacts) {
		t.Fatal(err)
	}
	if err := origin.AddContact(Contact{}); err == nil {
		t.Fatal("invalid seed accepted")
	}
	seedContact(t, origin, origin.Contact())
	seedContact(t, origin, rpcContact("127.0.0.1:8100")) // No endpoint.
	if _, err := origin.LookupContact(context.Background(), &target); !errors.Is(err, ErrNoReachableContacts) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := origin.LookupContact(ctx, &target); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	origin.Close()
	if _, err := origin.LookupContact(context.Background(), &target); !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	offline, err := NewKademlia("127.0.0.1:9000", config)
	if err != nil {
		t.Fatal(err)
	}
	defer offline.Close()
	if _, err := offline.LookupContact(context.Background(), &target); err == nil {
		t.Fatal("offline lookup accepted")
	}
}

func TestLookupCancellationDrainsWorkers(t *testing.T) {
	for _, closeNode := range []bool{false, true} {
		simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
		origin := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), DefaultConfig())
		peer := testEndpoint(t, simulation, "127.0.0.1:8100")
		seedContact(t, origin, rpcContact(peer.LocalAddr()))
		ctx, cancel := context.WithCancel(context.Background())
		target := KademliaID{}
		result := startLookup(origin, ctx, &target)
		readRPC(t, peer)
		want := error(context.Canceled)
		if closeNode {
			origin.Close()
			want = net.ErrClosed
		} else {
			cancel()
		}
		got := awaitLookup(t, result)
		cancel()
		if !errors.Is(got.err, want) {
			t.Fatalf("got %v, want %v", got.err, want)
		}
		requireNoPending(t, origin.network)
	}
}

func TestLookupUDP(t *testing.T) {
	origin := testRPCNode(t, testUDP(t), DefaultConfig())
	b := testRPCNode(t, testUDP(t), DefaultConfig())
	c := testRPCNode(t, testUDP(t), DefaultConfig())
	seedContact(t, origin, b.Contact())
	seedContact(t, b, c.Contact())
	seedContact(t, c, b.Contact())
	got, err := origin.LookupContact(context.Background(), c.me.ID)
	if err != nil {
		t.Fatal(err)
	}
	expectContacts(t, got, []Contact{c.Contact(), b.Contact()})
}

func TestConcurrentLookups(t *testing.T) {
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	origin := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), DefaultConfig())
	b := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8100"), DefaultConfig())
	c := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8101"), DefaultConfig())
	seedContact(t, origin, b.Contact())
	seedContact(t, b, c.Contact())
	var workers sync.WaitGroup
	for i := 0; i < 12; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			got, err := origin.LookupContact(context.Background(), c.me.ID)
			if err != nil {
				t.Error(err)
				return
			}
			if len(got) != 2 || !got[0].ID.Equals(c.me.ID) {
				t.Error("concurrent lookup missed target")
			}
		}()
	}
	workers.Wait()
}
