package kademlia

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func rpcContact(address string) Contact {
	id := KademliaID(sha256.Sum256([]byte(address)))
	return NewContact(&id, address)
}

func testRPCNode(t *testing.T, transport Transport, config Config) *Kademlia {
	t.Helper()
	node, err := NewKademliaWithTransport(transport, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(node.Close)
	return node
}

func readRPC(t *testing.T, transport Transport) rpcMessage {
	t.Helper()
	packet := receivePacket(t, transport)
	message, ok := decodeRPC(packet)
	if !ok {
		t.Fatalf("invalid RPC: %s", packet.Data)
	}
	return message
}

func sendRPC(t *testing.T, transport Transport, to string, message rpcMessage) {
	t.Helper()
	data, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.Send(to, data); err != nil {
		t.Fatal(err)
	}
}

type pingResult struct {
	elapsed time.Duration
	err     error
}

func startPing(node *Kademlia, ctx context.Context, peer Contact) <-chan pingResult {
	result := make(chan pingResult, 1)
	go func() {
		elapsed, err := node.Ping(ctx, &peer)
		result <- pingResult{elapsed, err}
	}()
	return result
}

func awaitPing(t *testing.T, result <-chan pingResult) pingResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(3 * time.Second):
		t.Fatal("PING did not finish")
		return pingResult{}
	}
}

func requireNoPending(t *testing.T, network *Network) {
	t.Helper()
	network.mu.Lock()
	defer network.mu.Unlock()
	if len(network.pending) != 0 {
		t.Fatal("completed call retained pending state")
	}
}

func TestPingSimulated(t *testing.T) {
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{Latency: 5 * time.Millisecond})
	a := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), DefaultConfig())
	b := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8001"), DefaultConfig())
	peer := b.Contact()
	elapsed, err := a.Ping(context.Background(), &peer)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed < 10*time.Millisecond {
		t.Fatal("RTT omitted network latency")
	}
	requireNoPending(t, a.network)
	// Pinging oneself also travels through the ordinary RPC path.
	self := a.Contact()
	if _, err := a.Ping(context.Background(), &self); err != nil {
		t.Fatal(err)
	}
}

func TestPingRetry(t *testing.T) {
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	config := DefaultConfig()
	config.RPCTimeout = 100 * time.Millisecond
	config.RPCRetries = 1
	a := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), config)
	b := testEndpoint(t, simulation, "127.0.0.1:8001")
	result := startPing(a, context.Background(), rpcContact(b.LocalAddr()))
	first := readRPC(t, b) // Deliberately lose the first reply.
	second := readRPC(t, b)
	if first.RequestID != second.RequestID {
		t.Fatal("retry changed logical request ID")
	}
	second.Response = true
	second.Sender = rpcContact(b.LocalAddr())
	sendRPC(t, b, a.me.Address, second)
	got := awaitPing(t, result)
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.elapsed < config.RPCTimeout {
		t.Fatal("elapsed time omitted retry wait")
	}
	requireNoPending(t, a.network)
}

func TestPingTimeoutAndRetryCount(t *testing.T) {
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	config := DefaultConfig()
	config.RPCTimeout = 15 * time.Millisecond
	config.RPCRetries = 2
	a := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), config)
	b := testEndpoint(t, simulation, "127.0.0.1:8001")
	peer := rpcContact(b.LocalAddr())
	_, err := a.Ping(context.Background(), &peer)
	if !errors.Is(err, ErrRPCTimeout) {
		t.Fatalf("expected timeout, got %v", err)
	}
	id := ""
	for i := 0; i < 3; i++ {
		request := readRPC(t, b)
		if i > 0 && request.RequestID != id {
			t.Fatal("retry ID mismatch")
		}
		id = request.RequestID
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := b.Receive(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("more retries than configured: %v", err)
	}
	requireNoPending(t, a.network)
}

func TestPingCompletePacketLoss(t *testing.T) {
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{PacketLoss: 1})
	config := DefaultConfig()
	config.RPCTimeout = 10 * time.Millisecond
	config.RPCRetries = 0
	a := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), config)
	b := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8001"), config)
	peer := b.Contact()
	if _, err := a.Ping(context.Background(), &peer); !errors.Is(err, ErrRPCTimeout) {
		t.Fatalf("lost PING returned %v", err)
	}
	requireNoPending(t, a.network)
}

func TestPingRejectsMismatchedResponses(t *testing.T) {
	for _, kind := range []string{"request ID", "method", "source", "sender ID", "sender address", "malformed JSON"} {
		t.Run(kind, func(t *testing.T) {
			simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
			a := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), DefaultConfig())
			b := testEndpoint(t, simulation, "127.0.0.1:8001")
			attacker := testEndpoint(t, simulation, "127.0.0.1:8002")
			result := startPing(a, context.Background(), rpcContact(b.LocalAddr()))
			request := readRPC(t, b)
			good := request
			good.Response = true
			good.Sender = rpcContact(b.LocalAddr())
			bad := good
			var sender Transport = b
			switch kind {
			case "request ID":
				bad.RequestID = strings.Repeat("0", 32)
				if bad.RequestID == good.RequestID {
					bad.RequestID = strings.Repeat("1", 32)
				}
			case "method":
				bad.Type = "FIND_NODE"
			case "source":
				sender = attacker
				bad.Sender = rpcContact(attacker.LocalAddr())
			case "sender ID":
				bad.Sender = NewContact(&KademliaID{}, b.LocalAddr())
			case "sender address":
				bad.Sender.Address = attacker.LocalAddr()
			}
			if kind == "malformed JSON" {
				if err := sender.Send(a.me.Address, []byte("{")); err != nil {
					t.Fatal(err)
				}
			} else {
				sendRPC(t, sender, a.me.Address, bad)
			}
			select {
			case got := <-result:
				t.Fatalf("invalid response completed call: %+v", got)
			case <-time.After(20 * time.Millisecond):
			}
			sendRPC(t, b, a.me.Address, good)
			if got := awaitPing(t, result); got.err != nil {
				t.Fatal(got.err)
			}
			requireNoPending(t, a.network)
		})
	}
}

func TestLateResponseDoesNotCompleteNextCall(t *testing.T) {
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	a := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), DefaultConfig())
	b := testEndpoint(t, simulation, "127.0.0.1:8001")
	ctx, cancel := context.WithCancel(context.Background())
	first := startPing(a, ctx, rpcContact(b.LocalAddr()))
	oldReply := readRPC(t, b)
	cancel()
	if got := awaitPing(t, first); !errors.Is(got.err, context.Canceled) {
		t.Fatal(got.err)
	}
	requireNoPending(t, a.network)
	second := startPing(a, context.Background(), rpcContact(b.LocalAddr()))
	newReply := readRPC(t, b)
	if oldReply.RequestID == newReply.RequestID {
		t.Fatal("separate calls reused request ID")
	}
	oldReply.Response = true
	oldReply.Sender = rpcContact(b.LocalAddr())
	sendRPC(t, b, a.me.Address, oldReply)
	select {
	case got := <-second:
		t.Fatalf("late response completed new call: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}
	newReply.Response = true
	newReply.Sender = rpcContact(b.LocalAddr())
	sendRPC(t, b, a.me.Address, newReply)
	if got := awaitPing(t, second); got.err != nil {
		t.Fatal(got.err)
	}
}

func TestConcurrentRPCsReorderedAndDuplicated(t *testing.T) {
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	a := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), DefaultConfig())
	b := testEndpoint(t, simulation, "127.0.0.1:8001")
	const count = 20
	results := make(chan error, count)
	for i := 0; i < count; i++ {
		go func(i int) {
			payload, _ := json.Marshal(i)
			peer := rpcContact(b.LocalAddr())
			response, err := a.network.call(context.Background(), &peer, rpcPing, payload)
			if err == nil && string(response.Payload) != string(payload) {
				err = errors.New("response delivered to wrong call")
			}
			results <- err
		}(i)
	}
	requests := make([]rpcMessage, count)
	ids := make(map[string]bool)
	for i := range requests {
		requests[i] = readRPC(t, b)
		if ids[requests[i].RequestID] {
			t.Fatal("duplicate request ID")
		}
		ids[requests[i].RequestID] = true
	}
	for i := count - 1; i >= 0; i-- {
		reply := requests[i]
		reply.Response = true
		reply.Sender = rpcContact(b.LocalAddr())
		sendRPC(t, b, a.me.Address, reply)
		sendRPC(t, b, a.me.Address, reply)
	}
	for i := 0; i < count; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent call stuck")
		}
	}
	requireNoPending(t, a.network)
}

func TestPingCancellationAndClose(t *testing.T) {
	for _, action := range []string{"cancel", "deadline", "close node", "close transport"} {
		t.Run(action, func(t *testing.T) {
			simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
			endpoint := testEndpoint(t, simulation, "127.0.0.1:8000")
			a := testRPCNode(t, endpoint, DefaultConfig())
			b := testEndpoint(t, simulation, "127.0.0.1:8001")
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			result := startPing(a, ctx, rpcContact(b.LocalAddr()))
			readRPC(t, b)
			want := error(net.ErrClosed)
			switch action {
			case "cancel":
				cancel()
				want = context.Canceled
			case "deadline":
				want = context.DeadlineExceeded
			case "close node":
				a.Close()
			case "close transport":
				_ = endpoint.Close()
			}
			if got := awaitPing(t, result); !errors.Is(got.err, want) {
				t.Fatalf("got %v, want %v", got.err, want)
			}
			requireNoPending(t, a.network)
		})
	}
}

type failingSendTransport struct {
	Transport
	err error
}

func (transport failingSendTransport) Send(string, []byte) error { return transport.err }

func TestRPCValidationAndSendFailure(t *testing.T) {
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	endpoint := testEndpoint(t, simulation, "127.0.0.1:8000")
	failure := errors.New("send failed")
	a := testRPCNode(t, failingSendTransport{endpoint, failure}, DefaultConfig())
	peer := rpcContact("127.0.0.1:8001")
	if _, err := a.Ping(context.Background(), &peer); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	requireNoPending(t, a.network)
	for _, peer := range []*Contact{nil, {}, {ID: &KademliaID{}, Address: "invalid"}, {ID: &KademliaID{}, Address: "127.0.0.1:8001"}} {
		if _, err := a.Ping(context.Background(), peer); err == nil {
			t.Fatal("invalid contact accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.Ping(ctx, &peer); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := a.network.call(context.Background(), &peer, rpcPing, json.RawMessage("{")); err == nil {
		t.Fatal("invalid payload accepted")
	}
	requireNoPending(t, a.network)
	a.Close()
	if _, err := a.Ping(context.Background(), &peer); !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	offline, err := NewKademlia("127.0.0.1:9000", DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer offline.Close()
	if _, err := offline.Ping(context.Background(), &peer); err == nil {
		t.Fatal("offline node sent PING")
	}
	if _, err := NewNetwork(nil, DefaultConfig()); err == nil {
		t.Fatal("nil transport accepted")
	}
	if _, err := NewNetwork(endpoint, Config{}); err == nil {
		t.Fatal("invalid config accepted")
	}
}

func TestDecodeRPCValidation(t *testing.T) {
	sender := rpcContact("127.0.0.1:8000")
	valid := rpcMessage{RequestID: strings.Repeat("a", 32), Type: rpcPing, Sender: sender}
	for _, change := range []func(*rpcMessage){
		func(m *rpcMessage) { m.RequestID = "short" },
		func(m *rpcMessage) { m.RequestID = strings.Repeat("z", 32) },
		func(m *rpcMessage) { m.Sender.ID = nil },
		func(m *rpcMessage) { m.Sender.Address = "127.0.0.1:9999" },
	} {
		message := valid
		change(&message)
		data, _ := json.Marshal(message)
		if _, ok := decodeRPC(Packet{From: sender.Address, Data: data}); ok {
			t.Fatal("malformed envelope accepted")
		}
	}
	data, _ := json.Marshal(valid)
	if _, ok := decodeRPC(Packet{From: "invalid", Data: data}); ok {
		t.Fatal("invalid source accepted")
	}
	if _, ok := decodeRPC(Packet{From: sender.Address, Data: make([]byte, MaxDatagramSize+1)}); ok {
		t.Fatal("oversized packet accepted")
	}
}

func TestUnknownRPCDoesNotStopReceiver(t *testing.T) {
	simulation := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	a := testRPCNode(t, testEndpoint(t, simulation, "127.0.0.1:8000"), DefaultConfig())
	b := testEndpoint(t, simulation, "127.0.0.1:8001")
	request := rpcMessage{RequestID: strings.Repeat("a", 32), Type: "UNKNOWN", Sender: rpcContact(b.LocalAddr())}
	sendRPC(t, b, a.me.Address, request)
	request.Type = rpcPing
	sendRPC(t, b, a.me.Address, request)
	response := readRPC(t, b)
	if !response.Response || response.Type != rpcPing {
		t.Fatal("receiver did not process PING")
	}
}
