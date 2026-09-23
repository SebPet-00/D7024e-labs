package kademlia

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"
)

func testSimulatedNetwork(t *testing.T, config SimulatedNetworkConfig) *SimulatedNetwork {
	t.Helper()
	network, err := NewSimulatedNetwork(config)
	if err != nil {
		t.Fatal(err)
	}
	return network
}

func testEndpoint(t *testing.T, network *SimulatedNetwork, address string) *SimulatedTransport {
	t.Helper()
	endpoint, err := network.Listen(address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	return endpoint
}

func receivePacket(t *testing.T, transport Transport) Packet {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	packet, err := transport.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return packet
}

func TestSimulatedPacketDelivery(t *testing.T) {
	network := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	a := testEndpoint(t, network, "127.0.0.1:8000")
	b := testEndpoint(t, network, "[::ffff:127.0.0.1]:8001")
	data := []byte{0, 1, 255}
	if err := a.Send("[::ffff:127.0.0.1]:8001", data); err != nil {
		t.Fatal(err)
	}
	data[0] = 99
	packet := receivePacket(t, b)
	if packet.From != a.LocalAddr() || !reflect.DeepEqual(packet.Data, []byte{0, 1, 255}) {
		t.Fatalf("wrong sender or data: %+v", packet)
	}
	if err := b.Send(packet.From, []byte("reply")); err != nil {
		t.Fatal(err)
	}
	if got := string(receivePacket(t, a).Data); got != "reply" {
		t.Fatalf("reply = %q", got)
	}
	if err := a.Send(a.LocalAddr(), nil); err != nil {
		t.Fatal(err)
	}
	if len(receivePacket(t, a).Data) != 0 {
		t.Fatal("empty datagram changed")
	}
}

func TestSimulatedLatency(t *testing.T) {
	latency := 40 * time.Millisecond
	network := testSimulatedNetwork(t, SimulatedNetworkConfig{Latency: latency})
	a := testEndpoint(t, network, "127.0.0.1:8000")
	b := testEndpoint(t, network, "127.0.0.1:8001")
	start := time.Now()
	if err := a.Send(b.LocalAddr(), []byte("delayed")); err != nil {
		t.Fatal(err)
	}
	if got := string(receivePacket(t, b).Data); got != "delayed" {
		t.Fatalf("received %q", got)
	}
	if time.Since(start) < latency {
		t.Fatal("packet arrived before configured latency")
	}
}

func TestSimulatedDropsAndCancellation(t *testing.T) {
	network := testSimulatedNetwork(t, SimulatedNetworkConfig{PacketLoss: 1})
	a := testEndpoint(t, network, "127.0.0.1:8000")
	b := testEndpoint(t, network, "127.0.0.1:8001")
	if err := a.Send(b.LocalAddr(), []byte("lost")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := b.Receive(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost packet should time out, got %v", err)
	}
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := b.Receive(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func lossPattern(t *testing.T, seed int64) []byte {
	t.Helper()
	network := testSimulatedNetwork(t, SimulatedNetworkConfig{PacketLoss: 0.5, Seed: seed})
	a := testEndpoint(t, network, "127.0.0.1:8000")
	b := testEndpoint(t, network, "127.0.0.1:8001")
	for i := 0; i < 100; i++ {
		if err := a.Send(b.LocalAddr(), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	// Zero-latency delivery has completed when Send returns. Drain the packets
	// through the interface; the final timeout marks the end of the sequence.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var delivered []byte
	for {
		packet, err := b.Receive(ctx)
		if errors.Is(err, context.DeadlineExceeded) {
			return delivered
		}
		if err != nil {
			t.Fatal(err)
		}
		delivered = append(delivered, packet.Data[0])
	}
}

func TestSimulatedLossIsSeeded(t *testing.T) {
	first := lossPattern(t, 42)
	if len(first) == 0 || len(first) == 100 {
		t.Fatal("expected both delivery and loss")
	}
	if !reflect.DeepEqual(first, lossPattern(t, 42)) {
		t.Fatal("same seed and send order produced different loss")
	}
	if reflect.DeepEqual(first, lossPattern(t, 43)) {
		t.Fatal("different seeds produced the same loss pattern")
	}
}

func TestSimulatedValidation(t *testing.T) {
	for _, config := range []SimulatedNetworkConfig{
		{Latency: -time.Second}, {PacketLoss: -0.1},
		{PacketLoss: 1.1}, {PacketLoss: math.NaN()},
	} {
		if network, err := NewSimulatedNetwork(config); err == nil || network != nil {
			t.Fatal("invalid simulation settings accepted")
		}
	}
	network := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	if _, err := network.Listen("localhost:8000"); err == nil {
		t.Fatal("invalid address accepted")
	}
	a := testEndpoint(t, network, "127.0.0.1:8000")
	if _, err := network.Listen("[::ffff:127.0.0.1]:8000"); err == nil {
		t.Fatal("duplicate canonical address accepted")
	}
	if err := a.Send("invalid", nil); err == nil {
		t.Fatal("invalid destination accepted")
	}
	if err := a.Send("127.0.0.1:8001", make([]byte, MaxDatagramSize+1)); err == nil {
		t.Fatal("oversized datagram accepted")
	}
	// An absent receiver looks like packet loss, not a local send failure.
	if err := a.Send("127.0.0.1:8001", []byte("missing")); err != nil {
		t.Fatal(err)
	}
}

func TestSimulatedFullInboxDropsWithoutBlocking(t *testing.T) {
	network := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	a := testEndpoint(t, network, "127.0.0.1:8000")
	b := testEndpoint(t, network, "127.0.0.1:8001")
	sent := make(chan error, 1)
	go func() {
		for i := 0; i <= simulatedInboxSize; i++ {
			if err := a.Send(b.LocalAddr(), []byte{byte(i)}); err != nil {
				sent <- err
				return
			}
		}
		sent <- nil
	}()
	select {
	case err := <-sent:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("send blocked on full inbox")
	}
	for i := 0; i < simulatedInboxSize; i++ {
		if got := receivePacket(t, b).Data[0]; got != byte(i) {
			t.Fatalf("packet %d replaced or reordered", i)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := b.Receive(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("overflow packet was not dropped: %v", err)
	}
}

func TestSimulatedCloseAndAddressReuse(t *testing.T) {
	network := testSimulatedNetwork(t, SimulatedNetworkConfig{Latency: time.Hour})
	a := testEndpoint(t, network, "127.0.0.1:8000")
	b := testEndpoint(t, network, "127.0.0.1:8001")
	if err := a.Send(b.LocalAddr(), []byte("in flight")); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := b.Receive(context.Background())
		result <- err
	}()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("receive returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not unblock Receive")
	}
	if err := b.Send(a.LocalAddr(), nil); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("closed sender returned %v", err)
	}
	network.mu.Lock()
	pending := len(b.timers)
	network.mu.Unlock()
	if pending != 0 {
		t.Fatal("Close retained pending delivery timers")
	}
	replacement := testEndpoint(t, network, b.LocalAddr())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := replacement.Receive(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("replacement endpoint inherited an old packet")
	}
}

func TestNodeTransportOwnership(t *testing.T) {
	if _, err := NewKademliaWithTransport(nil, DefaultConfig()); err == nil {
		t.Fatal("nil transport accepted")
	}
	network := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	a := testEndpoint(t, network, "127.0.0.1:8000")
	config := DefaultConfig()
	config.K = 0
	if _, err := NewKademliaWithTransport(a, config); err == nil {
		t.Fatal("invalid config accepted")
	}
	// Failed initialization must leave the caller's transport open.
	if err := a.Send(a.LocalAddr(), []byte("still open")); err != nil {
		t.Fatal(err)
	}
	receivePacket(t, a)
	node, err := NewKademliaWithTransport(a, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if node.Contact().Address != a.LocalAddr() || node.network != a {
		t.Fatal("node did not use supplied transport")
	}
	var workers sync.WaitGroup
	for i := 0; i < 10; i++ {
		workers.Add(1)
		go func() { defer workers.Done(); node.Close() }()
	}
	workers.Wait()
	if _, err := a.Receive(context.Background()); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("node Close did not close transport: %v", err)
	}
}

func TestSimulatedNetwork1000Nodes(t *testing.T) {
	// This checks transport scale, not full DHT lookups (implemented later).
	const count = 1000
	network := testSimulatedNetwork(t, SimulatedNetworkConfig{Seed: 1})
	nodes := make([]*Kademlia, count)
	for i := range nodes {
		endpoint := testEndpoint(t, network, fmt.Sprintf("127.0.0.1:%d", 10000+i))
		node, err := NewKademliaWithTransport(endpoint, DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = node
		t.Cleanup(node.Close)
	}
	var workers sync.WaitGroup
	for i, node := range nodes {
		workers.Add(1)
		go func(i int, node *Kademlia) {
			defer workers.Done()
			next := nodes[(i+1)%count]
			if err := node.network.Send(next.me.Address, []byte(node.me.Address)); err != nil {
				t.Errorf("send: %v", err)
			}
		}(i, node)
	}
	workers.Wait()
	for i, node := range nodes {
		packet := receivePacket(t, node.network)
		want := nodes[(i+count-1)%count].me.Address
		if packet.From != want || string(packet.Data) != want {
			t.Fatalf("node %d received wrong packet: %+v", i, packet)
		}
	}
}

func TestSimulatedConcurrentSendReceiveClose(t *testing.T) {
	network := testSimulatedNetwork(t, SimulatedNetworkConfig{Latency: time.Millisecond})
	a := testEndpoint(t, network, "127.0.0.1:8000")
	b := testEndpoint(t, network, "127.0.0.1:8001")
	var workers sync.WaitGroup
	for i := 0; i < 20; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 100; j++ {
				if err := a.Send(b.LocalAddr(), []byte("packet")); err != nil && !errors.Is(err, net.ErrClosed) {
					t.Errorf("send: %v", err)
				}
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			_, err := b.Receive(context.Background())
			if errors.Is(err, net.ErrClosed) {
				return
			}
			if err != nil {
				t.Errorf("receive: %v", err)
				return
			}
		}
	}()
	_ = a.Close()
	_ = b.Close()
	workers.Wait()
}
