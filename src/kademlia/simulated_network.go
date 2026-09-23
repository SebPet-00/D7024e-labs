package kademlia

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"sync"
	"time"
)

// SimulatedNetworkConfig controls one-way packet delivery.
type SimulatedNetworkConfig struct {
	Latency    time.Duration
	PacketLoss float64 // Probability in [0, 1], applied independently per send.
	Seed       int64
}

// SimulatedNetwork connects in-process endpoints without opening sockets.
// One mutex protects endpoint registration, delivery, timers and the RNG.
// A seed repeats loss decisions for the same send order. Concurrent goroutine
// scheduling and wall-clock timing are not made deterministic by the seed.
type SimulatedNetwork struct {
	mu        sync.Mutex
	config    SimulatedNetworkConfig
	random    *rand.Rand
	endpoints map[string]*SimulatedTransport
}

// SimulatedTransport is one node's endpoint on a shared simulated network.
type SimulatedTransport struct {
	network *SimulatedNetwork
	address string
	inbox   chan Packet
	done    chan struct{}
	closed  bool                     // Protected by network.mu.
	timers  map[*time.Timer]struct{} // Incoming packets awaiting delivery.
}

const simulatedInboxSize = 256

var _ Transport = (*SimulatedTransport)(nil)

func NewSimulatedNetwork(config SimulatedNetworkConfig) (*SimulatedNetwork, error) {
	if config.Latency < 0 {
		return nil, fmt.Errorf("simulated latency must not be negative")
	}
	if math.IsNaN(config.PacketLoss) || config.PacketLoss < 0 || config.PacketLoss > 1 {
		return nil, fmt.Errorf("packet loss must be between zero and one")
	}
	return &SimulatedNetwork{
		config:    config,
		random:    rand.New(rand.NewSource(config.Seed)),
		endpoints: make(map[string]*SimulatedTransport),
	}, nil
}

// Listen registers an address. Closing its transport frees it for reuse.
func (network *SimulatedNetwork) Listen(address string) (*SimulatedTransport, error) {
	address, err := canonicalAddress(address)
	if err != nil {
		return nil, err
	}
	network.mu.Lock()
	defer network.mu.Unlock()
	if _, exists := network.endpoints[address]; exists {
		return nil, fmt.Errorf("address already in use: %s", address)
	}
	transport := &SimulatedTransport{
		network: network,
		address: address,
		inbox:   make(chan Packet, simulatedInboxSize),
		done:    make(chan struct{}),
		timers:  make(map[*time.Timer]struct{}),
	}
	network.endpoints[address] = transport
	return transport, nil
}

func (transport *SimulatedTransport) LocalAddr() string {
	return transport.address
}

// Send drops packets silently for packet loss, absent destinations or full
// receive buffers. Successful sends therefore do not prove a peer is alive.
func (transport *SimulatedTransport) Send(to string, data []byte) error {
	to, err := canonicalAddress(to)
	if err != nil {
		return err
	}
	if len(data) > MaxDatagramSize {
		return fmt.Errorf("datagram exceeds %d bytes", MaxDatagramSize)
	}
	network := transport.network
	network.mu.Lock()
	defer network.mu.Unlock()
	if transport.closed {
		return net.ErrClosed
	}
	if network.random.Float64() < network.config.PacketLoss {
		return nil
	}
	receiver := network.endpoints[to]
	if receiver == nil || len(receiver.inbox)+len(receiver.timers) >= simulatedInboxSize {
		return nil
	}
	packet := Packet{From: transport.address, Data: append([]byte(nil), data...)}
	if network.config.Latency == 0 {
		receiver.inbox <- packet
		return nil
	}

	// Timer callbacks acquire the same lock, so registration completes before
	// delivery can run. Closing the receiver cancels its outstanding timers.
	var timer *time.Timer
	timer = time.AfterFunc(network.config.Latency, func() {
		network.mu.Lock()
		defer network.mu.Unlock()
		delete(receiver.timers, timer)
		if receiver.closed {
			return
		}
		receiver.inbox <- packet
	})
	receiver.timers[timer] = struct{}{}
	return nil
}

func (transport *SimulatedTransport) Receive(ctx context.Context) (Packet, error) {
	if err := ctx.Err(); err != nil {
		return Packet{}, err
	}
	select {
	case <-transport.done:
		return Packet{}, net.ErrClosed
	default:
	}
	select {
	case <-ctx.Done():
		return Packet{}, ctx.Err()
	case <-transport.done:
		return Packet{}, net.ErrClosed
	case packet := <-transport.inbox:
		// Prefer shutdown if a queued packet and Close become ready together.
		select {
		case <-transport.done:
			return Packet{}, net.ErrClosed
		default:
			return packet, nil
		}
	}
}

func (transport *SimulatedTransport) Close() error {
	network := transport.network
	network.mu.Lock()
	defer network.mu.Unlock()
	if transport.closed {
		return nil
	}
	transport.closed = true
	delete(network.endpoints, transport.address)
	close(transport.done)
	for timer := range transport.timers {
		timer.Stop()
	}
	clear(transport.timers)
	return nil
}
