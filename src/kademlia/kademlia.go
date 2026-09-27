package kademlia

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"time"
)

// Kademlia owns the state of one node. Create it with NewKademlia.
// A Kademlia must not be copied after use.
type Kademlia struct {
	maintenanceGate chan struct{}
	refreshStopped  chan struct{}
	me              Contact
	config          Config
	routingTable    *RoutingTable
	network         *Network // Nil until a transport is supplied.

	dataStore *valueStore // Shared with the RPC handler; owns its own lock.

	done      chan struct{}
	closeOnce sync.Once
}

// NewKademlia initializes a node without opening sockets or starting goroutines.
// address must be a concrete IP:port (IPv6 uses [IP]:port), not a hostname.
// The node ID is SHA-256 of the canonical address, for example "127.0.0.1:8000".
func NewKademlia(address string, config Config) (*Kademlia, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	address, err := canonicalAddress(address)
	if err != nil {
		return nil, err
	}
	id := KademliaID(sha256.Sum256([]byte(address)))
	me := NewContact(&id, address)

	return &Kademlia{
		maintenanceGate: make(chan struct{}, 1),
		refreshStopped:  make(chan struct{}),
		me:              me,
		config:          config,
		routingTable:    newRoutingTable(me, config.K),
		dataStore:       newValueStore(config.MaxValueSize),
		done:            make(chan struct{}),
	}, nil
}

// NewKademliaWithTransport starts RPC and periodic refresh on a bound transport.
// Ownership transfers to the node on success: Close will close the transport.
// On error, the caller still owns the transport and must close it.
func NewKademliaWithTransport(transport Transport, config Config) (*Kademlia, error) {
	if transport == nil {
		return nil, fmt.Errorf("transport must not be nil")
	}
	node, err := NewKademlia(transport.LocalAddr(), config)
	if err != nil {
		return nil, err
	}
	node.network, err = newNetwork(transport, config, node.routingTable, node.dataStore)
	if err != nil {
		return nil, err
	}
	go node.refreshLoop()
	return node, nil
}

// Contact returns the node's address and a copy of its ID.
func (kademlia *Kademlia) Contact() Contact {
	id := *kademlia.me.ID
	return NewContact(&id, kademlia.me.Address)
}

// Close signals shutdown. It is safe to call more than once or concurrently.
// It closes the transport and waits for RPC, eviction and refresh workers.
func (kademlia *Kademlia) Close() {
	kademlia.closeOnce.Do(func() {
		close(kademlia.done)
		if kademlia.network != nil {
			kademlia.network.Close()
			<-kademlia.refreshStopped
		}
	})
}

func (kademlia *Kademlia) LookupData(hash string) {
	// TODO
}

// Ping sends a PING RPC to a peer and returns its elapsed round-trip time.
func (kademlia *Kademlia) Ping(ctx context.Context, contact *Contact) (time.Duration, error) {
	if kademlia.network == nil {
		return 0, fmt.Errorf("node has no transport")
	}
	return kademlia.network.SendPingMessage(ctx, contact)
}

// AddContact seeds a known peer locally. It validates identity but does not
// check liveness or perform the full joining procedure.
func (kademlia *Kademlia) AddContact(contact Contact) error {
	contact, err := validatedContact(contact)
	if err != nil {
		return err
	}
	kademlia.routingTable.AddContact(contact)
	return nil
}
