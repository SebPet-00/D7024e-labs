package kademlia

import (
	"crypto/sha256"
	"fmt"
	"sync"
)

// Kademlia owns the state of one node. Create it with NewKademlia.
// A Kademlia must not be copied after use.
type Kademlia struct {
	me           Contact
	config       Config
	routingTable *RoutingTable
	network      Transport // Nil until a transport is supplied.

	dataMu    sync.RWMutex // Protects dataStore when storage is implemented.
	dataStore map[KademliaID][]byte

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
		me:           me,
		config:       config,
		routingTable: newRoutingTable(me, config.K),
		dataStore:    make(map[KademliaID][]byte),
		done:         make(chan struct{}),
	}, nil
}

// NewKademliaWithTransport initializes a node using an already bound transport.
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
	node.network = transport
	return node, nil
}

// Contact returns the node's address and a copy of its ID.
func (kademlia *Kademlia) Contact() Contact {
	id := *kademlia.me.ID
	return NewContact(&id, kademlia.me.Address)
}

// Close signals shutdown. It is safe to call more than once or concurrently.
// It also closes the transport to unblock any pending receive.
func (kademlia *Kademlia) Close() {
	kademlia.closeOnce.Do(func() {
		close(kademlia.done)
		if kademlia.network != nil {
			_ = kademlia.network.Close()
		}
	})
}

func (kademlia *Kademlia) LookupContact(target *Contact) {
	// TODO
}

func (kademlia *Kademlia) LookupData(hash string) {
	// TODO
}

func (kademlia *Kademlia) Store(data []byte) {
	// TODO
}
