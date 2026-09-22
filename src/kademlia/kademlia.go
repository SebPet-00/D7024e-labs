package kademlia

import (
	"crypto/sha256"
	"fmt"
	"net/netip"
	"sync"
)

// Kademlia owns the state of one node. Create it with NewKademlia.
// A Kademlia must not be copied after use.
type Kademlia struct {
	me           Contact
	config       Config
	routingTable *RoutingTable
	network      *Network // Replaced by a transport interface in the network step.

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
	endpoint, err := netip.ParseAddrPort(address)
	if err != nil {
		return nil, fmt.Errorf("invalid node address: %w", err)
	}
	ip := endpoint.Addr().Unmap()
	if endpoint.Port() == 0 || ip.IsUnspecified() || ip.IsMulticast() || ip.Zone() != "" {
		return nil, fmt.Errorf("node address must have a concrete unicast IP and a nonzero port without a zone")
	}
	address = netip.AddrPortFrom(ip, endpoint.Port()).String()
	id := KademliaID(sha256.Sum256([]byte(address)))
	me := NewContact(&id, address)

	return &Kademlia{
		me:           me,
		config:       config,
		routingTable: newRoutingTable(me, config.K),
		network:      &Network{},
		dataStore:    make(map[KademliaID][]byte),
		done:         make(chan struct{}),
	}, nil
}

// Contact returns the node's address and a copy of its ID.
func (kademlia *Kademlia) Contact() Contact {
	id := *kademlia.me.ID
	return NewContact(&id, kademlia.me.Address)
}

// Close signals shutdown. It is safe to call more than once or concurrently.
// Future background workers will observe done; there are none yet. 
func (kademlia *Kademlia) Close() {
	kademlia.closeOnce.Do(func() {
		close(kademlia.done)
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
