package kademlia

import (
	"context"
	"fmt"
	"net/netip"
)

// MaxDatagramSize is the IPv4 UDP payload limit, including encoded message
// headers. The configured value size is a separate limit on stored data.
const MaxDatagramSize = 65507

// Packet is one received datagram. Data belongs to the receiver.
type Packet struct {
	From string
	Data []byte
}

// Transport carries datagrams, not RPCs. Implementations must be safe for
// concurrent use. Send copies data before returning and does not promise
// delivery. Close is idempotent and unblocks Receive. Send and Receive report
// net.ErrClosed for a closed endpoint. Request IDs, responses, retries and RPC
// timeouts belong above this interface so both networks use the same RPC code.
type Transport interface {
	LocalAddr() string
	Send(to string, data []byte) error
	Receive(ctx context.Context) (Packet, error)
	Close() error
}

// canonicalAddress provides one encoding for both node IDs and packet routing.
func canonicalAddress(address string) (string, error) {
	endpoint, err := netip.ParseAddrPort(address)
	if err != nil {
		return "", fmt.Errorf("invalid node address: %w", err)
	}
	ip := endpoint.Addr().Unmap()
	if endpoint.Port() == 0 || ip.IsUnspecified() || ip.IsMulticast() || ip.Zone() != "" {
		return "", fmt.Errorf("node address must have a concrete unicast IP and a nonzero port without a zone")
	}
	return netip.AddrPortFrom(ip, endpoint.Port()).String(), nil
}
