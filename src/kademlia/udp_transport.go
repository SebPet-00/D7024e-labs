package kademlia

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync"
)

// UDPTransport implements Transport using a UDP socket and a bounded inbox.
type UDPTransport struct {
	conn      *net.UDPConn
	address   string
	inbox     chan Packet
	done      chan struct{}
	stopped   chan struct{}
	closeOnce sync.Once
	closeErr  error
}

var _ Transport = (*UDPTransport)(nil)

// Listen binds a concrete local IP. Port zero lets the OS choose a free port;
// LocalAddr returns the actual address used to derive the node ID.
func Listen(ip string, port int) (*UDPTransport, error) {
	address, err := netip.ParseAddr(ip)
	if err != nil {
		return nil, fmt.Errorf("invalid listen IP: %w", err)
	}
	address = address.Unmap()
	if address.IsUnspecified() || address.IsMulticast() || address.Zone() != "" {
		return nil, fmt.Errorf("listen requires a concrete unicast IP without a zone")
	}
	if port < 0 || port > 65535 {
		return nil, fmt.Errorf("port must be between 0 and 65535")
	}
	bind := netip.AddrPortFrom(address.Unmap(), uint16(port))
	conn, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(bind))
	if err != nil {
		return nil, err
	}
	local := conn.LocalAddr().(*net.UDPAddr).AddrPort()
	transport := &UDPTransport{
		conn:    conn,
		address: netip.AddrPortFrom(local.Addr().Unmap(), local.Port()).String(),
		inbox:   make(chan Packet, 256),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go transport.readLoop()
	return transport, nil
}

func (transport *UDPTransport) LocalAddr() string {
	return transport.address
}

func (transport *UDPTransport) Send(to string, data []byte) error {
	address, err := canonicalAddress(to)
	if err != nil {
		return err
	}
	if len(data) > MaxDatagramSize {
		return fmt.Errorf("datagram exceeds %d bytes", MaxDatagramSize)
	}
	destination, err := netip.ParseAddrPort(address)
	if err != nil {
		return err
	}
	// WriteToUDPAddrPort consumes data before returning; no caller-owned
	// slice is retained by the transport.
	_, err = transport.conn.WriteToUDPAddrPort(data, destination)
	return err
}

func (transport *UDPTransport) Receive(ctx context.Context) (Packet, error) {
	return receiveDatagram(ctx, transport.inbox, transport.done)
}

func (transport *UDPTransport) readLoop() {
	defer close(transport.stopped)
	buffer := make([]byte, MaxDatagramSize+1)
	for {
		size, from, err := transport.conn.ReadFromUDPAddrPort(buffer)
		if err != nil {
			transport.shutdown()
			return
		}
		if size > MaxDatagramSize {
			continue
		}
		packet := Packet{
			From: netip.AddrPortFrom(from.Addr().Unmap(), from.Port()).String(),
			Data: append([]byte(nil), buffer[:size]...),
		}
		select {
		case transport.inbox <- packet:
		case <-transport.done:
			return
		default:
			// Like UDP socket buffers, a full application inbox loses packets.
		}
	}
}

func (transport *UDPTransport) shutdown() {
	transport.closeOnce.Do(func() {
		close(transport.done)
		transport.closeErr = transport.conn.Close()
	})
}

// Close releases the socket and waits for the reader goroutine to exit.
func (transport *UDPTransport) Close() error {
	transport.shutdown()
	<-transport.stopped
	return transport.closeErr
}
