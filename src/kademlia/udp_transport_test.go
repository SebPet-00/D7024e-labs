package kademlia

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"
)

func testUDP(t *testing.T) *UDPTransport {
	t.Helper()
	transport, err := Listen("127.0.0.1", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Close() })
	return transport
}

func TestUDPPacketDelivery(t *testing.T) {
	a, b := testUDP(t), testUDP(t)
	data := []byte{0, 1, 255}
	if err := a.Send(b.LocalAddr(), data); err != nil {
		t.Fatal(err)
	}
	data[0] = 99
	packet := receivePacket(t, b)
	if packet.From != a.LocalAddr() || len(packet.Data) != 3 || packet.Data[0] != 0 || packet.Data[2] != 255 {
		t.Fatalf("wrong UDP packet: %+v", packet)
	}
	if err := b.Send(a.LocalAddr(), []byte("reply")); err != nil {
		t.Fatal(err)
	}
	if string(receivePacket(t, a).Data) != "reply" {
		t.Fatal("wrong reply")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := a.Receive(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

func TestPingUDP(t *testing.T) {
	a := testRPCNode(t, testUDP(t), DefaultConfig())
	b := testRPCNode(t, testUDP(t), DefaultConfig())
	peer := b.Contact()
	if elapsed, err := a.Ping(context.Background(), &peer); err != nil || elapsed <= 0 {
		t.Fatalf("UDP PING: elapsed=%s error=%v", elapsed, err)
	}
	peer = a.Contact()
	if _, err := b.Ping(context.Background(), &peer); err != nil {
		t.Fatal(err)
	}
}

func TestUDPValidationAndClose(t *testing.T) {
	for _, ip := range []string{"invalid", "0.0.0.0", "::ffff:0.0.0.0", "224.0.0.1", "fe80::1%eth0"} {
		if transport, err := Listen(ip, 0); err == nil {
			_ = transport.Close()
			t.Fatalf("invalid listen address accepted: %s", ip)
		}
	}
	for _, port := range []int{-1, 65536} {
		if transport, err := Listen("127.0.0.1", port); err == nil {
			_ = transport.Close()
			t.Fatal("invalid port accepted")
		}
	}
	a := testUDP(t)
	address, err := netip.ParseAddrPort(a.LocalAddr())
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, err := Listen("127.0.0.1", int(address.Port())); err == nil {
		_ = duplicate.Close()
		t.Fatal("duplicate UDP bind accepted")
	}
	if err := a.Send("invalid", nil); err == nil {
		t.Fatal("invalid destination accepted")
	}
	if err := a.Send(a.LocalAddr(), make([]byte, MaxDatagramSize+1)); err == nil {
		t.Fatal("oversized datagram accepted")
	}
	result := make(chan error, 1)
	go func() { _, err := a.Receive(context.Background()); result <- err }()
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock Receive")
	}
	if err := a.Send(a.LocalAddr(), nil); !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
}
