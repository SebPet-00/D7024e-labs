package kademlia

import (
	"crypto/sha256"
	"sync"
	"testing"
)

func TestNewKademlia(t *testing.T) {
	config := DefaultConfig()
	if config.K != 10 || config.Alpha != 3 {
		t.Fatal("defaults must use K=10 and Alpha=3")
	}
	node, err := NewKademlia("127.0.0.1:8000", config)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	want := KademliaID(sha256.Sum256([]byte("127.0.0.1:8000")))
	if !node.me.ID.Equals(&want) || node.me.Address != "127.0.0.1:8000" {
		t.Fatal("node identity does not match SHA-256(IP:port)")
	}
	if node.routingTable == nil || node.dataStore == nil {
		t.Fatal("node state was not initialized")
	}
	if len(node.routingTable.FindClosestContacts(&want, config.K)) != 0 {
		t.Fatal("a new routing table should be empty")
	}
	select {
	case <-node.done:
		t.Fatal("a new node must not be closed")
	default:
	}
	config.K = 99
	if node.config.K != 10 {
		t.Fatal("caller changes must not alter node configuration")
	}
	contact := node.Contact()
	contact.ID[0] ^= 0xff
	if !node.me.ID.Equals(&want) {
		t.Fatal("returned contact exposes the node's mutable ID")
	}
}

func TestNodeAddressCanonicalization(t *testing.T) {
	for _, pair := range [][2]string{
		{"[0:0:0:0:0:0:0:1]:8000", "[::1]:8000"},
		{"[::ffff:127.0.0.1]:8000", "127.0.0.1:8000"},
	} {
		first, err := NewKademlia(pair[0], DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		second, err := NewKademlia(pair[1], DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		if first.me.Address != pair[1] || !first.me.ID.Equals(second.me.ID) {
			t.Errorf("equivalent addresses have different identities: %v", pair)
		}
		first.Close()
		second.Close()
	}
	first, _ := NewKademlia("127.0.0.1:8000", DefaultConfig())
	second, _ := NewKademlia("127.0.0.1:8001", DefaultConfig())
	third, _ := NewKademlia("127.0.0.2:8000", DefaultConfig())
	defer first.Close()
	defer second.Close()
	defer third.Close()
	if first.me.ID.Equals(second.me.ID) || first.me.ID.Equals(third.me.ID) {
		t.Fatal("changing IP or port should change identity")
	}
}

func TestNewKademliaRejectsInvalidAddress(t *testing.T) {
	for _, address := range []string{
		"", "localhost:8000", "127.0.0.1", "127.0.0.1:0",
		"127.0.0.1:65536", "0.0.0.0:8000", "[::]:8000",
		"224.0.0.1:8000", "[fe80::1%eth0]:8000",
	} {
		t.Run(address, func(t *testing.T) {
			node, err := NewKademlia(address, DefaultConfig())
			if err == nil || node != nil {
				t.Fatal("expected nil node and an address error")
			}
		})
	}
}

func TestNewKademliaRejectsInvalidConfig(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*Config)
	}{
		{"K", func(c *Config) { c.K = 0 }},
		{"Alpha", func(c *Config) { c.Alpha = 0 }},
		{"timeout", func(c *Config) { c.RPCTimeout = 0 }},
		{"retries", func(c *Config) { c.RPCRetries = -1 }},
		{"replication", func(c *Config) { c.ReplicationPeriod = 0 }},
		{"refresh", func(c *Config) { c.RefreshPeriod = 0 }},
		{"value size", func(c *Config) { c.MaxValueSize = 254 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := DefaultConfig()
			test.change(&config)
			node, err := NewKademlia("127.0.0.1:8000", config)
			if err == nil || node != nil {
				t.Fatal("expected nil node and a configuration error")
			}
		})
	}
	config := DefaultConfig()
	config.RPCRetries = 0
	config.MaxValueSize = 255
	node, err := NewKademlia("127.0.0.1:8000", config)
	if err != nil {
		t.Fatal(err)
	}
	node.Close()
}

func TestConfiguredBucketCapacity(t *testing.T) {
	config := DefaultConfig()
	config.K = 2
	node, err := NewKademlia("127.0.0.1:8000", config)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	for i := byte(0); i < 3; i++ {
		id := *node.me.ID
		id[0] ^= 0x80 // All contacts fall into bucket zero.
		id[IDLength-1] = i
		node.routingTable.AddContact(NewContact(&id, "127.0.0.1:9000"))
	}
	if got := node.routingTable.buckets[0].Len(); got != 2 {
		t.Fatalf("bucket holds %d contacts, want configured K=2", got)
	}
	defaultTable := NewRoutingTable(node.Contact())
	for i := byte(0); i < 11; i++ {
		id := *node.me.ID
		id[0] ^= 0x80
		id[IDLength-1] = i
		defaultTable.AddContact(NewContact(&id, "127.0.0.1:9000"))
	}
	if got := defaultTable.buckets[0].Len(); got != 10 {
		t.Fatalf("default bucket holds %d contacts, want 10", got)
	}
}

func TestCloseConcurrentAndIndependent(t *testing.T) {
	first, err := NewKademlia("127.0.0.1:8000", DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewKademlia("127.0.0.1:8001", DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	var workers sync.WaitGroup
	for i := 0; i < 20; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			first.Close()
		}()
	}
	workers.Wait()
	select {
	case <-first.done:
	default:
		t.Fatal("Close did not signal shutdown")
	}
	select {
	case <-second.done:
		t.Fatal("closing one node shut down another")
	default:
	}
	key := KademliaID{}
	first.dataStore[key] = []byte("first node")
	if len(second.dataStore) != 0 {
		t.Fatal("nodes share a data store")
	}
}
