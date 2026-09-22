package kademlia

import (
	"fmt"
	"time"
)

const defaultK = 10

// Config holds the settings for one node. Pass DefaultConfig() to start
// with the lab defaults, then change individual fields as needed.
type Config struct {
	K                 int
	Alpha             int
	RPCTimeout        time.Duration
	RPCRetries        int // Additional attempts after the first request; zero disables retries.
	ReplicationPeriod time.Duration
	RefreshPeriod     time.Duration
	MaxValueSize      int // Bytes of unencoded value data.
}

// DefaultConfig returns independent settings for a new node.
// Timeout, retries and maintenance periods are implementation choices.
func DefaultConfig() Config {
	return Config{
		K:                 defaultK,
		Alpha:             3,
		RPCTimeout:        2 * time.Second,
		RPCRetries:        2,
		ReplicationPeriod: time.Hour,
		RefreshPeriod:     time.Hour,
		MaxValueSize:      1024,
	}
}

func (config Config) validate() error {
	switch {
	case config.K < 1:
		return fmt.Errorf("K must be positive")
	case config.Alpha < 1:
		return fmt.Errorf("Alpha must be positive")
	case config.RPCTimeout <= 0:
		return fmt.Errorf("RPC timeout must be positive")
	case config.RPCRetries < 0:
		return fmt.Errorf("RPC retries must not be negative")
	case config.ReplicationPeriod <= 0:
		return fmt.Errorf("replication period must be positive")
	case config.RefreshPeriod <= 0:
		return fmt.Errorf("refresh period must be positive")
	case config.MaxValueSize < 255:
		return fmt.Errorf("maximum value size must be at least 255 bytes")
	}
	return nil
}
