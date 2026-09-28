package kademlia

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"time"
)

// Join contacts a bootstrap address, looks up our own ID, then refreshes all
// buckets farther away than the closest discovered peer. The first node of a
// new network simply starts listening; it does not call Join on itself.
func (kademlia *Kademlia) Join(ctx context.Context, bootstrapAddress string) error {
	if err := kademlia.acquireMaintenance(ctx); err != nil {
		return err
	}
	defer func() { <-kademlia.maintenanceGate }()
	address, err := canonicalAddress(bootstrapAddress)
	if err != nil {
		return err
	}
	if address == kademlia.me.Address {
		return fmt.Errorf("bootstrap must be another node")
	}
	id := KademliaID(sha256.Sum256([]byte(address)))
	bootstrap := NewContact(&id, address)
	// Seed before contacting it so a failed bootstrap RPC removes the stale seed.
	if err := kademlia.AddContact(bootstrap); err != nil {
		return err
	}
	if _, err := kademlia.Ping(ctx, &bootstrap); err != nil {
		return fmt.Errorf("bootstrap ping: %w", err)
	}
	contacts, err := kademlia.LookupContact(ctx, kademlia.me.ID)
	if err != nil {
		return fmt.Errorf("join lookup: %w", err)
	}
	closestBucket := kademlia.routingTable.getBucketIndex(contacts[0].ID)
	// This implementation numbers the farthest bucket 0, so farther buckets
	// have LOWER indices. Empty buckets in this range must be refreshed too.
	for index := closestBucket - 1; index >= 0; index-- {
		if err := kademlia.refreshBucket(ctx, index); err != nil {
			return fmt.Errorf("join refresh bucket %d: %w", index, err)
		}
	}
	return nil
}

// Refresh performs lookups in stale bucket ranges, including empty ones.
// Successful and failed lookup attempts both reset a bucket's lookup timer.
func (kademlia *Kademlia) Refresh(ctx context.Context) error {
	if err := kademlia.acquireMaintenance(ctx); err != nil {
		return err
	}
	defer func() { <-kademlia.maintenanceGate }()
	if len(kademlia.routingTable.FindClosestContacts(kademlia.me.ID, 1)) == 0 {
		return nil // An isolated first node has no peer to ask yet.
	}
	for _, index := range kademlia.routingTable.staleBuckets(time.Now(), kademlia.config.RefreshPeriod) {
		if err := kademlia.refreshBucket(ctx, index); err != nil {
			return err
		}
	}
	return nil
}

func (kademlia *Kademlia) refreshBucket(ctx context.Context, index int) error {
	if err := kademlia.lookupStopped(ctx); err != nil {
		return err
	}
	target, err := randomBucketTarget(kademlia.me.ID, index)
	if err != nil {
		return err
	}
	_, err = kademlia.LookupContact(ctx, target)
	return err
}

// Serialize joining, refresh and replication; allow cancellation while waiting.
func (kademlia *Kademlia) acquireMaintenance(ctx context.Context) error {
	if kademlia.network == nil {
		return fmt.Errorf("node has no transport")
	}
	if err := kademlia.lookupStopped(ctx); err != nil {
		return err
	}
	select {
	case kademlia.maintenanceGate <- struct{}{}:
		if err := kademlia.lookupStopped(ctx); err != nil {
			<-kademlia.maintenanceGate
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-kademlia.done:
		return net.ErrClosed
	case <-kademlia.network.done:
		return net.ErrClosed
	}
}

func (kademlia *Kademlia) refreshLoop() {
	defer close(kademlia.refreshStopped)
	ticker := time.NewTicker(kademlia.config.RefreshPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-kademlia.done:
			return
		case <-kademlia.network.done:
			return
		case <-ticker.C:
			// A failed pass is retried at the next tick. Explicit Refresh/Join
			// return errors to their caller; an empty network is normal.
			_ = kademlia.Refresh(context.Background())
		}
	}
}

// observeContact never waits for a network response, so it is safe to call
// from the RPC receiver. The queue is bounded by one probe per bucket.
func (network *Network) observeContact(contact Contact) {
	probe := network.routingTable.observe(contact)
	if probe == nil {
		return
	}
	select {
	case network.evictions <- probe:
	case <-network.done:
		network.routingTable.finishProbe(probe, false)
	}
}

func (network *Network) evictionLoop() {
	defer close(network.evictionsStopped)
	for {
		select {
		case <-network.done:
			return
		case probe := <-network.evictions:
			if !network.routingTable.probeCurrent(probe) {
				network.routingTable.finishProbe(probe, false)
				continue
			}
			_, err := network.SendPingMessage(context.Background(), &probe.old)
			network.routingTable.finishProbe(probe, errors.Is(err, ErrRPCTimeout))
		}
	}
}
