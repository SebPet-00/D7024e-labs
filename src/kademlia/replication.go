package kademlia

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"time"
)

// Replicate republishes a snapshot of local values to their current closest
// nodes. Existing copies are retained, even if this node is no longer closest.
// Passes serialize with join/refresh. A failed value does not prevent others
// from being attempted; the next periodic pass retries failures.
func (kademlia *Kademlia) Replicate(ctx context.Context) error {
	if err := kademlia.acquireMaintenance(ctx); err != nil {
		return err
	}
	defer func() { <-kademlia.maintenanceGate }()
	var failures []error
	for key, data := range kademlia.dataStore.snapshot() {
		if err := kademlia.lookupStopped(ctx); err != nil {
			return errors.Join(append(failures, err)...)
		}
		// Never publish corrupt bytes under a newly computed, different key.
		if KademliaID(sha256.Sum256(data)) != key {
			failures = append(failures, fmt.Errorf("replicate %s: %w", key.String(), ErrHashMismatch))
			continue
		}
		if _, err := kademlia.Store(ctx, data); err != nil {
			failures = append(failures, fmt.Errorf("replicate %s: %w", key.String(), err))
		}
	}
	return errors.Join(failures...)
}

func (kademlia *Kademlia) replicationLoop() {
	defer close(kademlia.replicationStopped)
	ticker := time.NewTicker(kademlia.config.ReplicationPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-kademlia.done:
			return
		case <-kademlia.network.done:
			return
		case <-ticker.C:
			if err := kademlia.Replicate(context.Background()); err != nil {
				if kademlia.lookupStopped(context.Background()) != nil {
					return
				}
				log.Printf("replication failed: node=%s error=%v", kademlia.me.Address, err)
			}
		}
	}
}
