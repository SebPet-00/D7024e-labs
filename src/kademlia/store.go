package kademlia

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
)

// Store publishes data under its SHA-256 key to up to K closest discovered
// responsive nodes, including this node if it is among those closest.
// An isolated listening node stores locally. A small network may use fewer
// than K nodes; nil error means every selected target acknowledged.
//
// A valid key is returned even when lookup or some writes fail. Writes already
// performed are not rolled back, and an unacknowledged write may have succeeded
// if its reply was lost. Callers must check the error before reporting success.
func (kademlia *Kademlia) Store(ctx context.Context, data []byte) (*KademliaID, error) {
	if kademlia.network == nil {
		return nil, fmt.Errorf("node has no transport")
	}
	if err := kademlia.lookupStopped(ctx); err != nil {
		return nil, err
	}
	if len(data) > kademlia.config.MaxValueSize {
		return nil, fmt.Errorf("%w: maximum is %d bytes", ErrValueTooLarge, kademlia.config.MaxValueSize)
	}
	value := append([]byte(nil), data...)
	key := KademliaID(sha256.Sum256(value))
	peers, err := kademlia.LookupContact(ctx, &key)
	if err != nil && !errors.Is(err, ErrNoReachableContacts) {
		return &key, err
	}

	var candidates ContactCandidates
	peers = append(peers, kademlia.Contact())
	for i := range peers {
		peers[i].CalcDistance(&key)
	}
	candidates.Append(peers)
	candidates.Sort()
	targets := candidates.GetContacts(min(kademlia.config.K, candidates.Len()))

	acknowledged := 0
	var failures []error
	for start := 0; start < len(targets); start += kademlia.config.Alpha {
		if err := kademlia.lookupStopped(ctx); err != nil {
			failures = append(failures, err)
			break
		}
		batch := targets[start:min(start+kademlia.config.Alpha, len(targets))]
		results := make(chan error, len(batch))
		for _, target := range batch {
			go func(target Contact) {
				err := kademlia.lookupStopped(ctx)
				if err == nil {
					if target.ID.Equals(kademlia.me.ID) {
						err = kademlia.dataStore.put(key, value)
					} else {
						err = kademlia.network.SendStoreMessage(ctx, &target, &key, value)
					}
				}
				if err != nil {
					err = fmt.Errorf("%s: %w", target.Address, err)
				}
				results <- err
			}(target)
		}
		// Drain all workers even on cancellation; writes can be partially applied.
		for range batch {
			if err := <-results; err != nil {
				failures = append(failures, err)
			} else {
				acknowledged++
			}
		}
	}
	if len(failures) > 0 {
		return &key, fmt.Errorf("%d of %d storage targets acknowledged: %w",
			acknowledged, len(targets), errors.Join(failures...))
	}
	return &key, nil
}
