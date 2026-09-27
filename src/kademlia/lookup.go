package kademlia

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"time"
)

var ErrNoReachableContacts = errors.New("lookup found no reachable contacts")

type lookupState uint8

const (
	lookupUnqueried lookupState = iota
	lookupPending
	lookupResponded
	lookupFailed
)

type lookupCandidate struct {
	contact Contact
	state   lookupState
}

type lookupReply struct {
	candidate *lookupCandidate
	contacts  []Contact
	err       error
}

// LookupContact returns up to K responsive peers closest to target by XOR
// distance. The target is an ID, not necessarily an existing node.
// It uses strict parallelism: complete a batch of up to Alpha probes before
// selecting the next batch. RPC retries are handled by Network.
//
// Stop when every candidate in the current K closest non-failed contacts has
// replied. A round without a closer discovery is not enough to stop: the
// remaining nearest candidates must still be probed. Keep farther candidates
// as replacements for failures. The local node is never included in results.
func (kademlia *Kademlia) LookupContact(ctx context.Context, target *KademliaID) ([]Contact, error) {
	if target == nil {
		return nil, fmt.Errorf("lookup requires a target ID")
	}
	if kademlia.network == nil {
		return nil, fmt.Errorf("node has no transport")
	}
	if err := kademlia.lookupStopped(ctx); err != nil {
		return nil, err
	}
	targetID := *target
	kademlia.routingTable.markLookup(&targetID, time.Now())
	candidates := make(map[KademliaID]*lookupCandidate)
	add := func(contact Contact) {
		contact, err := validatedContact(contact)
		if err != nil || contact.ID.Equals(kademlia.me.ID) {
			return
		}
		if _, exists := candidates[*contact.ID]; exists {
			return
		}
		contact.CalcDistance(&targetID)
		candidates[*contact.ID] = &lookupCandidate{contact: contact}
	}
	// Include farther known contacts as fallbacks if the first K are offline.
	for _, contact := range kademlia.routingTable.FindClosestContacts(&targetID, math.MaxInt) {
		add(contact)
	}

	for {
		if err := kademlia.lookupStopped(ctx); err != nil {
			return nil, err
		}
		shortlist := make([]*lookupCandidate, 0, len(candidates))
		for _, candidate := range candidates {
			if candidate.state != lookupFailed {
				shortlist = append(shortlist, candidate)
			}
		}
		sort.Slice(shortlist, func(i, j int) bool {
			return shortlist[i].contact.Less(&shortlist[j].contact)
		})
		if len(shortlist) > kademlia.config.K {
			shortlist = shortlist[:kademlia.config.K]
		}

		var batch []*lookupCandidate
		for _, candidate := range shortlist {
			if candidate.state == lookupUnqueried {
				candidate.state = lookupPending
				batch = append(batch, candidate)
				if len(batch) == kademlia.config.Alpha {
					break
				}
			}
		}
		if len(batch) == 0 {
			if len(shortlist) == 0 {
				return nil, ErrNoReachableContacts
			}
			result := make([]Contact, 0, len(shortlist))
			for _, candidate := range shortlist {
				result = append(result, cloneContact(candidate.contact))
			}
			return result, nil
		}

		replies := make(chan lookupReply, len(batch))
		for _, candidate := range batch {
			go func(candidate *lookupCandidate) {
				contacts, err := kademlia.network.SendFindContactMessage(ctx, &candidate.contact, &targetID)
				replies <- lookupReply{candidate, contacts, err}
			}(candidate)
		}
		// Draining the batch also joins all lookup workers before returning.
		// Context cancellation and node closure interrupt each RPC.
		for range batch {
			reply := <-replies
			if reply.err != nil {
				reply.candidate.state = lookupFailed
				continue
			}
			reply.candidate.state = lookupResponded
			for _, contact := range reply.contacts {
				add(contact)
			}
		}
	}
}

func (kademlia *Kademlia) lookupStopped(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-kademlia.done:
		return net.ErrClosed
	case <-kademlia.network.done:
		return net.ErrClosed
	default:
		return nil
	}
}
