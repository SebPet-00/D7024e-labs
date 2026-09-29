package kademlia

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"net"
)

var ErrValueNotFound = errors.New("value not found among the closest reachable peers")

// ValueResult identifies both the verified bytes and the node that supplied
// them. Data and Source are owned by the caller.
type ValueResult struct {
	Data   []byte
	Source Contact
}

// LookupData checks local storage before iteratively querying peers. A successful
// empty value has a non-nil result and nil error. Results are not cached.
func (kademlia *Kademlia) LookupData(ctx context.Context, hash string) (result *ValueResult, err error) {
	ctx, trace := kademlia.startLookup(ctx, rpcFindValue, hash)
	defer func() { trace.finish(err) }()
	key, err := NewKademliaID(hash)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-kademlia.done:
		return nil, net.ErrClosed
	default:
	}
	if data, found := kademlia.dataStore.get(*key); found {
		if KademliaID(sha256.Sum256(data)) != *key {
			log.Printf("FIND_VALUE rejected local data: key=%s error=%v", key.String(), ErrHashMismatch)
			return nil, ErrHashMismatch
		}
		return &ValueResult{Data: data, Source: kademlia.Contact()}, nil
	}
	if kademlia.network == nil {
		return nil, fmt.Errorf("node has no transport")
	}
	_, result, err = kademlia.lookup(ctx, key, true)
	return result, err
}
