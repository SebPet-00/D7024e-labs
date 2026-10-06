package kademlia

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type registryRequest struct {
	Key  string         `json:"key"`
	Head *PackageRecord `json:"head,omitempty"`
}
type registryResponse struct {
	Key   string         `json:"key"`
	Head  *PackageRecord `json:"head,omitempty"`
	Error string         `json:"error,omitempty"`
}

func (r *Registry) call(ctx context.Context, peer Contact, method string, request registryRequest) (registryResponse, error) {
	payload, _ := json.Marshal(request)
	message, e := r.node.network.call(ctx, &peer, method, payload)
	if e != nil {
		return registryResponse{}, e
	}
	var response registryResponse
	if e = json.Unmarshal(message.Payload, &response); e != nil {
		return response, e
	}
	if response.Key != request.Key {
		return response, fmt.Errorf("wrong registry response key")
	}
	if response.Error != "" {
		return response, fmt.Errorf("registry rejected: %s", response.Error)
	}
	if method == "REGISTRY_UPDATE" && response.Head == nil {
		return response, fmt.Errorf("missing update acknowledgment")
	}
	return response, nil
}
func (r *Registry) send(ctx context.Context, peer Contact, head PackageRecord) error {
	key := latestKey(head.Domain, head.Package)
	response, e := r.call(ctx, peer, "REGISTRY_UPDATE", registryRequest{Key: key.String(), Head: &head})
	if e != nil {
		// An older publisher/replica learns the newer head from a rejection.
		// Keep reporting the rejected write, but catch up for the next pass.
		if response.Head != nil && response.Head.Domain == head.Domain &&
			response.Head.Package == head.Package && response.Head.Version > head.Version {
			if catchup := r.accept(ctx, *response.Head); catchup != nil {
				return errors.Join(e, catchup)
			}
		}
		return e
	}
	if string(recordBytes(*response.Head)) != string(recordBytes(head)) {
		return fmt.Errorf("wrong update acknowledgment")
	}
	return nil
}
func (r *Registry) distribute(ctx context.Context, head PackageRecord) error {
	key := latestKey(head.Domain, head.Package)
	peers, e := r.node.LookupContact(ctx, &key)
	if e != nil && !errors.Is(e, ErrNoReachableContacts) {
		return e
	}
	peers = append(peers, r.node.Contact())
	var candidates ContactCandidates
	for i := range peers {
		peers[i].CalcDistance(&key)
	}
	candidates.Append(peers)
	candidates.Sort()
	var failures []error
	// Send even a forced invalid update to receivers; each target validates it.
	for _, peer := range candidates.GetContacts(min(r.node.config.K, len(peers))) {
		if peer.ID.Equals(r.node.me.ID) {
			e = r.accept(ctx, head)
		} else {
			e = r.send(ctx, peer, head)
		}
		if e != nil {
			failures = append(failures, fmt.Errorf("%s: %w", peer.Address, e))
		}
	}
	return errors.Join(failures...)
}
func (network *Network) registryPayload(request rpcMessage) json.RawMessage {
	var input registryRequest
	if json.Unmarshal(request.Payload, &input) != nil {
		return nil
	}
	key, e := NewKademliaID(input.Key)
	if e != nil {
		return nil
	}
	result := registryResponse{Key: key.String()}
	r := network.registry
	if request.Type == "REGISTRY_GET" {
		for _, head := range r.snapshot() {
			if latestKey(head.Domain, head.Package) == *key {
				copy := head
				result.Head = &copy
				break
			}
		}
	} else {
		if input.Head == nil || latestKey(input.Head.Domain, input.Head.Package) != *key {
			e = fmt.Errorf("wrong latest-pointer key")
		} else {
			ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
			e = r.accept(ctx, *input.Head)
			cancel()
		}
		if e != nil {
			result.Error = e.Error()
			for _, current := range r.snapshot() {
				if latestKey(current.Domain, current.Package) == *key {
					copy := current
					result.Head = &copy
					break
				}
			}
		} else {
			result.Head = input.Head
		}
	}
	payload, _ := json.Marshal(result)
	return payload
}

// Update validation can fetch history. Bounded workers leave the receive loop
// free to process those replies; overload is handled by ordinary RPC retries.
func (network *Network) dispatchRegistry(request rpcMessage, from string) {
	select {
	case network.registrySlots <- struct{}{}:
		network.registryWorkers.Add(1)
		go func() {
			defer network.registryWorkers.Done()
			defer func() { <-network.registrySlots }()
			network.handleRequest(request, from)
		}()
	default:
	}
}
