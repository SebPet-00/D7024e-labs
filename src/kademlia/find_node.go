package kademlia

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
)

const rpcFindNode = "FIND_NODE"

type findNodeRequest struct {
	Target string `json:"target"`
}

type findNodeResponse struct {
	Contacts []Contact `json:"contacts"`
}

// SendFindContactMessage asks one peer for contacts near a target.
// The peer is the destination; target may be any key in the ID space.
func (network *Network) SendFindContactMessage(ctx context.Context, contact *Contact, target *KademliaID) ([]Contact, error) {
	if target == nil {
		return nil, fmt.Errorf("FIND_NODE requires a target ID")
	}
	payload, err := json.Marshal(findNodeRequest{Target: target.String()})
	if err != nil {
		return nil, err
	}
	response, err := network.call(ctx, contact, rpcFindNode, payload)
	if err != nil {
		return nil, err
	}
	var decoded findNodeResponse
	if err := json.Unmarshal(response.Payload, &decoded); err != nil {
		return nil, fmt.Errorf("invalid FIND_NODE response: %w", err)
	}
	if decoded.Contacts == nil || len(decoded.Contacts) > network.config.K {
		return nil, fmt.Errorf("FIND_NODE response must contain a list of at most K contacts")
	}
	contacts := make([]Contact, 0, len(decoded.Contacts))
	seen := make(map[KademliaID]bool)
	for _, candidate := range decoded.Contacts {
		candidate, err := validatedContact(candidate)
		if err != nil {
			return nil, fmt.Errorf("invalid FIND_NODE contact: %w", err)
		}
		if candidate.ID.Equals(network.me.ID) || seen[*candidate.ID] {
			continue
		}
		seen[*candidate.ID] = true
		contacts = append(contacts, candidate)
	}
	return contacts, nil
}

// findNodePayload returns only locally known contacts, never a recursive lookup.
// Exclude the requester and keep filling the result until K contacts are found.
func (network *Network) findNodePayload(payload json.RawMessage, requester *KademliaID) (json.RawMessage, error) {
	var request findNodeRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return nil, err
	}
	target, err := NewKademliaID(request.Target)
	if err != nil {
		return nil, err
	}
	contacts := make([]Contact, 0)
	for _, contact := range network.routingTable.FindClosestContacts(target, math.MaxInt) {
		if contact.ID.Equals(requester) {
			continue
		}
		contacts = append(contacts, contact)
		if len(contacts) == network.config.K {
			break
		}
	}
	return json.Marshal(findNodeResponse{Contacts: contacts})
}
