package kademlia

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
)

const rpcFindValue = "FIND_VALUE"

type findValueResponse struct {
	Found    *bool     `json:"found"`
	Data     []byte    `json:"data"`
	Contacts []Contact `json:"contacts"`
}

// SendFindDataMessage returns either a verified value or contacts to continue
// searching. found distinguishes an empty value from a missing value.
func (network *Network) SendFindDataMessage(ctx context.Context, peer *Contact, key *KademliaID) (data []byte, contacts []Contact, found bool, err error) {
	if key == nil {
		return nil, nil, false, fmt.Errorf("FIND_VALUE requires a key")
	}
	expected := *key
	payload, err := json.Marshal(findNodeRequest{Target: expected.String()})
	if err != nil {
		return nil, nil, false, err
	}
	response, err := network.call(ctx, peer, rpcFindValue, payload)
	if err != nil {
		return nil, nil, false, err
	}
	var result findValueResponse
	if err := json.Unmarshal(response.Payload, &result); err != nil {
		return nil, nil, false, fmt.Errorf("invalid FIND_VALUE response: %w", err)
	}
	if result.Found == nil {
		return nil, nil, false, fmt.Errorf("FIND_VALUE response requires found")
	}
	if *result.Found {
		if result.Data == nil || result.Contacts != nil {
			return nil, nil, false, fmt.Errorf("invalid FIND_VALUE value response")
		}
		if len(result.Data) > network.config.MaxValueSize {
			return nil, nil, false, ErrValueTooLarge
		}
		if KademliaID(sha256.Sum256(result.Data)) != expected {
			log.Printf("FIND_VALUE rejected: key=%s peer=%s error=%v", expected.String(), response.Sender.Address, ErrHashMismatch)
			return nil, nil, false, ErrHashMismatch
		}
		return result.Data, nil, true, nil
	}
	if result.Data != nil || result.Contacts == nil || len(result.Contacts) > network.config.K {
		return nil, nil, false, fmt.Errorf("invalid FIND_VALUE contact response")
	}
	seen := make(map[KademliaID]bool)
	for _, contact := range result.Contacts {
		candidate, err := validatedContact(contact)
		if err != nil {
			return nil, nil, false, fmt.Errorf("invalid FIND_VALUE contact: %w", err)
		}
		if candidate.ID.Equals(network.me.ID) || seen[*candidate.ID] {
			continue
		}
		seen[*candidate.ID] = true
		contacts = append(contacts, candidate)
	}
	return nil, contacts, false, nil
}

// A FIND_VALUE handler only examines local state; the caller drives iteration.
func (network *Network) findValuePayload(payload json.RawMessage, requester *KademliaID) (json.RawMessage, error) {
	var request findNodeRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return nil, err
	}
	key, err := NewKademliaID(request.Target)
	if err != nil {
		return nil, err
	}
	data, found := network.dataStore.get(*key)
	result := findValueResponse{Found: &found}
	if found {
		result.Data = append([]byte{}, data...)
	} else {
		// Reuse the same nearest-contact selection and requester exclusion.
		encoded, err := network.findNodePayload(payload, requester)
		if err != nil {
			return nil, err
		}
		var nearest findNodeResponse
		if err := json.Unmarshal(encoded, &nearest); err != nil {
			return nil, err
		}
		result.Contacts = nearest.Contacts
	}
	return json.Marshal(result)
}
