package kademlia

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	rpcStore           = "STORE"
	storeHashMismatch  = "hash_mismatch"
	storeValueTooLarge = "value_too_large"
)

type storeRequest struct {
	Key  string `json:"key"`
	Data []byte `json:"data"` // JSON encodes binary data as base64.
}

type storeResponse struct {
	Key    string `json:"key"`
	Stored bool   `json:"stored"`
	Error  string `json:"error,omitempty"`
}

// SendStoreMessage requires an explicit acknowledgment for this key.
// The receiving node independently validates size and hash before writing.
func (network *Network) SendStoreMessage(ctx context.Context, contact *Contact, key *KademliaID, data []byte) error {
	if key == nil {
		return fmt.Errorf("STORE requires a key")
	}
	if len(data) > network.config.MaxValueSize {
		return ErrValueTooLarge
	}
	// Make the empty value a non-nil slice so it encodes as "" rather than null.
	value := append([]byte{}, data...)
	expectedKey := *key
	if KademliaID(sha256.Sum256(value)) != expectedKey {
		return ErrHashMismatch
	}
	payload, err := json.Marshal(storeRequest{Key: expectedKey.String(), Data: value})
	if err != nil {
		return err
	}
	response, err := network.call(ctx, contact, rpcStore, payload)
	if err != nil {
		return err
	}
	var result storeResponse
	if err := json.Unmarshal(response.Payload, &result); err != nil {
		return fmt.Errorf("invalid STORE response: %w", err)
	}
	if result.Key != expectedKey.String() {
		return fmt.Errorf("STORE acknowledgment has the wrong key")
	}
	if result.Stored {
		if result.Error != "" {
			return fmt.Errorf("contradictory STORE acknowledgment")
		}
		return nil
	}
	switch result.Error {
	case storeHashMismatch:
		return ErrHashMismatch
	case storeValueTooLarge:
		return ErrValueTooLarge
	case "":
		return fmt.Errorf("STORE response has no acknowledgment or rejection reason")
	default:
		return fmt.Errorf("STORE rejected: %s", result.Error)
	}
}

// storePayload rejects malformed requests without writing. Well-formed requests
// receive an explicit success or hash/size rejection. Repeating a valid STORE
// is idempotent because the same key always corresponds to the same value.
func (network *Network) storePayload(payload json.RawMessage) (json.RawMessage, error) {
	var request storeRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return nil, err
	}
	key, err := NewKademliaID(request.Key)
	if err != nil {
		return nil, err
	}
	if request.Data == nil {
		return nil, fmt.Errorf("STORE requires data (empty base64 string is allowed)")
	}
	response := storeResponse{Key: key.String()}
	err = network.dataStore.put(*key, request.Data)
	switch {
	case err == nil:
		response.Stored = true
	case errors.Is(err, ErrHashMismatch):
		response.Error = storeHashMismatch
	case errors.Is(err, ErrValueTooLarge):
		response.Error = storeValueTooLarge
	default:
		return nil, err
	}
	return json.Marshal(response)
}
