package kademlia

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

const rpcPing = "PING"

var ErrRPCTimeout = errors.New("RPC timed out")

// rpcMessage is the shared wire format for requests and responses.
// Payload carries RPC-specific arguments or results.
type rpcMessage struct {
	RequestID string          `json:"request_id"`
	Type      string          `json:"type"`
	Response  bool            `json:"response"`
	Sender    Contact         `json:"sender"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

type pendingRPC struct {
	address  string
	id       KademliaID
	method   string
	response chan rpcMessage
}

// Network implements RPC independently of how packets are delivered.
// One receiver dispatches requests and matches replies to pending calls.
type Network struct {
	evictions        chan *evictionProbe
	evictionsStopped chan struct{}
	routingTable     *RoutingTable
	transport        Transport
	me               Contact
	config           Config
	mu               sync.Mutex
	pending          map[string]*pendingRPC
	done             chan struct{}
	stopped          chan struct{}
	closeOnce        sync.Once
}

// NewNetwork takes ownership of transport on success and starts the RPC reader.
// The caller must not also call Receive on the supplied transport.
func NewNetwork(transport Transport, config Config) (*Network, error) {
	return newNetwork(transport, config, nil)
}

// The table is supplied before the receiver starts, so handlers never race
// against initialization. Standalone RPC instances get their own empty table.
func newNetwork(transport Transport, config Config, table *RoutingTable) (*Network, error) {
	if transport == nil {
		return nil, fmt.Errorf("transport must not be nil")
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	address, err := canonicalAddress(transport.LocalAddr())
	if err != nil {
		return nil, err
	}
	id := KademliaID(sha256.Sum256([]byte(address)))
	if table == nil {
		table = newRoutingTable(NewContact(&id, address), config.K)
	}
	network := &Network{
		evictions:        make(chan *evictionProbe, IDLength*8),
		evictionsStopped: make(chan struct{}),
		routingTable:     table,
		transport:        transport,
		me:               NewContact(&id, address),
		config:           config,
		pending:          make(map[string]*pendingRPC),
		done:             make(chan struct{}),
		stopped:          make(chan struct{}),
	}
	go network.evictionLoop()
	go network.receiveLoop()
	return network, nil
}

// SendPingMessage returns elapsed time from the first attempt until a valid
// reply. If retries were needed, this includes their timeout periods.
func (network *Network) SendPingMessage(ctx context.Context, contact *Contact) (time.Duration, error) {
	start := time.Now()
	_, err := network.call(ctx, contact, rpcPing, nil)
	if err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

// call uses one unpredictable ID for all attempts of a logical RPC. It retries
// only on timeout, up to RPCRetries additional times. Local send errors fail
// immediately. Cancellation, closure and completion remove the pending entry.
func (network *Network) call(ctx context.Context, contact *Contact, method string, payload json.RawMessage) (rpcMessage, error) {
	if err := ctx.Err(); err != nil {
		return rpcMessage{}, err
	}
	if contact == nil || contact.ID == nil {
		return rpcMessage{}, fmt.Errorf("RPC requires a contact with an ID")
	}
	address, err := canonicalAddress(contact.Address)
	if err != nil {
		return rpcMessage{}, err
	}
	expectedID := KademliaID(sha256.Sum256([]byte(address)))
	if !contact.ID.Equals(&expectedID) {
		return rpcMessage{}, fmt.Errorf("contact ID does not match its address")
	}
	pending := &pendingRPC{address: address, id: expectedID, method: method, response: make(chan rpcMessage, 1)}
	version := network.routingTable.contactVersion(&expectedID)
	requestID, err := network.register(pending)
	if err != nil {
		return rpcMessage{}, err
	}
	defer func() {
		network.mu.Lock()
		delete(network.pending, requestID)
		network.mu.Unlock()
	}()
	request := rpcMessage{RequestID: requestID, Type: method, Sender: network.me, Payload: payload}
	data, err := json.Marshal(request)
	if err != nil {
		return rpcMessage{}, err
	}

	for attempt := 0; ; attempt++ {
		select {
		case <-ctx.Done():
			return rpcMessage{}, ctx.Err()
		case <-network.done:
			return rpcMessage{}, net.ErrClosed
		default:
		}
		if err := network.transport.Send(address, data); err != nil {
			return rpcMessage{}, err
		}
		timer := time.NewTimer(network.config.RPCTimeout)
		select {
		case response := <-pending.response:
			timer.Stop()
			return response, nil
		case <-ctx.Done():
			timer.Stop()
			return rpcMessage{}, ctx.Err()
		case <-network.done:
			timer.Stop()
			return rpcMessage{}, net.ErrClosed
		case <-timer.C:
			if attempt == network.config.RPCRetries {
				network.routingTable.removeUnresponsive(&expectedID, version)
				return rpcMessage{}, fmt.Errorf("%w: %s to %s", ErrRPCTimeout, method, address)
			}
		}
	}
}

func (network *Network) register(pending *pendingRPC) (string, error) {
	network.mu.Lock()
	defer network.mu.Unlock()
	select {
	case <-network.done:
		return "", net.ErrClosed
	default:
	}
	for {
		var token [16]byte
		if _, err := rand.Read(token[:]); err != nil {
			return "", err
		}
		id := hex.EncodeToString(token[:])
		if _, exists := network.pending[id]; !exists {
			network.pending[id] = pending
			return id, nil
		}
	}
}

func (network *Network) receiveLoop() {
	defer close(network.stopped)
	for {
		packet, err := network.transport.Receive(context.Background())
		if err != nil {
			network.shutdown()
			return
		}
		message, ok := decodeRPC(packet)
		if !ok {
			continue
		}
		if message.Response {
			network.mu.Lock()
			pending := network.pending[message.RequestID]
			if pending != nil && pending.address == packet.From &&
				pending.method == message.Type && message.Sender.ID.Equals(&pending.id) {
				network.observeContact(message.Sender)
				select {
				case pending.response <- message:
				default: // Ignore duplicate responses.
				}
			}
			network.mu.Unlock()
			continue
		}
		network.handleRequest(message, packet.From)
	}
}

// decodeRPC checks the observed source address, its derived ID, and token
// format. Unpredictable tokens protect against off-path response guessing;
// this is not authentication against an attacker observing the traffic.
func decodeRPC(packet Packet) (rpcMessage, bool) {
	var message rpcMessage
	if len(packet.Data) > MaxDatagramSize || json.Unmarshal(packet.Data, &message) != nil {
		return message, false
	}
	if len(message.RequestID) != 32 {
		return message, false
	}
	if _, err := hex.DecodeString(message.RequestID); err != nil {
		return message, false
	}
	address, err := canonicalAddress(packet.From)
	if err != nil || address != packet.From || message.Sender.Address != address || message.Sender.ID == nil {
		return message, false
	}
	id := KademliaID(sha256.Sum256([]byte(address)))
	if !message.Sender.ID.Equals(&id) {
		return message, false
	}
	return message, true
}

func (network *Network) handleRequest(request rpcMessage, from string) {
	response := rpcMessage{RequestID: request.RequestID, Type: request.Type, Response: true, Sender: network.me}
	switch request.Type {
	case rpcPing:
	case rpcFindNode:
		payload, err := network.findNodePayload(request.Payload, request.Sender.ID)
		if err != nil {
			return
		}
		response.Payload = payload
	default:
		return
	}
	network.observeContact(request.Sender)
	data, err := json.Marshal(response)
	if err == nil {
		// Replies are best effort; the caller handles loss through retries.
		_ = network.transport.Send(from, data)
	}
}

func (network *Network) shutdown() {
	network.closeOnce.Do(func() {
		close(network.done)
		_ = network.transport.Close()
	})
}

// Close stops receiving and unblocks all pending calls.
func (network *Network) Close() {
	network.shutdown()
	<-network.stopped
	<-network.evictionsStopped
}

func (network *Network) SendFindDataMessage(hash string) {
	// TODO
}

func (network *Network) SendStoreMessage(data []byte) {
	// TODO
}
