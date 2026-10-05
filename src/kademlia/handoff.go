package kademlia

import (
	"container/list"
	"context"
	"log"
	"math"
	"sync"
)

const handoffQueueSize = 256
const handoffHistorySize = 4096

// handoffQueue remembers recently observed peers independently of bucket
// admission: a live peer can be dropped by a full bucket and still need values.
// Pending peers cannot be queued twice. History is bounded and permits another
// transfer after a peer has fallen out of the recent-observation window.
type handoffQueue struct {
	mu      sync.Mutex
	inbox   chan Contact
	stopped chan struct{}
	pending map[KademliaID]bool
	recent  map[KademliaID]*list.Element
	order   *list.List
}

func newHandoffQueue() *handoffQueue {
	return &handoffQueue{
		inbox: make(chan Contact, handoffQueueSize), stopped: make(chan struct{}),
		pending: make(map[KademliaID]bool), recent: make(map[KademliaID]*list.Element), order: list.New(),
	}
}

// observe never waits for the transfer worker. If the queue is full, leave the
// peer unmarked so later traffic can retry admission; periodic replication also
// repairs missed transfers. Only validated, directly communicating peers enter.
func (queue *handoffQueue) observe(peer Contact, self *KademliaID) {
	if peer.ID == nil || peer.ID.Equals(self) {
		return
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	id := *peer.ID
	if entry := queue.recent[id]; entry != nil {
		queue.order.MoveToFront(entry)
		return
	}
	if queue.pending[id] {
		return
	}
	select {
	case queue.inbox <- cloneContact(peer):
		queue.pending[id] = true
		queue.recent[id] = queue.order.PushFront(id)
		if queue.order.Len() > handoffHistorySize {
			oldest := queue.order.Back()
			delete(queue.recent, oldest.Value.(KademliaID))
			queue.order.Remove(oldest)
		}
	default:
	}
}

func (network *Network) handoffLoop() {
	defer close(network.handoffs.stopped)
	for {
		select {
		case <-network.done:
			return
		case peer := <-network.handoffs.inbox:
			network.transferToNewPeer(peer)
			network.handoffs.mu.Lock()
			delete(network.handoffs.pending, *peer.ID)
			network.handoffs.mu.Unlock()
		}
	}
}

// transferToNewPeer follows section 2.5 of the paper: send relevant values to
// a new responsible peer, with the closest existing holder sending the copy.
// Flat buckets give partial knowledge, so eligibility uses the known contacts;
// periodic replication performs a full lookup and repairs incomplete knowledge.
// Snapshot locks are released before sending, and original copies are retained.
func (network *Network) transferToNewPeer(peer Contact) {
	for key, data := range network.dataStore.snapshot() {
		select {
		case <-network.done:
			return
		default:
		}
		contacts := network.routingTable.FindClosestContacts(&key, math.MaxInt)
		if !handoffEligible(network.me, peer, &key, contacts, network.config.K) {
			continue
		}
		if err := network.SendStoreMessage(context.Background(), &peer, &key, data); err != nil {
			select {
			case <-network.done:
				return
			default:
			}
			log.Printf("join transfer failed: key=%s peer=%s error=%v", key.String(), peer.Address, err)
		}
	}
}

func handoffEligible(self, peer Contact, key *KademliaID, contacts []Contact, k int) bool {
	selfDistance := self.ID.CalcDistance(key)
	peerDistance := peer.ID.CalcDistance(key)
	closer := 0
	if selfDistance.Less(peerDistance) {
		closer++
	}
	seen := map[KademliaID]bool{*self.ID: true, *peer.ID: true}
	for _, contact := range contacts {
		if seen[*contact.ID] {
			continue
		}
		seen[*contact.ID] = true
		distance := contact.ID.CalcDistance(key)
		// Suppress duplicate copies from holders farther away than another known
		// existing node. The new peer is excluded from this sender election.
		if distance.Less(selfDistance) {
			return false
		}
		if distance.Less(peerDistance) {
			closer++
		}
	}
	return closer < k
}
