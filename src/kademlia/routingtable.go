package kademlia

import "sync"

// RoutingTable definition
// keeps a refrence contact of me and an array of buckets
type RoutingTable struct {
	mu      sync.RWMutex
	me      Contact
	buckets [IDLength * 8]*bucket
}

// NewRoutingTable returns a new instance of a RoutingTable
func NewRoutingTable(me Contact) *RoutingTable {
	return newRoutingTable(me, defaultK)
}

// newRoutingTable uses the node's validated bucket capacity.
func newRoutingTable(me Contact, capacity int) *RoutingTable {
	routingTable := &RoutingTable{}
	for i := 0; i < IDLength*8; i++ {
		routingTable.buckets[i] = newBucket()
		routingTable.buckets[i].capacity = capacity
	}
	routingTable.me = cloneContact(me)
	return routingTable
}

// AddContact add a new contact to the correct Bucket
func (routingTable *RoutingTable) AddContact(contact Contact) {
	if contact.ID == nil || contact.ID.Equals(routingTable.me.ID) {
		return
	}
	routingTable.mu.Lock()
	defer routingTable.mu.Unlock()
	contact = cloneContact(contact)
	bucketIndex := routingTable.getBucketIndex(contact.ID)
	bucket := routingTable.buckets[bucketIndex]
	bucket.AddContact(contact)
}

// FindClosestContacts finds the count closest Contacts to the target in the RoutingTable
func (routingTable *RoutingTable) FindClosestContacts(target *KademliaID, count int) []Contact {
	if target == nil || count <= 0 {
		return nil
	}
	routingTable.mu.RLock()
	defer routingTable.mu.RUnlock()
	var candidates ContactCandidates
	// Bucket adjacency is not XOR-distance order relative to an arbitrary
	// target. Scan all buckets before sorting to avoid missing closer peers.
	for _, bucket := range routingTable.buckets {
		contacts := bucket.GetContactAndCalcDistance(target)
		for i := range contacts {
			contacts[i] = cloneContact(contacts[i])
		}
		candidates.Append(contacts)
	}
	candidates.Sort()
	if count > candidates.Len() {
		count = candidates.Len()
	}
	return candidates.GetContacts(count)
}

// getBucketIndex get the correct Bucket index for the KademliaID
func (routingTable *RoutingTable) getBucketIndex(id *KademliaID) int {
	distance := id.CalcDistance(routingTable.me.ID)
	for i := 0; i < IDLength; i++ {
		for j := 0; j < 8; j++ {
			if (distance[i]>>uint8(7-j))&0x1 != 0 {
				return i*8 + j
			}
		}
	}

	return IDLength*8 - 1
}
