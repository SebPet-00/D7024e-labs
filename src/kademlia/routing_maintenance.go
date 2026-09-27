package kademlia

import (
	"crypto/rand"
	"fmt"
	"time"
)

// evictionProbe records the peer and observation version checked outside the
// table lock. One probe per bucket prevents competing replacement decisions.
type evictionProbe struct {
	bucket      int
	old         Contact
	replacement Contact
	version     uint64
}

func (table *RoutingTable) observe(contact Contact) *evictionProbe {
	if contact.ID == nil || contact.ID.Equals(table.me.ID) {
		return nil
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	index := table.getBucketIndex(contact.ID)
	bucket := table.buckets[index]
	if _, exists := bucket.versions[*contact.ID]; exists || bucket.Len() < bucket.capacity {
		bucket.AddContact(cloneContact(contact))
		return nil
	}
	if bucket.probing {
		return nil
	}
	old := bucket.list.Back().Value.(Contact) // Front is most recently seen.
	bucket.probing = true
	return &evictionProbe{
		bucket: index, old: cloneContact(old), replacement: cloneContact(contact),
		version: bucket.versions[*old.ID],
	}
}

func (table *RoutingTable) probeCurrent(probe *evictionProbe) bool {
	table.mu.RLock()
	defer table.mu.RUnlock()
	return table.buckets[probe.bucket].versions[*probe.old.ID] == probe.version
}

func (table *RoutingTable) finishProbe(probe *evictionProbe, timedOut bool) {
	table.mu.Lock()
	defer table.mu.Unlock()
	bucket := table.buckets[probe.bucket]
	bucket.probing = false
	if !timedOut {
		return
	}
	current := bucket.versions[*probe.old.ID]
	if current != 0 && current != probe.version {
		return // More recent communication proves the old timeout is stale.
	}
	if current != 0 {
		bucket.remove(probe.old.ID)
	}
	// Another RPC may already have removed the old peer. Fill the free slot,
	// but never evict an unrelated contact that has since taken it.
	bucket.AddContact(cloneContact(probe.replacement))
}

func (table *RoutingTable) contactVersion(id *KademliaID) uint64 {
	table.mu.RLock()
	defer table.mu.RUnlock()
	return table.buckets[table.getBucketIndex(id)].versions[*id]
}

// removeUnresponsive is called only after all RPC attempts time out.
// Cancellation, local send errors and malformed replies do not remove peers.
func (table *RoutingTable) removeUnresponsive(id *KademliaID, version uint64) {
	if version == 0 {
		return
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	bucket := table.buckets[table.getBucketIndex(id)]
	if bucket.versions[*id] == version {
		bucket.remove(id)
	}
}

func (table *RoutingTable) markLookup(target *KademliaID, now time.Time) {
	table.mu.Lock()
	defer table.mu.Unlock()
	table.buckets[table.getBucketIndex(target)].lastLookup = now
}

func (table *RoutingTable) staleBuckets(now time.Time, period time.Duration) []int {
	table.mu.RLock()
	defer table.mu.RUnlock()
	var indices []int
	for index, bucket := range table.buckets {
		if now.Sub(bucket.lastLookup) >= period {
			indices = append(indices, index)
		}
	}
	return indices
}

// randomBucketTarget preserves the shared prefix with self, flips the first
// differing bit, and randomizes the lower bits. Index 0 is the farthest bucket.
func randomBucketTarget(self *KademliaID, index int) (*KademliaID, error) {
	if self == nil || index < 0 || index >= IDLength*8 {
		return nil, fmt.Errorf("invalid refresh bucket")
	}
	var distance KademliaID
	if _, err := rand.Read(distance[:]); err != nil {
		return nil, err
	}
	byteIndex, bit := index/8, uint(7-index%8)
	for i := 0; i < byteIndex; i++ {
		distance[i] = 0
	}
	distance[byteIndex] &= byte((1 << (bit + 1)) - 1)
	distance[byteIndex] |= 1 << bit
	return self.CalcDistance(&distance), nil
}
