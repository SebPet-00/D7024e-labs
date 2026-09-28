package kademlia

import "sort"

// BucketSnapshot contains copies of contacts in one non-empty bucket.
type BucketSnapshot struct {
	Index    int
	Contacts []Contact
}

func (kademlia *Kademlia) RoutingSnapshot() []BucketSnapshot {
	table := kademlia.routingTable
	table.mu.RLock()
	defer table.mu.RUnlock()
	var result []BucketSnapshot
	for index, bucket := range table.buckets {
		if bucket.list.Len() == 0 {
			continue
		}
		entry := BucketSnapshot{Index: index}
		for element := bucket.list.Front(); element != nil; element = element.Next() {
			entry.Contacts = append(entry.Contacts, cloneContact(element.Value.(Contact)))
		}
		result = append(result, entry)
	}
	return result
}

// StoredValueInfo exposes keys and sizes without copying or exposing value data.
type StoredValueInfo struct {
	Key  KademliaID
	Size int
}

func (kademlia *Kademlia) DataSnapshot() []StoredValueInfo {
	store := kademlia.dataStore
	store.mu.RLock()
	defer store.mu.RUnlock()
	result := make([]StoredValueInfo, 0, len(store.values))
	for key, value := range store.values {
		result = append(result, StoredValueInfo{Key: key, Size: len(value)})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key.Less(&result[j].Key) })
	return result
}
