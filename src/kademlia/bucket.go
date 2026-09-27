package kademlia

import (
	"container/list"
	"time"
)

// bucket definition
// contains a List
type bucket struct {
	versions   map[KademliaID]uint64
	version    uint64
	probing    bool
	lastLookup time.Time
	list       *list.List
	capacity   int
}

// newBucket returns a new instance of a bucket
func newBucket() *bucket {
	bucket := &bucket{}
	bucket.list = list.New()
	bucket.capacity = defaultK
	bucket.versions = make(map[KademliaID]uint64)
	bucket.lastLookup = time.Now()
	return bucket
}

// AddContact adds the Contact to the front of the bucket
// or moves it to the front of the bucket if it already existed
func (bucket *bucket) AddContact(contact Contact) {
	var element *list.Element
	for e := bucket.list.Front(); e != nil; e = e.Next() {
		nodeID := e.Value.(Contact).ID

		if (contact).ID.Equals(nodeID) {
			element = e
		}
	}

	if element == nil {
		if bucket.list.Len() < bucket.capacity {
			bucket.list.PushFront(contact)
		} else {
			return
		}
	} else {
		element.Value = contact
		bucket.list.MoveToFront(element)
	}
	bucket.version++
	bucket.versions[*contact.ID] = bucket.version
}

// GetContactAndCalcDistance returns an array of Contacts where
// the distance has already been calculated
func (bucket *bucket) GetContactAndCalcDistance(target *KademliaID) []Contact {
	var contacts []Contact

	for elt := bucket.list.Front(); elt != nil; elt = elt.Next() {
		contact := elt.Value.(Contact)
		contact.CalcDistance(target)
		contacts = append(contacts, contact)
	}

	return contacts
}

// Len return the size of the bucket
func (bucket *bucket) Len() int {
	return bucket.list.Len()
}

// remove is called only while the owning routing table is locked.
func (bucket *bucket) remove(id *KademliaID) {
	for element := bucket.list.Front(); element != nil; element = element.Next() {
		contact := element.Value.(Contact)
		if contact.ID.Equals(id) {
			bucket.list.Remove(element)
			delete(bucket.versions, *id)
			return
		}
	}
}
