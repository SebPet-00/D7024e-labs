package kademlia

import (
	"crypto/sha256"
	"fmt"
	"sort"
)

// Contact definition
// stores the KademliaID, the ip address and the distance
type Contact struct {
	ID       *KademliaID
	Address  string
	distance *KademliaID
}

// NewContact returns a new instance of a Contact
func NewContact(id *KademliaID, address string) Contact {
	return Contact{id, address, nil}
}

// CalcDistance calculates the distance to the target and
// fills the contacts distance field
func (contact *Contact) CalcDistance(target *KademliaID) {
	contact.distance = contact.ID.CalcDistance(target)
}

// Less returns true if contact.distance < otherContact.distance
func (contact *Contact) Less(otherContact *Contact) bool {
	return contact.distance.Less(otherContact.distance)
}

// String returns a simple string representation of a Contact
func (contact *Contact) String() string {
	return fmt.Sprintf(`contact("%s", "%s")`, contact.ID, contact.Address)
}

// ContactCandidates definition
// stores an array of Contacts
type ContactCandidates struct {
	contacts []Contact
}

// Append an array of Contacts to the ContactCandidates
func (candidates *ContactCandidates) Append(contacts []Contact) {
	candidates.contacts = append(candidates.contacts, contacts...)
}

// GetContacts returns the first count number of Contacts
func (candidates *ContactCandidates) GetContacts(count int) []Contact {
	return candidates.contacts[:count]
}

// Sort the Contacts in ContactCandidates
func (candidates *ContactCandidates) Sort() {
	sort.Sort(candidates)
}

// Len returns the length of the ContactCandidates
func (candidates *ContactCandidates) Len() int {
	return len(candidates.contacts)
}

// Swap the position of the Contacts at i and j
// WARNING does not check if either i or j is within range
func (candidates *ContactCandidates) Swap(i, j int) {
	candidates.contacts[i], candidates.contacts[j] = candidates.contacts[j], candidates.contacts[i]
}

// Less returns true if the Contact at index i is smaller than
// the Contact at index j
func (candidates *ContactCandidates) Less(i, j int) bool {
	return candidates.contacts[i].Less(&candidates.contacts[j])
}

// cloneContact keeps pointer fields from exposing mutable routing-table state.
func cloneContact(contact Contact) Contact {
	if contact.ID != nil {
		id := *contact.ID
		contact.ID = &id
	}
	if contact.distance != nil {
		distance := *contact.distance
		contact.distance = &distance
	}
	return contact
}

// validatedContact checks the lab identity rule and returns an owned copy.
func validatedContact(contact Contact) (Contact, error) {
	if contact.ID == nil {
		return Contact{}, fmt.Errorf("contact requires an ID")
	}
	address, err := canonicalAddress(contact.Address)
	if err != nil {
		return Contact{}, err
	}
	id := KademliaID(sha256.Sum256([]byte(address)))
	if !contact.ID.Equals(&id) {
		return Contact{}, fmt.Errorf("contact ID does not match its address")
	}
	return NewContact(&id, address), nil
}
