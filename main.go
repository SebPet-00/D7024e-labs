// TODO: Add package documentation for `main`, like this:
// Package main something something...
package main

import (
	"d7024e/src/kademlia"
	"fmt"
)

func main() {
	fmt.Println("Pretending to run the kademlia app...")
	// Using stuff from the kademlia package here. Something like...
	id, err := kademlia.NewKademliaID("FFFFFFFF00000000000000000000000000000000000000000000000000000000")
	if err != nil {
		fmt.Println(err)
		return
	}
	contact := kademlia.NewContact(id, "localhost:8000")
	fmt.Println(contact.String())
	fmt.Printf("%v\n", contact)
}
