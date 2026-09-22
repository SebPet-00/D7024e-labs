// Package main currently demonstrates initialization of a Kademlia node.
package main

import (
	"fmt"

	"d7024e/src/kademlia"
)

func main() {
	node, err := kademlia.NewKademlia("127.0.0.1:8000", kademlia.DefaultConfig())
	if err != nil {
		fmt.Println(err)
		return
	}
	defer node.Close()

	contact := node.Contact()
	fmt.Println(contact.String())
}
