package kademlia

import (
	"strings"
	"testing"
)

func mustKademliaID(t *testing.T, input string) *KademliaID {
	t.Helper()
	id, err := NewKademliaID(input)
	if err != nil {
		t.Fatalf("NewKademliaID(%q): %v", input, err)
	}
	return id
}

func TestNewKademliaID(t *testing.T) {
	for _, input := range []string{
		strings.Repeat("0", 64),
		strings.Repeat("f", 64),
		"ABCDEF01" + strings.Repeat("0", 56),
	} {
		t.Run(input, func(t *testing.T) {
			id := mustKademliaID(t, input)
			if got := id.String(); got != strings.ToLower(input) {
				t.Fatalf("String() = %q, want %q", got, strings.ToLower(input))
			}
		})
	}
}

func TestNewKademliaIDRejectsInvalidInput(t *testing.T) {
	for _, input := range []string{
		"", strings.Repeat("0", 40), strings.Repeat("0", 63),
		strings.Repeat("0", 65), strings.Repeat("0", 66),
		strings.Repeat("0", 63) + "g",
	} {
		t.Run(input, func(t *testing.T) {
			id, err := NewKademliaID(input)
			if err == nil || id != nil {
				t.Fatalf("expected nil ID and an error, got %v, %v", id, err)
			}
		})
	}
}

func TestKademliaIDEqualsAndLess(t *testing.T) {
	zero := mustKademliaID(t, strings.Repeat("0", 64))
	one := mustKademliaID(t, strings.Repeat("0", 63)+"1")
	high := mustKademliaID(t, "80"+strings.Repeat("0", 62))
	max := mustKademliaID(t, strings.Repeat("f", 64))
	ordered := []*KademliaID{zero, one, high, max}
	for i, left := range ordered {
		for j, right := range ordered {
			if got := left.Equals(right); got != (i == j) {
				t.Errorf("Equals at (%d, %d) = %v", i, j, got)
			}
			if got := left.Less(right); got != (i < j) {
				t.Errorf("Less at (%d, %d) = %v", i, j, got)
			}
		}
	}
	copyOfOne := mustKademliaID(t, one.String())
	if !one.Equals(copyOfOne) {
		t.Error("equal IDs at different addresses must compare equal")
	}
}

func TestKademliaIDCalcDistance(t *testing.T) {
	left := mustKademliaID(t, "a5"+strings.Repeat("00", 30)+"0f")
	right := mustKademliaID(t, "3c"+strings.Repeat("00", 30)+"f0")
	want := mustKademliaID(t, "99"+strings.Repeat("00", 30)+"ff")
	beforeLeft, beforeRight := *left, *right
	if got := left.CalcDistance(right); !got.Equals(want) {
		t.Fatalf("distance = %s, want %s", got, want)
	}
	if !right.CalcDistance(left).Equals(want) {
		t.Error("XOR distance must be symmetric")
	}
	zero := KademliaID{}
	if !left.CalcDistance(left).Equals(&zero) {
		t.Error("distance to self must be zero")
	}
	if *left != beforeLeft || *right != beforeRight {
		t.Error("distance calculation changed an input")
	}
}
