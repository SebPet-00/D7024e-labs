package kademlia

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func readLookupEvents(t *testing.T, data []byte) []LookupEvent {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	var events []LookupEvent
	for {
		var event LookupEvent
		err := decoder.Decode(&event)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	return events
}
func TestLookupEventsCountRetries(t *testing.T) {
	var output bytes.Buffer
	logger := NewLookupLogger(&output)
	config := DefaultConfig()
	config.LookupLogger = logger
	config.RPCTimeout = time.Millisecond
	config.RPCRetries = 2
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	node := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9900"), config)
	seedContact(t, node, rpcContact("127.0.0.1:9901"))
	target := KademliaID{}
	if _, err := node.LookupContact(context.Background(), &target); !errors.Is(err, ErrNoReachableContacts) {
		t.Fatal(err)
	}
	events := readLookupEvents(t, output.Bytes())
	if len(events) != 5 || events[0].Event != "lookup_start" {
		t.Fatalf("events: %+v", events)
	}
	for i := 1; i <= 3; i++ {
		if events[i].Event != "probe" || events[i].Attempt != i || events[i].LookupID != events[0].LookupID || events[i].RequestID != events[1].RequestID {
			t.Fatal("retry correlation failed")
		}
	}
	end := events[4]
	if end.Event != "lookup_end" || end.Success == nil || *end.Success || end.Probes != 1 || end.Attempts != 3 || end.Rounds != 1 || end.Error == "" {
		t.Fatalf("bad outcome: %+v", end)
	}
	if err := logger.Err(); err != nil {
		t.Fatal(err)
	}
}
func TestLookupEventsConcurrentAndLocal(t *testing.T) {
	var output bytes.Buffer
	config := DefaultConfig()
	config.LookupLogger = NewLookupLogger(&output)
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	a := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9910"), config)
	b := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9911"), config)
	seedContact(t, a, b.Contact())
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := a.LookupContact(context.Background(), b.me.ID); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	starts, ends := map[string]bool{}, map[string]bool{}
	for _, event := range readLookupEvents(t, output.Bytes()) {
		if event.Event == "lookup_start" {
			if starts[event.LookupID] {
				t.Fatal("duplicate ID")
			}
			starts[event.LookupID] = true
		}
		if event.Event == "lookup_end" {
			ends[event.LookupID] = true
			if event.Success == nil || !*event.Success || event.Attempts != 1 {
				t.Fatal("invalid success")
			}
		}
	}
	if len(starts) != 10 || len(ends) != 10 {
		t.Fatal("missing boundaries")
	}
	config.LookupLogger.SetEnabled(false)
	key, err := a.Store(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	output.Reset()
	config.LookupLogger.SetEnabled(true)
	if _, err := a.LookupData(context.Background(), key.String()); err != nil {
		t.Fatal(err)
	}
	events := readLookupEvents(t, output.Bytes())
	if len(events) != 2 || events[1].Attempts != 0 || !*events[1].Success {
		t.Fatal("local hit not logged")
	}
	if _, err := a.LookupData(context.Background(), "bad"); err == nil {
		t.Fatal("invalid key accepted")
	}
	events = readLookupEvents(t, output.Bytes())
	if *events[len(events)-1].Success {
		t.Fatal("invalid argument reported as success")
	}
}

type brokenLogWriter struct{}

func (brokenLogWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func TestLookupLoggerFailureAndLossSwitch(t *testing.T) {
	config := DefaultConfig()
	config.LookupLogger = NewLookupLogger(brokenLogWriter{})
	sim := testSimulatedNetwork(t, SimulatedNetworkConfig{})
	node := testRPCNode(t, testEndpoint(t, sim, "127.0.0.1:9920"), config)
	if _, err := node.LookupContact(context.Background(), nil); err == nil {
		t.Fatal("nil target accepted")
	}
	if !errors.Is(config.LookupLogger.Err(), io.ErrClosedPipe) {
		t.Fatal("write failure lost")
	}
	if err := sim.SetPacketLoss(2); err == nil {
		t.Fatal("invalid loss accepted")
	}
	if err := sim.SetPacketLoss(1); err != nil {
		t.Fatal(err)
	}
	if err := sim.SetPacketLoss(0); err != nil {
		t.Fatal(err)
	}
}
