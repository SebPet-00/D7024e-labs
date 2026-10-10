// Package experiments writes raw JSONL from seeded workloads on simulated nodes.
package experiments

import (
	"bufio"
	"context"
	"crypto/sha256"
	"d7024e/src/kademlia"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type Settings struct {
	Output       string
	Sizes        []int
	Losses       []float64
	Seeds        []int64
	Queries      int
	LossNodes    int
	RPCTimeout   time.Duration
	QueryTimeout time.Duration
}

func DefaultSettings() Settings {
	return Settings{Output: "results", Sizes: []int{25, 100, 250, 500, 1000, 1500, 2000},
		Losses: []float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.9},
		Seeds:  []int64{11, 22, 33, 44, 55}, Queries: 20, LossNodes: 100, RPCTimeout: 100 * time.Millisecond, QueryTimeout: 2 * time.Second}
}
func Run(settings Settings) error {
	if settings.Queries < 1 || len(settings.Seeds) == 0 || settings.RPCTimeout <= 0 || settings.QueryTimeout <= 0 {
		return fmt.Errorf("positive query count, timeouts and at least one seed required")
	}
	for _, n := range append(append([]int(nil), settings.Sizes...), settings.LossNodes) {
		if n <= 10 || n > 60000 {
			return fmt.Errorf("network size must be between 11 and 60000")
		}
	}
	for _, loss := range settings.Losses {
		if !(loss >= 0 && loss <= 1) {
			return fmt.Errorf("loss must be in [0,1]")
		}
	}
	if err := os.MkdirAll(settings.Output, 0755); err != nil {
		return err
	}
	for _, seed := range settings.Seeds {
		for _, n := range settings.Sizes {
			if err := runTrialSet(settings, "scale", n, 0, seed); err != nil {
				return err
			}
		}
		for _, loss := range settings.Losses {
			if err := runTrialSet(settings, "loss", settings.LossNodes, loss, seed); err != nil {
				return err
			}
		}
	}
	return nil
}

type query struct {
	source int
	key    kademlia.KademliaID
	value  []byte
}

func runTrialSet(settings Settings, experiment string, n int, loss float64, seed int64) (err error) {
	path := filepath.Join(settings.Output, fmt.Sprintf("%s-n%d-p%.2f-seed%d.jsonl", experiment, n, loss, seed))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if e := file.Close(); err == nil {
			err = e
		}
	}()
	buffer := bufio.NewWriterSize(file, 256*1024)
	defer func() {
		if e := buffer.Flush(); err == nil {
			err = e
		}
	}()
	logger := kademlia.NewLookupLogger(buffer)
	logger.SetEnabled(false)
	encoder := json.NewEncoder(buffer)
	metadata := map[string]any{"event": "run_metadata", "experiment": experiment, "n": n, "loss": loss, "seed": seed,
		"queries": settings.Queries, "k": 10, "alpha": 3, "rpc_retries": 2, "rpc_timeout_ms": settings.RPCTimeout.Milliseconds(),
		"query_timeout_ms": settings.QueryTimeout.Milliseconds(), "latency_ms": 0, "topology": "sequential full joins",
		"maintenance_period_hours": 24}
	if err := encoder.Encode(metadata); err != nil {
		return err
	}
	simulation, err := kademlia.NewSimulatedNetwork(kademlia.SimulatedNetworkConfig{Seed: seed + 100000})
	if err != nil {
		return err
	}
	nodes := make([]*kademlia.Kademlia, 0, n)
	defer func() {
		for _, node := range nodes {
			node.Close()
		}
	}()
	config := kademlia.DefaultConfig()
	config.LookupLogger = logger
	config.RPCTimeout = settings.RPCTimeout
	config.RefreshPeriod = 24 * time.Hour
	config.ReplicationPeriod = 24 * time.Hour
	random := rand.New(rand.NewSource(seed))
	used := map[string]bool{}
	for len(nodes) < n {
		address := fmt.Sprintf("10.%d.%d.%d:%d", random.Intn(256), random.Intn(256), 1+random.Intn(254), 10000+random.Intn(50000))
		if used[address] {
			continue
		}
		used[address] = true
		endpoint, err := simulation.Listen(address)
		if err != nil {
			return err
		}
		node, err := kademlia.NewKademliaWithTransport(endpoint, config)
		if err != nil {
			_ = endpoint.Close()
			return err
		}
		nodes = append(nodes, node)
		if len(nodes) > 1 {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err = node.Join(ctx, nodes[0].Contact().Address)
			cancel()
			if err != nil {
				return fmt.Errorf("setup node %d: %w", len(nodes), err)
			}
		}
	}
	queries := make([]query, settings.Queries)
	for i := range queries {
		data := make([]byte, 64)
		_, _ = random.Read(data)
		key := kademlia.KademliaID(sha256.Sum256(data))
		source := random.Intn(n)
		if experiment == "loss" {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, err := nodes[random.Intn(n)].Store(ctx, data)
			cancel()
			if err != nil {
				return fmt.Errorf("setup store: %w", err)
			}
			// Choose a non-holder so local hits cannot inflate reliability.
			for {
				stored := false
				for _, entry := range nodes[source].DataSnapshot() {
					if entry.Key == key {
						stored = true
						break
					}
				}
				if !stored {
					break
				}
				source = (source + 1) % n
			}
		}
		queries[i] = query{source: source, key: key, value: data}
	}
	if err := simulation.SetPacketLoss(loss); err != nil {
		return err
	}
	logger.SetEnabled(true)
	for i, q := range queries {
		ctx, cancel := context.WithTimeout(context.Background(), settings.QueryTimeout)
		correct := false
		var lookupErr error
		if experiment == "scale" {
			var contacts []kademlia.Contact
			contacts, lookupErr = nodes[q.source].LookupContact(ctx, &q.key)
			expected := nearest(nodes, q.source, &q.key, config.K)
			correct = lookupErr == nil && len(contacts) == len(expected)
			if correct {
				for j := range expected {
					if !contacts[j].ID.Equals(expected[j].ID) {
						correct = false
						break
					}
				}
			}
		} else {
			var result *kademlia.ValueResult
			result, lookupErr = nodes[q.source].LookupData(ctx, q.key.String())
			correct = lookupErr == nil && string(result.Data) == string(q.value)
		}
		cancel()
		sample := map[string]any{"event": "trial_result", "trial": i, "source": nodes[q.source].Contact().Address, "target": q.key.String(), "correct": correct}
		if lookupErr != nil {
			sample["error"] = lookupErr.Error()
		}
		// Foreground lookups drain their workers before returning.
		if err := logger.Err(); err != nil {
			return err
		}
		if err := encoder.Encode(sample); err != nil {
			return err
		}
	}
	logger.SetEnabled(false)
	fmt.Printf("completed %s n=%d loss=%.2f seed=%d queries=%d\n", experiment, n, loss, seed, settings.Queries)
	return logger.Err()
}
func nearest(nodes []*kademlia.Kademlia, source int, target *kademlia.KademliaID, k int) []kademlia.Contact {
	contacts := make([]kademlia.Contact, 0, len(nodes)-1)
	for i, node := range nodes {
		if i != source {
			contacts = append(contacts, node.Contact())
		}
	}
	sort.Slice(contacts, func(i, j int) bool {
		return contacts[i].ID.CalcDistance(target).Less(contacts[j].ID.CalcDistance(target))
	})
	return contacts[:min(k, len(contacts))]
}
