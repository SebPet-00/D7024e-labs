package kademlia

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// LookupEvent is one JSONL record. Probe records describe transmission attempts:
// the same request ID with attempt > 1 is a retry, not a new logical probe.
type LookupEvent struct {
	Event     string    `json:"event"`
	LookupID  string    `json:"lookup_id"`
	Node      string    `json:"node"`
	Kind      string    `json:"kind"`
	Target    string    `json:"target"`
	Time      time.Time `json:"time"`
	Peer      string    `json:"peer,omitempty"`
	RequestID string    `json:"request_id,omitempty"`
	Attempt   int       `json:"attempt,omitempty"`
	Success   *bool     `json:"success,omitempty"`
	Error     string    `json:"error,omitempty"`
	Probes    int64     `json:"probes"`
	Attempts  int64     `json:"attempts"`
	Rounds    int64     `json:"rounds"`
	ElapsedNS int64     `json:"elapsed_ns,omitempty"`
}

// LookupLogger serializes writes shared by many nodes. It owns neither the
// writer nor its lifetime. Err reports the first write error; logging failures
// do not change routing behavior. Do not change settings in a measured lookup.
type LookupLogger struct {
	mu      sync.Mutex
	encoder *json.Encoder
	enabled bool
	err     error
}

func NewLookupLogger(writer io.Writer) *LookupLogger {
	return &LookupLogger{encoder: json.NewEncoder(writer), enabled: true}
}
func (logger *LookupLogger) SetEnabled(enabled bool) {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	logger.enabled = enabled
}
func (logger *LookupLogger) Err() error {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	return logger.err
}
func (logger *LookupLogger) write(event LookupEvent) {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if logger.err == nil {
		logger.err = logger.encoder.Encode(event)
	}
}

type lookupTraceKey struct{}

var lookupSequence atomic.Uint64

type lookupTrace struct {
	logger   *LookupLogger
	base     LookupEvent
	started  time.Time
	probes   atomic.Int64
	attempts atomic.Int64
	rounds   atomic.Int64
}

func (kademlia *Kademlia) startLookup(ctx context.Context, kind, target string) (context.Context, *lookupTrace) {
	logger := kademlia.config.LookupLogger
	if logger == nil {
		return ctx, nil
	}
	logger.mu.Lock()
	enabled := logger.enabled
	logger.mu.Unlock()
	if !enabled {
		return ctx, nil
	}
	trace := &lookupTrace{logger: logger, started: time.Now(), base: LookupEvent{
		LookupID: fmt.Sprintf("%s/%d", kademlia.me.Address, lookupSequence.Add(1)),
		Node:     kademlia.me.Address, Kind: kind, Target: target,
	}}
	trace.emit("lookup_start", nil)
	return context.WithValue(ctx, lookupTraceKey{}, trace), trace
}
func (trace *lookupTrace) emit(kind string, edit func(*LookupEvent)) {
	if trace == nil {
		return
	}
	event := trace.base
	event.Event = kind
	event.Time = time.Now().UTC()
	event.Probes = trace.probes.Load()
	event.Attempts = trace.attempts.Load()
	event.Rounds = trace.rounds.Load()
	if edit != nil {
		edit(&event)
	}
	trace.logger.write(event)
}
func (trace *lookupTrace) finish(err error) {
	trace.emit("lookup_end", func(event *LookupEvent) {
		success := err == nil
		event.Success = &success
		event.ElapsedNS = time.Since(trace.started).Nanoseconds()
		if err != nil {
			event.Error = err.Error()
		}
	})
}
func lookupTraceFrom(ctx context.Context) *lookupTrace {
	trace, _ := ctx.Value(lookupTraceKey{}).(*lookupTrace)
	return trace
}
func (trace *lookupTrace) probe(peer, requestID string, attempt int) {
	if trace == nil {
		return
	}
	trace.attempts.Add(1)
	if attempt == 1 {
		trace.probes.Add(1)
	}
	trace.emit("probe", func(event *LookupEvent) {
		event.Peer = peer
		event.RequestID = requestID
		event.Attempt = attempt
	})
}
