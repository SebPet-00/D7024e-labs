package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"d7024e/src/kademlia"
)

func cliNode(t *testing.T) *kademlia.Kademlia {
	t.Helper()
	transport, err := kademlia.Listen("127.0.0.1", 0)
	if err != nil {
		t.Fatal(err)
	}
	config := kademlia.DefaultConfig()
	config.RPCTimeout = 10 * time.Millisecond
	config.RPCRetries = 0
	node, err := kademlia.NewKademliaWithTransport(transport, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(node.Close)
	return node
}

func TestShellCommandsOverUDP(t *testing.T) {
	publisher := cliNode(t)
	reader := cliNode(t)
	if err := reader.Join(context.Background(), publisher.Contact().Address); err != nil {
		t.Fatal(err)
	}
	folder := t.TempDir()
	input := filepath.Join(folder, "input with spaces")
	output := filepath.Join(folder, "output with spaces")
	data := []byte{0, 1, 2, 255, 10}
	if err := os.WriteFile(input, data, 0600); err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(data))
	var buf bytes.Buffer
	commands := fmt.Sprintf("\nhelp\nping %s\nput \"%s\"\nshow ds\nshow rt\nexit\n", reader.Contact().Address, input)
	if err := runShell(context.Background(), publisher, 1024, time.Second, strings.NewReader(commands), &buf); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"RTT=", "Stored " + hash, "5 bytes", "Bucket "} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("missing %q in %q", want, buf.String())
		}
	}
	buf.Reset()
	reader = cliNode(t)
	if err := reader.AddContact(publisher.Contact()); err != nil {
		t.Fatal(err)
	}
	if _, err := executeCommand(context.Background(), reader, 1024, "get "+hash+" "+output, &buf); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(output)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("binary roundtrip: %v %v", got, err)
	}
	if !strings.Contains(buf.String(), "Received from "+publisher.Contact().Address) {
		t.Fatal(buf.String())
	}
	buf.Reset()
	if _, err := executeCommand(context.Background(), reader, 1024, "get "+hash, &buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(buf.Bytes(), data) {
		t.Fatal("printed bytes changed")
	}
}

func TestShellErrorsAndEmptyState(t *testing.T) {
	node := cliNode(t)
	var out bytes.Buffer
	for _, command := range []string{"show rt", "show ds"} {
		if _, err := executeCommand(context.Background(), node, 1024, command, &out); err != nil {
			t.Fatal(err)
		}
	}
	if !strings.Contains(out.String(), "Routing table is empty") || !strings.Contains(out.String(), "Data store is empty") {
		t.Fatal(out.String())
	}
	large := filepath.Join(t.TempDir(), "large")
	if err := os.WriteFile(large, make([]byte, 1025), 0600); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"nonsense", "ping", "ping bad", "ping 127.0.0.1:1", "put", "put /missing-file", "put " + large, "get", "get bad", "show", "show nope", "exit extra"} {
		if _, err := executeCommand(context.Background(), node, 1024, command, io.Discard); err == nil {
			t.Fatalf("accepted %q", command)
		}
	}
	key, err := node.Store(context.Background(), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executeCommand(context.Background(), node, 1024, "get "+key.String()+" "+t.TempDir(), io.Discard); err == nil {
		t.Fatal("wrote to directory")
	}
	if _, err := executeCommand(context.Background(), node, 1024, "get "+key.String(), failingWriter{}); err == nil {
		t.Fatal("ignored output failure")
	}
	out.Reset()
	if err := runShell(context.Background(), node, 1024, time.Second, strings.NewReader("bad\nhelp\nexit\n"), &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Error:") || strings.Count(out.String(), "Commands:") != 2 {
		t.Fatal(out.String())
	}
	if err := runShell(context.Background(), node, 1024, time.Second, failingReader{}, io.Discard); err == nil {
		t.Fatal("ignored input failure")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runShell(ctx, node, 1024, time.Second, strings.NewReader("ping bad\n"), io.Discard); err != nil {
		t.Fatal(err)
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("input failed") }

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("output failed") }

func TestStartup(t *testing.T) {
	bootstrap := cliNode(t)
	for _, args := range [][]string{
		{"-listen", "127.0.0.1:0"},
		{"-listen", "127.0.0.1:0", "-bootstrap", bootstrap.Contact().Address},
		{"-help"},
	} {
		var out bytes.Buffer
		if err := run(context.Background(), args, strings.NewReader("exit\n"), &out, io.Discard); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{
		{"-unknown"}, {"extra"}, {"-command-timeout", "0"}, {"-listen", "bad"},
		{"-listen", "127.0.0.1:bad"}, {"-listen", "0.0.0.0:0"},
		{"-listen", "127.0.0.1:0", "-k", "0"},
		{"-listen", "127.0.0.1:0", "-bootstrap", "bad"},
	} {
		if err := run(context.Background(), args, strings.NewReader(""), io.Discard, io.Discard); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := run(ctx, []string{"-listen", "127.0.0.1:0", "-headless"}, nil, cancelWriter{cancel}, io.Discard); err != nil {
		t.Fatal(err)
	}
}

type cancelWriter struct{ cancel context.CancelFunc }

func (w cancelWriter) Write(p []byte) (int, error) { w.cancel(); return len(p), nil }

func TestResolvePeer(t *testing.T) {
	for _, address := range []string{"127.0.0.1:8000", "[::1]:8000", "localhost:8000"} {
		if _, err := resolvePeer(context.Background(), address); err != nil {
			t.Fatal(err)
		}
	}
	for _, address := range []string{"bad", "127.0.0.1:0", "127.0.0.1:65536", "0.0.0.0:8000", "224.0.0.1:8000", "127.0.0.1:bad"} {
		if _, err := resolvePeer(context.Background(), address); err == nil {
			t.Fatalf("accepted %s", address)
		}
	}
}

func TestInteractiveStartupCancellationWithoutInput(t *testing.T) {
	input, writer := io.Pipe()
	defer input.Close()
	defer writer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, []string{"-listen", "127.0.0.1:0"}, input, cancelWriter{cancel}, io.Discard) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown waited for keyboard input")
	}
}

func TestLookupLogCLI(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lookup.jsonl")
	var output bytes.Buffer
	args := []string{"-listen", "127.0.0.1:0", "-lookup-log", path}
	if err := run(context.Background(), args, strings.NewReader("get bad\nexit\n"), &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("lookup_start")) || !bytes.Contains(data, []byte("lookup_end")) || !bytes.Contains(data, []byte(`"success":false`)) {
		t.Fatal("missing structured lookup failure")
	}
	if err := run(context.Background(), args, strings.NewReader("exit\n"), io.Discard, io.Discard); err == nil {
		t.Fatal("existing log was overwritten")
	}
}

type dropValueReplies struct {
	kademlia.Transport
	sent chan struct{}
}

func (transport *dropValueReplies) Send(to string, data []byte) error {
	var message struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(data, &message) == nil && message.Type == "FIND_VALUE" {
		select {
		case transport.sent <- struct{}{}:
		default:
		}
		return nil
	}
	return transport.Transport.Send(to, data)
}

func TestShutdownLogsActiveLookupBeforeClosingFile(t *testing.T) {
	udp, err := kademlia.Listen("127.0.0.1", 0)
	if err != nil {
		t.Fatal(err)
	}
	transport := &dropValueReplies{Transport: udp, sent: make(chan struct{}, 1)}
	bootstrap, err := kademlia.NewKademliaWithTransport(transport, kademlia.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer bootstrap.Close()
	path := filepath.Join(t.TempDir(), "events.jsonl")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	var diagnostics bytes.Buffer
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte("missing")))
	go func() {
		done <- run(ctx, []string{"-listen", "127.0.0.1:0", "-bootstrap", bootstrap.Contact().Address,
			"-lookup-log", path, "-rpc-timeout", "1h", "-command-timeout", "1h"},
			strings.NewReader("get "+hash+"\n"), io.Discard, &diagnostics)
	}()
	select {
	case <-transport.sent:
	case <-time.After(3 * time.Second):
		t.Fatal("value lookup did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		var event kademlia.LookupEvent
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatal(err)
		}
		if event.Kind == "FIND_VALUE" && event.Event == "lookup_end" {
			found = true
			if event.Success == nil || *event.Success || !strings.Contains(event.Error, "context canceled") {
				t.Fatal("missing cancellation outcome")
			}
		}
	}
	if !found || diagnostics.Len() != 0 {
		t.Fatalf("lost final event or logging error: %s", diagnostics.String())
	}
}
