package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"d7024e/src/kademlia"
)

const shellHelp = "Commands: ping IP:PORT | put FILENAME | get KEY [FILENAME] | show rt | show ds | publish [--force] [--prev=N] DOMAIN:PACKAGE:VERSION FILENAME | install DOMAIN:PACKAGE:VERSION | show DOMAIN:PACKAGE | show dns DOMAIN | exit"

func runShell(ctx context.Context, node *kademlia.Kademlia, maxValueSize int, timeout time.Duration, input io.Reader, output io.Writer, signing ...map[string]ed25519.PrivateKey) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Only the input reader runs independently. The foreground command remains
	// here, so cancellation drains its RPCs/log events before run closes the file.
	type inputLine struct {
		text string
		err  error
	}
	lines := make(chan inputLine)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(input)
		scanner.Buffer(make([]byte, 4096), 64*1024)
		for scanner.Scan() {
			select {
			case lines <- inputLine{text: scanner.Text()}:
			case <-ctx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case lines <- inputLine{err: err}:
			case <-ctx.Done():
			}
		}
		// A terminal read itself may block; process exit releases stdin.
	}()
	fmt.Fprintln(output, shellHelp)
	for {
		fmt.Fprint(output, "> ")
		var item inputLine
		select {
		case <-ctx.Done():
			return nil
		case next, ok := <-lines:
			if !ok {
				return nil
			}
			item = next
		}
		if ctx.Err() != nil {
			return nil
		}
		if item.err != nil {
			return item.err
		}
		line := strings.TrimSpace(item.text)
		if line == "" {
			continue
		}
		commandCtx, cancel := context.WithTimeout(ctx, timeout)
		exit, err := executeCommand(commandCtx, node, maxValueSize, line, output, signing...)
		cancel()
		if err != nil {
			fmt.Fprintf(output, "Error: %v\n", err)
		}
		if exit {
			return nil
		}
	}
}

// Filenames occupy the remainder of the command, so spaces need no escaping.
// A surrounding pair of double quotes is also accepted.
func filename(text string) string { return strings.Trim(strings.TrimSpace(text), "\"") }

func executeCommand(ctx context.Context, node *kademlia.Kademlia, maxValueSize int, line string, output io.Writer, signing ...map[string]ed25519.PrivateKey) (bool, error) {
	command, rest, _ := strings.Cut(strings.TrimSpace(line), " ")
	rest = strings.TrimSpace(rest)
	var keys map[string]ed25519.PrivateKey
	if len(signing) > 0 {
		keys = signing[0]
	}
	switch command {
	case "publish", "install":
		return false, executeRegistryCommand(ctx, node, maxValueSize, line, output, keys)
	case "exit":
		if rest != "" {
			return false, fmt.Errorf("usage: exit")
		}
		return true, nil
	case "help":
		fmt.Fprintln(output, shellHelp)
	case "ping":
		if len(strings.Fields(rest)) != 1 {
			return false, fmt.Errorf("usage: ping IP:PORT")
		}
		address, err := resolvePeer(ctx, rest)
		if err != nil {
			return false, err
		}
		id := kademlia.KademliaID(sha256.Sum256([]byte(address)))
		peer := kademlia.NewContact(&id, address)
		elapsed, err := node.Ping(ctx, &peer)
		if err != nil {
			return false, err
		}
		fmt.Fprintf(output, "PONG %s RTT=%s\n", address, elapsed)
	case "put":
		path := filename(rest)
		if path == "" {
			return false, fmt.Errorf("usage: put FILENAME")
		}
		file, err := os.Open(path)
		if err != nil {
			return false, err
		}
		data, readErr := io.ReadAll(io.LimitReader(file, int64(maxValueSize)+1))
		closeErr := file.Close()
		if readErr != nil {
			return false, readErr
		}
		if closeErr != nil {
			return false, closeErr
		}
		if len(data) > maxValueSize {
			return false, fmt.Errorf("%w: maximum is %d bytes", kademlia.ErrValueTooLarge, maxValueSize)
		}
		key, err := node.Store(ctx, data)
		if err != nil {
			if key != nil {
				fmt.Fprintf(output, "Key %s (write incomplete)\n", key.String())
			}
			return false, err
		}
		fmt.Fprintf(output, "Stored %s\n", key.String())
	case "get":
		key, path, _ := strings.Cut(rest, " ")
		if key == "" {
			return false, fmt.Errorf("usage: get KEY [FILENAME]")
		}
		result, err := node.LookupData(ctx, key)
		if err != nil {
			return false, err
		}
		path = filename(path)
		if path != "" {
			if err := os.WriteFile(path, result.Data, 0600); err != nil {
				return false, err
			}
			fmt.Fprintf(output, "Saved %d bytes to %s\n", len(result.Data), path)
		} else {
			if _, err := output.Write(result.Data); err != nil {
				return false, err
			}
			fmt.Fprintln(output)
		}
		fmt.Fprintf(output, "Received from %s id=%s\n", result.Source.Address, result.Source.ID.String())
	case "show":
		switch rest {
		case "rt":
			buckets := node.RoutingSnapshot()
			if len(buckets) == 0 {
				fmt.Fprintln(output, "Routing table is empty")
			}
			for _, bucket := range buckets {
				fmt.Fprintf(output, "Bucket %d (%d contacts)\n", bucket.Index, len(bucket.Contacts))
				for _, contact := range bucket.Contacts {
					fmt.Fprintf(output, "  %s %s\n", shortID(contact.ID.String()), contact.Address)
				}
			}
		case "ds":
			for _, description := range node.Registry().Describe() {
				fmt.Fprintln(output, description)
			}
			entries := node.DataSnapshot()
			if len(entries) == 0 {
				fmt.Fprintln(output, "Data store is empty")
			}
			for _, entry := range entries {
				fmt.Fprintf(output, "%s %d bytes\n", shortID(entry.Key.String()), entry.Size)
			}
		default:
			return false, executeRegistryCommand(ctx, node, maxValueSize, line, output, keys)
		}
	default:
		return false, fmt.Errorf("unknown command %q; type help", command)
	}
	return false, nil
}

func shortID(id string) string { return id[:4] + "..." + id[len(id)-4:] }
