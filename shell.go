package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"d7024e/src/kademlia"
)

const shellHelp = "Commands: ping IP:PORT | put FILENAME | get KEY [FILENAME] | show rt | show ds | exit"

func runShell(ctx context.Context, node *kademlia.Kademlia, maxValueSize int, timeout time.Duration, input io.Reader, output io.Writer) error {
	fmt.Fprintln(output, shellHelp)
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 64*1024)
	for {
		fmt.Fprint(output, "> ")
		if !scanner.Scan() {
			break
		}
		if ctx.Err() != nil {
			return nil
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		commandCtx, cancel := context.WithTimeout(ctx, timeout)
		exit, err := executeCommand(commandCtx, node, maxValueSize, line, output)
		cancel()
		if err != nil {
			fmt.Fprintf(output, "Error: %v\n", err)
		}
		if exit {
			return nil
		}
	}
	if ctx.Err() != nil {
		return nil
	}
	return scanner.Err()
}

// Filenames occupy the remainder of the command, so spaces need no escaping.
// A surrounding pair of double quotes is also accepted.
func filename(text string) string { return strings.Trim(strings.TrimSpace(text), "\"") }

func executeCommand(ctx context.Context, node *kademlia.Kademlia, maxValueSize int, line string, output io.Writer) (bool, error) {
	command, rest, _ := strings.Cut(strings.TrimSpace(line), " ")
	rest = strings.TrimSpace(rest)
	switch command {
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
			entries := node.DataSnapshot()
			if len(entries) == 0 {
				fmt.Fprintln(output, "Data store is empty")
			}
			for _, entry := range entries {
				fmt.Fprintf(output, "%s %d bytes\n", shortID(entry.Key.String()), entry.Size)
			}
		default:
			return false, fmt.Errorf("usage: show rt | show ds")
		}
	default:
		return false, fmt.Errorf("unknown command %q; type help", command)
	}
	return false, nil
}

func shortID(id string) string { return id[:4] + "..." + id[len(id)-4:] }
