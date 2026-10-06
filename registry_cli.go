package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"d7024e/src/kademlia"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type registryKeys struct {
	Owners map[string]string `json:"owners"`
	Seeds  map[string]string `json:"seeds,omitempty"`
}

func generateRegistryKeys(domain string, out io.Writer) error {
	if _, e := kademlia.ParsePackage(domain+":check", false); e != nil {
		return e
	}
	pub, key, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		return e
	}
	return json.NewEncoder(out).Encode(registryKeys{Owners: map[string]string{domain: hex.EncodeToString(pub)}, Seeds: map[string]string{domain: hex.EncodeToString(key.Seed())}})
}
func loadRegistryKeys(path string) (kademlia.Ownership, map[string]ed25519.PrivateKey, error) {
	owners := make(kademlia.Ownership)
	keys := make(map[string]ed25519.PrivateKey)
	if path == "" {
		return owners, keys, nil
	}
	data, e := os.ReadFile(path)
	if e != nil {
		return nil, nil, e
	}
	var config registryKeys
	if e = json.Unmarshal(data, &config); e != nil {
		return nil, nil, e
	}
	for d, text := range config.Owners {
		if _, e = kademlia.ParsePackage(d+":check", false); e != nil {
			return nil, nil, e
		}
		pk, e := hex.DecodeString(text)
		if e != nil || len(pk) != ed25519.PublicKeySize {
			return nil, nil, fmt.Errorf("invalid public key for %s", d)
		}
		owners[d] = pk
	}
	for d, text := range config.Seeds {
		seed, e := hex.DecodeString(text)
		if e != nil || len(seed) != ed25519.SeedSize {
			return nil, nil, fmt.Errorf("invalid signing seed for %s", d)
		}
		key := ed25519.NewKeyFromSeed(seed)
		if !key.Public().(ed25519.PublicKey).Equal(owners[d]) {
			return nil, nil, fmt.Errorf("signing seed does not match owner of %s", d)
		}
		keys[d] = key
	}
	return owners, keys, nil
}
func executeRegistryCommand(ctx context.Context, node *kademlia.Kademlia, limit int, line string, out io.Writer, keys map[string]ed25519.PrivateKey) error {
	command, rest, _ := strings.Cut(line, " ")
	rest = strings.TrimSpace(rest)
	r := node.Registry()
	switch command {
	case "publish":
		opts := kademlia.PublishOptions{}
		for strings.HasPrefix(rest, "--") {
			option, tail, _ := strings.Cut(rest, " ")
			rest = strings.TrimSpace(tail)
			if option == "--force" {
				opts.Force = true
			} else if strings.HasPrefix(option, "--prev=") {
				n, e := strconv.ParseUint(strings.TrimPrefix(option, "--prev="), 10, 64)
				if e != nil {
					return e
				}
				opts.Previous = &n
			} else {
				return fmt.Errorf("unknown publish option %s", option)
			}
		}
		name, path, _ := strings.Cut(rest, " ")
		parts, e := kademlia.ParsePackage(name, true)
		if e != nil {
			return e
		}
		version, e := strconv.ParseUint(parts[2], 10, 64)
		if e != nil {
			return fmt.Errorf("version must be a positive integer")
		}
		file, e := os.Open(filename(path))
		if e != nil {
			return e
		}
		data, e := io.ReadAll(io.LimitReader(file, int64(limit)+1))
		file.Close()
		if e != nil {
			return e
		}
		if len(data) > limit {
			return kademlia.ErrValueTooLarge
		}
		if e = r.Publish(ctx, parts[0], parts[1], version, data, keys[parts[0]], opts); e != nil {
			return e
		}
		fmt.Fprintf(out, "Published %s\n", name)
	case "install":
		parts, e := kademlia.ParsePackage(rest, true)
		if e != nil {
			return e
		}
		result, e := r.Install(ctx, parts[0], parts[1], parts[2])
		if e != nil {
			return e
		}
		// Exclusive creation avoids silently overwriting an earlier download.
		path := parts[0] + "_" + parts[1] + "_" + parts[2] + ".pkg"
		file, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		_, e = file.Write(result.Data)
		closeErr := file.Close()
		if e != nil {
			return e
		}
		if closeErr != nil {
			return closeErr
		}
		fmt.Fprintf(out, "Installed %s to %s (%d bytes), received from %s\n", rest, path, len(result.Data), result.Source.Address)
	case "show":
		if strings.HasPrefix(rest, "dns ") {
			domain := strings.TrimSpace(strings.TrimPrefix(rest, "dns "))
			key := r.Owner(domain)
			if len(key) == 0 {
				fmt.Fprintf(out, "No owner key for %s\n", domain)
			} else {
				fmt.Fprintf(out, "%s %x\n", domain, key)
			}
			return nil
		}
		parts, e := kademlia.ParsePackage(rest, false)
		if e != nil {
			return e
		}
		history, e := r.History(ctx, parts[0], parts[1])
		if e != nil {
			return e
		}
		for _, v := range history {
			fmt.Fprintf(out, "%s:%s:%d blob=%s previous=%s\n", v.Domain, v.Package, v.Version, shortID(v.BlobHash), shortID(v.Previous))
		}
	}
	return nil
}
