package kademlia

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Ownership is a static DNS oracle. Configure identical public keys on all nodes.
type Ownership map[string]ed25519.PublicKey

// PackageRecord is encoded with Go JSON in field order. Signatures cover the
// SHA-256 of that encoding with sig omitted. Hashes are lowercase hexadecimal.
type PackageRecord struct {
	Tag        string `json:"tag"`
	Domain     string `json:"domain"`
	Package    string `json:"package"`
	Version    uint64 `json:"version"`
	BlobHash   string `json:"blobHash,omitempty"`
	Previous   string `json:"previous,omitempty"`
	RecordHash string `json:"versionRecordHash,omitempty"`
	Signature  []byte `json:"sig,omitempty"`
}

var ErrPackageNotFound = errors.New("package not found")
var ErrHistory = errors.New("fork or rollback rejected")
var zeroHash = strings.Repeat("0", 64)

type Registry struct {
	node   *Kademlia
	owners Ownership
	mu     sync.Mutex
	heads  map[KademliaID]PackageRecord
	ctx    context.Context
	cancel context.CancelFunc
}

func newRegistry(node *Kademlia, owners Ownership) *Registry {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Registry{node: node, owners: make(Ownership), heads: make(map[KademliaID]PackageRecord), ctx: ctx, cancel: cancel}
	for d, k := range owners {
		r.owners[d] = append(ed25519.PublicKey(nil), k...)
	}
	return r
}
func (k *Kademlia) Registry() *Registry { return k.registry }
func (r *Registry) Owner(domain string) ed25519.PublicKey {
	return append(ed25519.PublicKey(nil), r.owners[domain]...)
}
func latestKey(d, p string) KademliaID {
	return KademliaID(sha256.Sum256([]byte(d + ":" + p + ":latest")))
}
func validName(s string) bool {
	if s == "" || len(s) > 253 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '.' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}
func recordBytes(v PackageRecord) []byte    { b, _ := json.Marshal(v); return b }
func recordDigest(v PackageRecord) [32]byte { v.Signature = nil; return sha256.Sum256(recordBytes(v)) }
func signRecord(v PackageRecord, k ed25519.PrivateKey) PackageRecord {
	h := recordDigest(v)
	v.Signature = ed25519.Sign(k, h[:])
	return v
}
func (r *Registry) verify(v PackageRecord, tag string) error {
	if v.Tag != tag || !validName(v.Domain) || !validName(v.Package) || v.Version == 0 {
		return fmt.Errorf("invalid package record")
	}
	pk := r.owners[v.Domain]
	h := recordDigest(v)
	if len(pk) != ed25519.PublicKeySize || !ed25519.Verify(pk, h[:], v.Signature) {
		return fmt.Errorf("invalid domain-owner signature")
	}
	if len(recordBytes(v)) > r.node.config.MaxValueSize {
		return ErrValueTooLarge
	}
	hashes := []string{v.RecordHash}
	if tag == "version-record" {
		if v.RecordHash != "" {
			return fmt.Errorf("invalid version record fields")
		}
		hashes = []string{v.BlobHash, v.Previous}
	} else if v.BlobHash != "" || v.Previous != "" {
		return fmt.Errorf("invalid latest pointer fields")
	}
	for _, s := range hashes {
		id, e := NewKademliaID(s)
		if e != nil {
			return e
		}
		if id.String() != s {
			return fmt.Errorf("noncanonical hash")
		}
	}
	return nil
}

// Network I/O happens outside the head lock. Verified records are retained
// locally so head holders can serve their history when other nodes leave.
func (r *Registry) chain(ctx context.Context, head PackageRecord) ([]PackageRecord, []string, error) {
	if e := r.verify(head, "latest-pointer"); e != nil {
		return nil, nil, e
	}
	hash, bound, first := head.RecordHash, head.Version, true
	var records []PackageRecord
	var hashes []string
	seen := map[string]bool{}
	for hash != zeroHash {
		if e := ctx.Err(); e != nil {
			return nil, nil, e
		}
		if seen[hash] {
			return nil, nil, ErrHistory
		}
		seen[hash] = true
		value, e := r.node.LookupData(ctx, hash)
		if e != nil {
			return nil, nil, e
		}
		var v PackageRecord
		if e = json.Unmarshal(value.Data, &v); e != nil {
			return nil, nil, e
		}
		if e = r.verify(v, "version-record"); e != nil {
			return nil, nil, e
		}
		if v.Domain != head.Domain || v.Package != head.Package || (first && v.Version != bound) || (!first && v.Version >= bound) {
			return nil, nil, ErrHistory
		}
		key, _ := NewKademliaID(hash)
		if e = r.node.dataStore.put(*key, value.Data); e != nil {
			return nil, nil, e
		}
		records = append(records, v)
		hashes = append(hashes, hash)
		hash, bound, first = v.Previous, v.Version, false
	}
	if len(records) == 0 {
		return nil, nil, ErrHistory
	}
	return records, hashes, nil
}

// accept atomically checks ancestry against the current head before committing.
// Identical retransmissions succeed; neither stale nor forked updates overwrite it.
func (r *Registry) accept(ctx context.Context, head PackageRecord) error {
	_, hashes, e := r.chain(ctx, head)
	if e != nil {
		return e
	}
	key := latestKey(head.Domain, head.Package)
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.heads[key]; ok {
		if old.RecordHash == head.RecordHash && old.Version == head.Version {
			return nil
		}
		if head.Version <= old.Version {
			return ErrHistory
		}
		found := false
		for _, h := range hashes {
			if h == old.RecordHash {
				found = true
			}
		}
		if !found {
			return ErrHistory
		}
	}
	head.Signature = append([]byte(nil), head.Signature...)
	r.heads[key] = head
	return nil
}
func (r *Registry) snapshot() []PackageRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]PackageRecord, 0, len(r.heads))
	for _, h := range r.heads {
		h.Signature = append([]byte(nil), h.Signature...)
		out = append(out, h)
	}
	return out
}

// Latest compares all closest reachable replicas and local knowledge. This
// detects observed forks; it does not promise consensus across a partition.
func (r *Registry) Latest(ctx context.Context, domain, pkg string) (PackageRecord, error) {
	if !validName(domain) || !validName(pkg) {
		return PackageRecord{}, fmt.Errorf("invalid domain or package")
	}
	key := latestKey(domain, pkg)
	peers, e := r.node.LookupContact(ctx, &key)
	if e != nil && !errors.Is(e, ErrNoReachableContacts) {
		return PackageRecord{}, e
	}
	var heads []PackageRecord
	var failures []error
	for _, h := range r.snapshot() {
		if latestKey(h.Domain, h.Package) == key {
			heads = append(heads, h)
		}
	}
	for _, peer := range peers {
		response, e := r.call(ctx, peer, "REGISTRY_GET", registryRequest{Key: key.String()})
		if e != nil {
			failures = append(failures, e)
			continue
		}
		if response.Head != nil {
			h := *response.Head
			if h.Domain != domain || h.Package != pkg {
				return PackageRecord{}, fmt.Errorf("wrong package response")
			}
			heads = append(heads, h)
		}
	}
	if len(heads) == 0 {
		if len(failures) > 0 {
			return PackageRecord{}, errors.Join(failures...)
		}
		return PackageRecord{}, ErrPackageNotFound
	}
	sort.Slice(heads, func(i, j int) bool { return heads[i].Version < heads[j].Version })
	newest := heads[len(heads)-1]
	records, hashes, e := r.chain(ctx, newest)
	if e != nil {
		return PackageRecord{}, e
	}
	for _, h := range heads {
		if e = r.verify(h, "latest-pointer"); e != nil {
			return PackageRecord{}, e
		}
		found := false
		for i, hash := range hashes {
			if hash == h.RecordHash && records[i].Version == h.Version {
				found = true
				break
			}
		}
		if !found {
			return PackageRecord{}, ErrHistory
		}
	}
	if e = r.accept(ctx, newest); e != nil {
		return PackageRecord{}, e
	}
	return newest, nil
}

type PublishOptions struct {
	Force    bool
	Previous *uint64
}

func (r *Registry) Publish(ctx context.Context, domain, pkg string, version uint64, data []byte, key ed25519.PrivateKey, opts PublishOptions) error {
	if len(key) != ed25519.PrivateKeySize {
		return fmt.Errorf("publisher requires an Ed25519 private key")
	}
	head, e := r.Latest(ctx, domain, pkg)
	if e != nil && !errors.Is(e, ErrPackageNotFound) {
		return e
	}
	previous := zeroHash
	if e == nil {
		previous = head.RecordHash
	}
	if opts.Previous != nil {
		previous = zeroHash
		if *opts.Previous != 0 {
			chain, hashes, err := r.chain(ctx, head)
			if err != nil {
				return err
			}
			found := false
			for i, v := range chain {
				if v.Version == *opts.Previous {
					previous = hashes[i]
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("previous version not found")
			}
		}
	}
	if !opts.Force {
		if version == 0 || (e == nil && (version <= head.Version || previous != head.RecordHash)) {
			return ErrHistory
		}
		if !key.Public().(ed25519.PublicKey).Equal(r.Owner(domain)) {
			return fmt.Errorf("signing key is not the domain owner")
		}
	}
	blob, e := r.node.Store(ctx, data)
	if e != nil {
		return e
	}
	v := signRecord(PackageRecord{Tag: "version-record", Domain: domain, Package: pkg, Version: version, BlobHash: blob.String(), Previous: previous}, key)
	record, e := r.node.Store(ctx, recordBytes(v))
	if e != nil {
		return e
	}
	h := signRecord(PackageRecord{Tag: "latest-pointer", Domain: domain, Package: pkg, Version: version, RecordHash: record.String()}, key)
	return r.distribute(ctx, h)
}
func (r *Registry) History(ctx context.Context, d, p string) ([]PackageRecord, error) {
	h, e := r.Latest(ctx, d, p)
	if e != nil {
		return nil, e
	}
	records, _, e := r.chain(ctx, h)
	return records, e
}
func (r *Registry) Install(ctx context.Context, d, p, version string) (*ValueResult, error) {
	records, e := r.History(ctx, d, p)
	if e != nil {
		return nil, e
	}
	for _, v := range records {
		if version == "latest" || fmt.Sprint(v.Version) == version {
			return r.node.LookupData(ctx, v.BlobHash)
		}
	}
	return nil, fmt.Errorf("version not found")
}
func (r *Registry) Describe() []string {
	var out []string
	for _, h := range r.snapshot() {
		out = append(out, fmt.Sprintf("latest-pointer %s:%s:%d -> %.8s", h.Domain, h.Package, h.Version, h.RecordHash))
	}
	for _, data := range r.node.dataStore.snapshot() {
		var v PackageRecord
		if json.Unmarshal(data, &v) == nil && v.Tag == "version-record" {
			out = append(out, fmt.Sprintf("version-record %s:%s:%d blob=%.8s prev=%.8s", v.Domain, v.Package, v.Version, v.BlobHash, v.Previous))
		}
	}
	sort.Strings(out)
	return out
}
func ParsePackage(s string, version bool) ([]string, error) {
	parts := strings.Split(s, ":")
	n := 2
	if version {
		n = 3
	}
	if len(parts) != n || !validName(parts[0]) || !validName(parts[1]) {
		return nil, fmt.Errorf("expected DOMAIN:PACKAGE[:VERSION]")
	}
	return parts, nil
}
