# Part 2: package registry

Part 2 adds signed package history above the existing DHT. It uses only Go's
standard library and keeps the Part 1 blob storage and lookup rules unchanged.

## Choices and limits

- Versions are positive uint64 integers; each successor must be strictly greater
  than its predecessor. Zero is reserved for `--prev=0` (no predecessor).
- Domain and package names use lowercase ASCII letters, digits, dots, hyphens
  and underscores, up to 253 bytes each.
- Ownership verification uses the lab's permitted fake DNS: a static JSON
  `owners` map from domain to a hex-encoded 32-byte Ed25519 public key. Every node
  receives the same public map. There are no real DNS queries or key rotation.
- Publishers additionally load a `seeds` map containing their own hex-encoded
  32-byte Ed25519 seeds. These seeds stay in the publisher's shell process and
  are never transmitted. A node without a seed can serve, verify and install.
- The existing per-value limit applies to files and encoded records: 1,024 bytes
  by default, configurable up to 32,768 with `-max-value-size`. The default fits
  ordinary short-name signed records and small demonstration packages. Long
  names may require raising it. UDP transport is unchanged.
- Stored data and accepted heads remain in memory, with no expiration. Restart
  loses local history knowledge, as with Part 1 data. The system has no global
  consensus or partition-wide fork prevention: nodes enforce consistency with
  their accepted history and report conflicting histories they observe.

## Data and validation

`PackageRecord` in `src/kademlia/registry.go` defines the wire fields in order.
A version record contains tag, domain, package, version, blobHash, previous and
sig. A latest pointer contains tag, domain, package, version, versionRecordHash
and sig. Absent fields are omitted. Hashes are lowercase SHA-256 hex strings;
JSON encodes signatures as base64.

Signatures are Ed25519 over SHA-256 of the Go JSON encoding with sig omitted.
Version records are stored under SHA-256 of the complete signed record bytes.
Their previous field is the preceding record's hash, or 64 zeroes for genesis.
Package blobs retain the ordinary `hash(bytes) -> bytes` mapping.

Latest pointers are held separately under `hash("domain:package:latest")`.
Only the registry update path can modify this map; ordinary STORE still rejects
any value that does not match its content hash. Signed records alone do not
publish a version: the latest-pointer update must be accepted.

An update verifies ownership, record fields, matching domain/package/version,
and the entire chain to genesis. Missing records are fetched with existing DHT
lookups and retained locally. Versions must decrease when walking backwards.
Repeated hashes, broken links, invalid signatures and inconsistent fields fail.
After fetching, a mutex protects the ancestry check against the *current* head
and its replacement together. Competing local updates cannot both fork the head.
An identical head is idempotent; a lower/equal conflicting version or a chain
omitting the accepted head is rejected.

Latest lookup queries all discovered closest peers, includes local knowledge,
and checks that all observed heads belong to the newest verified chain.
It updates local state rather than trusting the first reply or a local hit.
Unreachable replicas can still conceal newer versions; this is not a quorum or
a guarantee of globally freshest reads. A stale update rejection includes the
receiver's current head so the sender can verify it and catch up.

## RPC, maintenance and thread safety

Two RPC methods reuse existing unpredictable request IDs, response matching,
UDP transport, cancellation and retries:

- `REGISTRY_GET`: request `{key}`; response `{key, head?}`.
- `REGISTRY_UPDATE`: request `{key, head}`; response `{key, head?, error?}`.
  A success echoes the exact head; a rejection may include the current head.

Update handlers use at most 16 concurrent workers with a 30-second validation
deadline. This lets the receive loop process RPC responses needed to fetch
history. Excess updates are dropped and subject to the existing retry policy.
Shutdown cancels and joins workers. No head lock is held during network I/O.

Periodic replication first replicates immutable values, then signed heads.
Join transfers also include relevant heads. Receivers use the same validation
on all paths. A failed publication may leave blobs/records or some updated
replicas, just as Part 1 STORE permits partial writes; it reports an error and
does not claim an atomic network-wide transaction.

## Running a demonstration

From the repository, using a WSL shell:

```sh
umask 077
go run . -registry-keygen example.test > /tmp/publisher.json
python3 -c 'import json; p=json.load(open("/tmp/publisher.json")); json.dump({"owners":p["owners"]},open("/tmp/owners.json","w"))'
printf 'version one' > /tmp/package-one
printf 'version two' > /tmp/package-two
```

Publisher terminal:

```sh
go run . -listen 127.0.0.1:8000 -registry-keys /tmp/publisher.json
```

Reader terminal:

```sh
go run . -listen 127.0.0.1:8001 -bootstrap 127.0.0.1:8000 -registry-keys /tmp/owners.json
```

Publish in the first shell:

```text
show dns example.test
publish example.test:demo:1 /tmp/package-one
publish example.test:demo:2 /tmp/package-two
show example.test:demo
show ds
publish --force --prev=0 example.test:demo:3 /tmp/package-two
publish --force example.test:demo:1 /tmp/package-one
```

The final two attempts must be rejected. Without `--force`, publication also
checks ownership, version ordering and predecessor selection before uploading.
With `--force`, it sends the invalid update to storage targets, which still
validate it. `--prev=N` selects an existing version from the known chain;
`--prev=0` attempts a new genesis. Unknown predecessors cannot be resolved.
Literal cycles in content-addressed records cannot practically be constructed
without breaking hash assumptions; validation also rejects repeated hashes and
non-decreasing backward links.

Install in the reader shell:

```text
install example.test:demo:latest
install example.test:demo:1
```

Files are written into the reader's current directory as
`example.test_demo_latest.pkg` and `example.test_demo_1.pkg`. Existing files are
not overwritten. For containers, mount the appropriate key JSON read-only and
pass `-registry-keys` to each process; include seeds only for publisher nodes.

## Verification

The registry tests cover publication and installation of old/latest versions,
wrong owners, tampered records, forks, rollback, concurrent conflicting updates,
idempotence, missing-history catch-up, stale-sender catch-up, replication,
join transfer, publisher departure, cancellation, and real UDP. CLI tests cover
key generation/configuration, all new commands and malformed input. Existing
1,000-node simulated-network tests remain part of the full suite.

Run:

```sh
go test -race -count=1 -timeout=180s -coverprofile=/tmp/coverage.out ./...
go tool cover -func=/tmp/coverage.out
go vet ./...
```

The existing Part 1 experiment results in REPORT.md predate this implementation;
they have not been regenerated as Part 2 measurements.

