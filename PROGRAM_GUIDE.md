# Running the Kademlia node and package registry

This guide covers the current application, its interactive commands, Docker deployment, tests, and experiments.

## 1. Environment and prerequisites

Run the `bash` commands below in **Ubuntu/WSL**, not PowerShell. Run `text` commands inside the application's `>` prompt. Unless stated otherwise, start in the repository root:

```bash
cd /home/sebpet-9/D7024e/New-D7024e-Labs/D7024e-labs
go version
```

The module requires Go 1.22 or newer. The Docker build uses Go 1.25.4. The application uses the Go standard library, so no separate application dependencies need installing.

Additional requirements:

- Python 3 for key-file preparation, Docker helpers, and experiment analysis.
- Docker with Compose v2 for container deployment; Docker must be accessible from WSL.
- A C compiler and CGO enabled for Go's race detector.
- Python `venv` support and the plotting dependencies below for experiment figures.

Build an executable, or use `go run .` directly:

```bash
go build -o /tmp/kadlab .
/tmp/kadlab -help
```

Every example using `go run .` can instead use `/tmp/kadlab`. Rebuild the executable after changing the source.

## 2. Start two local nodes

In terminal 1, start the first node:

```bash
go run . -listen 127.0.0.1:8000
```

In terminal 2, join it from another node:

```bash
go run . -listen 127.0.0.1:8001 -bootstrap 127.0.0.1:8000
```

Wait for `Ready` before entering commands. Each node needs a different listening address/port. A first node needs no bootstrap; additional nodes can join through a reachable existing node.

In terminal 2's application prompt:

```text
ping 127.0.0.1:8000
show rt
help
```

Use `exit`, Ctrl-C, or EOF (Ctrl-D in an interactive Linux terminal) to stop an interactive node. A headless node serves until interrupted:

```bash
go run . -listen 127.0.0.1:8002 -bootstrap 127.0.0.1:8000 -headless
```

For separate machines, use a concrete reachable IP address or `-listen auto:8000`, and pass the actual address printed by the bootstrap to peers. Loopback addresses work only on the same machine. UDP traffic must be able to reach the selected ports. `auto` selects a non-loopback IPv4 address; port `0` requests an available port. Bootstrap and ping addresses may use hostnames.

## 3. Store and retrieve a file by hash

In an ordinary WSL terminal, create a small file:

```bash
printf 'Hello from the DHT\n' > /tmp/kadlab-example.txt
```

In node 1's application prompt:

```text
put /tmp/kadlab-example.txt
show ds
```

Copy the full 64-character key printed after `Stored`. In node 2's prompt, replace `KEY` with that key:

```text
get KEY
get KEY /tmp/kadlab-downloaded.txt
```

`get KEY` prints the contents; the filename form writes the exact bytes and is preferable for binary data. Both report the supplying node. `get` **overwrites an existing destination file**. Filenames can contain spaces and can be enclosed in double quotes.

Raw `put` and `get` verify content hashes but do not create named package versions or publisher signatures. Use the registry workflow below for that.

## 4. Publish and install package versions

Stop the two example nodes first if you want to reuse ports 8000 and 8001. Create the key configuration and demonstration files from the repository root:

```bash
umask 077
go run . -registry-keygen example.test > /tmp/kadlab-publisher.json
python3 - <<'PY'
import json
from pathlib import Path
publisher = json.loads(Path('/tmp/kadlab-publisher.json').read_text())
Path('/tmp/kadlab-owners.json').write_text(json.dumps({'owners': publisher['owners']}))
PY
printf 'version one\n' > /tmp/kadlab-package-one.txt
printf 'version two\n' > /tmp/kadlab-package-two.txt
```

The publisher configuration contains both `owners` (public keys) and `seeds` (private signing material). The reader configuration contains only `owners`. Give all participating nodes the same public ownership mapping; keep publisher seeds private. This mapping is the simulated DNS, and no real DNS query establishes package ownership. Generate keys once for a demonstration: regenerating them changes the owner key.

In terminal 1:

```bash
go run . -listen 127.0.0.1:8000 -registry-keys /tmp/kadlab-publisher.json
```

In terminal 2:

```bash
go run . -listen 127.0.0.1:8001 -bootstrap 127.0.0.1:8000 -registry-keys /tmp/kadlab-owners.json
```

In the publisher's application prompt:

```text
show dns example.test
publish example.test:demo:1 /tmp/kadlab-package-one.txt
publish example.test:demo:2 /tmp/kadlab-package-two.txt
show example.test:demo
show ds
```

In the reader's prompt:

```text
install example.test:demo:latest
install example.test:demo:1
```

These save `example.test_demo_latest.pkg` and `example.test_demo_1.pkg` in the reader process's working directory. Only the package contents are written; signed records are fetched and checked during installation. Earlier versions remain in the history. Installation **refuses to overwrite existing files**; use a fresh working directory with the compiled executable or rename previous output files before repeating it. This repository may already contain demonstration `.pkg` files.

Versions are explicitly supplied positive integers, not automatically incremented or semantic-version strings. New versions must increase, but need not be consecutive. Domain and package names accept lowercase ASCII letters, digits, dots, hyphens, and underscores, up to 253 bytes each.

### Demonstrate rejected history updates

After publishing versions 1 and 2, run these in the publisher's prompt:

```text
publish --force --prev=0 example.test:demo:3 /tmp/kadlab-package-two.txt
publish --force example.test:demo:1 /tmp/kadlab-package-one.txt
```

The first attempts to replace the history with a new genesis; the second attempts a rollback. Both should be rejected. `--force` bypasses publisher-side prechecks, but receiving nodes still validate updates. `--prev=N` chooses a known preceding version; `--prev=0` means no predecessor. Invalid attempts can still leave uploaded blobs or records, even though the latest-pointer update is rejected.

## 5. Interactive command reference

| Command | Purpose |
| --- | --- |
| `help` | Print the available commands. |
| `ping IP:PORT` | Contact a node and print its round-trip time. |
| `put FILENAME` | Store bytes and print their SHA-256 key. |
| `get KEY [FILENAME]` | Retrieve verified bytes; print them or save to a file. |
| `show rt` | Display non-empty routing buckets and contacts. |
| `show ds` | Display local stored keys, sizes, and registry records/pointers. |
| `publish [--force] [--prev=N] DOMAIN:PACKAGE:VERSION FILENAME` | Publish a signed package version using a configured owner key. |
| `install DOMAIN:PACKAGE:VERSION` | Retrieve a numbered version or use `latest` as VERSION. |
| `show DOMAIN:PACKAGE` | Fetch and display the verified package history. |
| `show dns DOMAIN` | Display the configured owner's public key. |
| `exit` | Shut down this node. |

The application prompt is not a general shell: use another terminal for filesystem commands. Relative file paths are relative to the node process's working directory.

## 6. Startup flags

```bash
go run . -help
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `-listen` | `127.0.0.1:8000` | Concrete IP:port or `auto:PORT`; port 0 selects a free port. |
| `-bootstrap` | Empty | Existing node to contact when joining. |
| `-headless` | `false` | Serve without reading interactive commands. |
| `-registry-keys` | Empty | JSON public ownership mapping and optional private seeds. |
| `-registry-keygen` | Empty | Generate keys for the supplied domain, print JSON, and exit. |
| `-lookup-log` | Empty | Create a new JSONL lookup log; existing files are refused. |
| `-command-timeout` | `30s` | Deadline for startup joining and each interactive command. |
| `-k` | `10` | Bucket capacity and target replica count; positive integer. |
| `-alpha` | `3` | Lookup/blob-write parallelism; positive integer. |
| `-max-value-size` | `1024` | Maximum unencoded value size, from 255 to 32768 bytes. |
| `-rpc-timeout` | `2s` | Positive timeout for each RPC attempt. |
| `-rpc-retries` | `2` | Additional attempts after timeouts; 0 disables retries. |
| `-refresh-period` | `1h` | Positive interval between routing refresh passes. |
| `-replication-period` | `1h` | Positive interval between replication passes. |
| `-help` | — | Print usage and exit. |

Duration values use Go notation such as `100ms`, `10s`, or `2m`. Use consistent value-size and ownership settings across nodes. The value limit applies to encoded registry records as well as package files, so long names may require increasing it.

For example, start a node with more frequent maintenance and a new lookup log:

```bash
go run . -listen 127.0.0.1:8003 -bootstrap 127.0.0.1:8000 \
  -k 10 -alpha 3 -max-value-size 32768 \
  -refresh-period 1m -replication-period 1m -lookup-log /tmp/kadlab-lookups-01.jsonl
```

## 7. Docker deployment

From the repository root, check Docker and start the default 50-node network:

```bash
docker version
docker compose version
python3 scripts/lab.py up
```

The helper builds the image, starts a bootstrap and 49 peers, and waits for readiness. The current Compose file gives containers separate interfaces on a private bridge network. No host port mappings are configured.

For a smaller, separately named deployment:

```bash
python3 scripts/lab.py up --nodes 5 --project kadlab-small
docker compose -p kadlab-small ps
```

`--nodes` includes the bootstrap. Helper options also include `--batch-size` (default 3) and `--startup-retries` (default 3). For slower startup, use `--batch-size 1 --startup-retries 5`. Repeating `up` can grow/recover a deployment, but does not silently shrink it or recreate all existing nodes with changed settings. Use `down` and `up` for a complete restart.

Attach to the default deployment's existing bootstrap:

```bash
docker attach --detach-keys=ctrl-p,ctrl-q "$(docker compose -p kadlab ps -q bootstrap)"
```

Press Enter if needed. Detach with Ctrl-P followed by Ctrl-Q to leave it running. `exit` or Ctrl-C stops the node. Use `docker attach CONTAINER` for another existing node; launching the executable with `docker exec` would create a separate node.

Files must exist inside the container for `put` or `publish`. In a WSL terminal:

```bash
docker cp /tmp/kadlab-example.txt "$(docker compose -p kadlab ps -q bootstrap)":/tmp/example.txt
```

The image runs as UID 10001, so copied files must be readable by that user. Inside the attached application:

```text
put /tmp/example.txt
```

Use `docker cp CONTAINER:/path/to/output ./output` to copy downloaded files back. Inspect logs and run the isolated three-node smoke test with:

```bash
docker compose -p kadlab logs --tail 50
python3 scripts/lab.py smoke
```

The smoke test checks storage and retrieval after a temporary publisher exits and removes its temporary deployment when finished.

Stop and remove a deployment with its matching project name:

```bash
python3 scripts/lab.py down
python3 scripts/lab.py down --project kadlab-small
```

### Registry configuration in containers

The default Compose setup does not load registry keys. To enable publication/installation, add a read-only bind mount for each service's key configuration and append `-registry-keys /config/keys.json` to that service's existing command. Mount the publisher configuration only on publisher nodes and the public-only configuration on other nodes. Ensure UID 10001 can read the mounted file. Recreate the deployment after changing Compose configuration. See [PART2.md](PART2.md) for the record and ownership design.

## 8. Tests and static checks

From the repository root:

```bash
go test ./...
CGO_ENABLED=1 go test -race -count=1 -timeout=180s -coverprofile=/tmp/kadlab-coverage.out ./...
go tool cover -func=/tmp/kadlab-coverage.out
go vet ./...
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_*.py'
```

The race-enabled suite requires a working C compiler. Coverage output reports statement coverage; the Python tests cover helper/analysis behaviour.

## 9. Simulated experiments and plots

Docker is not required. Prepare the plotting environment once:

```bash
python3 -m venv .venv-analysis
.venv-analysis/bin/python -m pip install -r scripts/requirements-analysis.txt
```

Preview or run the combined experiment and analysis workflow:

```bash
.venv-analysis/bin/python scripts/run_experiments.py --dry-run
.venv-analysis/bin/python scripts/run_experiments.py
```

Each real invocation creates a timestamped directory under `results/` with raw JSONL, analysed CSVs/tables/figures, and a manifest. Existing output directories are refused. The full matrix builds many simulated networks and can take substantial time.

Optional wrapper arguments:

| Argument | Meaning |
| --- | --- |
| `--out results/my-run` | Use a new named output directory. |
| `--queries 100` | Lookups per seed and condition; default 20. |
| `--dry-run` | Print planned commands without executing the experiments. |

Inspect the actual matrix and underlying runner options:

```bash
go run ./cmd/experiments -describe
go run ./cmd/experiments -help
```

The runner accepts `-out`, `-queries`, `-rpc-timeout`, `-query-timeout`, and `-describe`. Network sizes, loss probabilities, and seeds are defined in `experiments.DefaultSettings`. Lowering query count does not reduce the number of network topologies built.

To run and analyse separately, choose fresh output paths:

```bash
go run ./cmd/experiments -out results/manual-run
.venv-analysis/bin/python scripts/analyze_experiments.py results/manual-run --out results/manual-analysis
```

To regenerate plots from a completed wrapper run, replace `YOUR-RUN`:

```bash
.venv-analysis/bin/python scripts/analyze_experiments.py results/YOUR-RUN/raw --out results/YOUR-RUN/replotted
```

Interrupted runs leave partial evidence. Choose a new directory for a rerun and use only complete runs for report conclusions. See [RUNNING.md](RUNNING.md) for output details and [RPC.md](RPC.md) for logging semantics.

## 10. Common problems and behaviour

| Symptom | Check/action |
| --- | --- |
| Address already in use | Stop the old node or select another port. |
| Join timeout | Check the bootstrap's printed address, readiness, and UDP connectivity; increase `-command-timeout` when appropriate. |
| Value too large | Check both file and signed-record sizes; configure participating nodes with a larger supported limit. |
| Publisher requires a private key | Start the publisher with the configuration containing the domain's seed. |
| Invalid owner signature | Ensure every node has the same domain/public-key mapping and that the publisher uses the matching private key. |
| Fork or rollback rejected | Publish a higher version extending the accepted history. `--force` cannot disable receiver validation. |
| Installation output already exists | Rename the old output or run the compiled node from a fresh directory. |
| Lookup log already exists | Choose a fresh filename. |
| Container cannot read input | Check the path inside the container and readability for UID 10001. |
| Data missing after restart | Data and accepted latest pointers are memory-only. Keep replicas running or publish again in a fresh network. |

Replication and absence of expiration do not provide disk persistence. A partial storage/publication failure can leave some copies behind; do not treat an error as proof that nothing changed. `latest` discovers the newest consistent history among observed replicas, not a globally guaranteed newest version.
