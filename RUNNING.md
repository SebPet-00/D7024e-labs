# Running the Part 1 node (step 10)

Run these commands from the repository in WSL, with Go on PATH.

## Two local nodes

First terminal:

```sh
go run . -listen 127.0.0.1:8000
```

Second terminal:

```sh
go run . -listen 127.0.0.1:8001 -bootstrap 127.0.0.1:8000
```

A node prints Ready after binding UDP and completing its optional join.
The address used for its ID is the actual bound IP:port. Port 0 selects a free
port. Bootstrap and ping accept hostnames and resolve them to concrete addresses.
The first node starts without a bootstrap.

## Interactive commands

| Command | Result |
| --- | --- |
| `ping IP:PORT` | Prints the responding address and RTT. |
| `put FILENAME` | Reads the file as bytes, distributes it, and prints its full SHA-256 key. |
| `get KEY [FILENAME]` | Saves the exact bytes to a file, or prints them, and reports the supplying node. |
| `show rt` | Prints non-empty buckets with abbreviated IDs and addresses. |
| `show ds` | Prints abbreviated stored keys and byte counts. |
| `exit` | Closes the node and its workers. |

`help` prints the command list. Filenames occupy the rest of the command, so
spaces work; surrounding double quotes are accepted. File output replaces an
existing destination file. For binary values, use the filename form of get.
A failed command prints an error and leaves the shell running. A partial put
prints the key marked as incomplete rather than reporting success.
EOF also closes an interactive node.

The default value limit is 1024 bytes. Use `-max-value-size` to change it
within 255–32768 bytes. `-k`, `-alpha`, `-rpc-timeout`, `-rpc-retries`,
`-refresh-period`, and `-replication-period` expose the existing settings.
`-command-timeout` defaults to 30s and bounds each command and the startup join.
`-headless` serves without reading stdin. SIGINT/SIGTERM closes either mode.
Run `go run . -help` for all flags.

## Docker: 50 nodes

Requires Docker with Compose v2 and Python 3. The script builds the local image,
starts one bootstrap plus 49 peers in groups of at most three, and waits until all nodes report successful
startup and join:

```sh
python3 scripts/lab.py up
```

Change the total with `--nodes 5`, or isolate another deployment with
`--project another-lab`. The default Compose project is `kadlab`.

Each container has its own IP on the private `kademlia` bridge network.
Bootstrap listens on port 8000; peers use `auto:0` for an OS-assigned UDP port.
Stopping bootstrap does not remove the other containers' network interfaces.

Startup retries transient join failures individually, with backoff and a default
limit of three retries per node. Fatal errors are reported rather than hidden.
Readiness uses logs from the current process start, not historical Ready lines.
Rerunning up recovers existing peers before growing the lab and refuses to
silently reduce the node count. Existing peers are not recreated during growth;
run down then up to apply a changed image or Compose command to every peer.
Permanent failures still produce an error.
Use `--batch-size 1 --startup-retries 5` for a more conservative startup.
Docker Desktop/WSL has previously shown transient UDP join failures during batch
startup; retries mitigate these without assuming a particular underlying cause.

The script discovers the bootstrap's assigned IP and passes it through
KAD_BOOTSTRAP. Direct Compose use defaults to bootstrap:8000.
No host port mappings or Swarm setup are needed. The multi-stage Dockerfile
builds a static Go executable and runs it as an unprivileged user.

Attach to the existing bootstrap shell:

```sh
docker attach --detach-keys=ctrl-p,ctrl-q "$(docker compose -p kadlab ps -q bootstrap)"
```

Press Enter if the initial prompt is no longer visible. Detach with Ctrl-P,
then Ctrl-Q to keep the node running. Typing exit or Ctrl-C stops that node.
Use `docker compose -p kadlab ps` to list peers and `docker attach CONTAINER`
to interact with another node. A new `docker exec ... kadlab` process would
create a separate node, so use attach to control the existing one.

Files used by put/get are inside the selected container. For example:

```sh
docker cp ./example.txt "$(docker compose -p kadlab ps -q bootstrap)":/tmp/example.txt
```

Then run `put /tmp/example.txt` in its shell. Ensure uploaded files are readable
by the container's unprivileged user. Download files can be copied back with
docker cp. Values are memory-only and are lost when all storing nodes stop.

Stop and remove this deployment:

```sh
python3 scripts/lab.py down
```

## Container smoke test

```sh
python3 scripts/lab.py smoke
```

This creates a uniquely named three-node deployment, joins a temporary publisher,
checks PING and STORE, exits that publisher, then retrieves the value from a
different temporary node. It checks the supplying-node output and routing
display, and removes its containers/network on completion or failure.

## Verification

```sh
CGO_ENABLED=1 go test -race -count=1 -cover ./...
go vet ./...
```

Go tests cover shell commands, binary files, filenames with spaces, error paths,
startup/join/shutdown, and independent inspection snapshots.
Step 11 below adds lookup instrumentation and reproducible experiments.

## Lookup logs and experiments (step 11)

Create a new structured JSONL log while running the shell:

```sh
go run . -listen 127.0.0.1:8000 -lookup-log lookups.jsonl
```

A repeated invocation must use a new filename; existing logs are never overwritten.
Probes count logical peer RPCs; attempts include retransmissions. Local hits
have zero probes. Field definitions and failure behavior are in RPC.md.

Run both mandatory report experiments and their analysis with one script.
In WSL, prepare the plotting environment once:

```sh
python3 -m venv .venv-analysis
.venv-analysis/bin/python -m pip install -r scripts/requirements-analysis.txt
```

Then run:

```sh
.venv-analysis/bin/python scripts/run_experiments.py
```

The script uses the existing Go workload runner and external Python analyzer.
It runs an expanded version of the original matrix documented in REPORT.md:

| Experiment | Conditions | Seeds | Lookups per seed/condition |
| --- | --- | --- | --- |
| Node lookup scalability | N = 25, 100, 250, 500, 1000, 1500, 2000; loss = 0 | 11, 22, 33, 44, 55 | 20 |
| Value retrieval reliability | N = 100; loss = 0, 0.1, 0.3, then 0.50 through 0.70 in steps of 0.02, then 0.9 | 11, 22, 33, 44, 55 | 20 |

These are 110 fresh simulated networks and 2,200 measured lookups. K=10,
alpha=3, two retries, 100 ms per RPC attempt and a two-second lookup deadline
match the report. Setup joins and stores are loss-free. The analyzer verifies
logged probe counts and reports averages and sample variance across seed means;
figure error bars show standard deviation. Scalability figures include an
anchored log2(N) reference. Docker is not needed for these experiments.

Each invocation creates a fresh `results/report-<UTC timestamp>/` containing
`raw/*.jsonl`, `analysis/` (CSV data, Markdown tables, PNG/SVG/PDF figures), and
`manifest.json` (commands, source revision/working-tree status and completion
status). Existing output directories are refused. Failures stop analysis and
retain partial evidence; an incomplete manifest must not be treated as a
finished evaluation. A failed analyzer can leave partial analysis files.

Use `--out results/my-report-run` for a named directory or `--dry-run` to
preview commands without running anything. `--queries 2` reduces measured
lookups but still builds every topology; use the default 20 for the report.
Relative output paths are resolved against the repository, even when invoked
from another directory. The script checks Go and plotting dependencies before
starting; it does not install dependencies itself.

For more samples near the onset of observed failures, use:

```sh
.venv-analysis/bin/python scripts/run_experiments.py --queries 100
```

This collects 500 lookups per condition (11,000 total). More samples help reveal
rare failures; the first condition with an observed failure is not a universal
failure threshold. Queries within each run share evolving routing state, so a
longer workload also measures more of that evolution. The same five seeds are
used. Change `experiments.DefaultSettings` to adjust sizes, loss probabilities
or seeds; the wrapper reads these settings using the Go command's `-describe`
flag and records them in the manifest. JSON duration fields are nanoseconds.

### Reusing the data for other graphs

No new experiment is needed to plot already recorded measurements:

- `analysis/summary.csv`: one row per condition, with means and sample variances.
- `analysis/runs.csv`: one row per seed/condition, for comparing seeds.
- `analysis/lookups.csv`: one row per lookup, for distributions, failures and retries.
- `raw/*.jsonl`: original events and settings, including elapsed time on
  `lookup_end` records (not currently exported to the CSVs).

These files can be read with Python, R, Excel or another plotting tool. Use
`sqrt(variance)` for the current figures' standard-deviation error bars. Keep
seed-level variation distinct from variation between individual lookups.
Re-run just the analyzer to regenerate the existing graphs from saved raw data:

```sh
.venv-analysis/bin/python scripts/analyze_experiments.py results/YOUR-RUN/raw --out results/YOUR-RUN/replotted
```

The current REPORT.md warns that its measurements predate an implementation
change. After a successful run, use the new tables and figures to update the
report's results and discussion; the script preserves the existing report and
historical data. Optional alpha, churn and replication-factor sweeps from the
lab specification are not part of the mandatory matrix.

The underlying commands can also be run separately:

```sh
go run ./cmd/experiments -out results/repeat
python3 -m venv .venv-analysis
.venv-analysis/bin/pip install -r scripts/requirements-analysis.txt
.venv-analysis/bin/python scripts/analyze_experiments.py results/repeat --out results/repeat-analysis
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_*.py'
```

Default sizes, loss probabilities and seeds are in experiments.DefaultSettings.
The command exposes query count, RPC timeout and lookup deadline. Refusing to
overwrite raw files prevents accidentally mixing a rerun with earlier data.
An interrupted run leaves partial evidence; choose a fresh output directory
for a rerun. The analyzer rejects incomplete runs rather than treating them as
successful samples.

REPORT.md explains the methodology, measured results, limitations and remaining
report cover-page information. Raw results are in results/part1; CSV summaries,
Markdown tables and PNG/SVG/PDF figures are in results/analysis.

## Part 2 package registry

See [PART2.md](PART2.md) for key generation, static DNS ownership configuration,
publish/install commands, invalid-update demonstrations, and container configuration.
