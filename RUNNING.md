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
starts one bootstrap plus 49 peers in groups of at most ten, and waits until all nodes report successful
startup and join:

```sh
python3 scripts/lab.py up
```

Change the total with `--nodes 5`, or isolate another deployment with
`--project another-lab`. The default Compose project is `kadlab`.

The bootstrap owns a private Docker network namespace and binds port 8000.
Peers run in separate containers but share that network namespace using Compose
`network_mode: service:bootstrap`. Each peer uses `-listen auto:0` to obtain a
different UDP port. All nodes therefore share an IP but have distinct IP:port
addresses and SHA-256 node IDs. The Ready line prints each actual address.

This topology was verified with 50 joined nodes. Separate bridge endpoints
repeatedly produced DNS/UDP timeouts above roughly 30 containers on this Docker
Desktop/WSL setup; the underlying environment cause was not established.
Sharing the namespace avoids that observed limitation, but does not test network
isolation between containers. Node processes, storage and filesystems remain
separate. This is a single-computer deployment, not a multi-host configuration.

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
Lookup instrumentation and experiments remain separate planned steps.
