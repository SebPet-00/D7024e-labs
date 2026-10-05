#!/usr/bin/env python3
"""Start/stop a local Compose lab or run an isolated container smoke test."""
import argparse
import json
import os
import pathlib
import re
import subprocess
import time
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]


def compose(project, *args, **kwargs):
    return subprocess.run(
        ["docker", "compose", "-p", project, "-f", str(ROOT / "docker-compose.yml"), *args],
        cwd=ROOT, check=True, text=True, **kwargs)


def snapshot(container):
    """Read state and only logs from the current process, never an old Ready."""
    info = json.loads(subprocess.check_output(
        ["docker", "inspect", container], text=True))[0]
    logs = subprocess.run(
        ["docker", "logs", "--since", info["State"]["StartedAt"], container],
        check=True, text=True, capture_output=True)
    return info, logs.stdout + logs.stderr


def wait_node(container, timeout=180, retries=3):
    deadline = time.monotonic() + timeout
    attempts = 0
    while time.monotonic() < deadline:
        info, logs = snapshot(container)
        state = info["State"]
        name = info["Name"].lstrip("/")
        if state["Running"] and re.search(r"^Ready ", logs, re.MULTILINE):
            return
        if state["Status"] == "exited":
            # Retry network failures during join, not crashes or intentional exit.
            transient = "join:" in logs and any(message in logs for message in (
                "RPC timed out", "context deadline exceeded",
                "network is unreachable", "no route to host"))
            if not transient or attempts >= retries:
                raise RuntimeError(f"Node {name} stopped (exit {state['ExitCode']}): {logs}")
            attempts += 1
            print(f"Retrying join for {name} ({attempts}/{retries})", flush=True)
            time.sleep(min(2 ** attempts, 8))
            subprocess.run(["docker", "start", container], check=True,
                           text=True, capture_output=True)
        elif state["Status"] in ("dead", "removing"):
            raise RuntimeError(f"Node {name} is {state['Status']}: {logs}")
        else:
            time.sleep(1)
    raise RuntimeError(f"Timed out waiting for {container}: {snapshot(container)[1]}")


def start(project, count, batch_size=3, retries=3):
    if count < 2:
        raise ValueError("Use at least two nodes (one bootstrap and its peers)")
    if batch_size < 1 or retries < 0:
        raise ValueError("batch-size must be positive and startup-retries nonnegative")
    compose(project, "build", "bootstrap")
    existing = compose(project, "ps", "-a", "-q", "node",
                       capture_output=True).stdout.split()
    if len(existing) > count - 1:
        raise ValueError("Refusing to remove existing nodes; run down before reducing --nodes")
    compose(project, "up", "-d", "--no-build", "--pull", "never", "bootstrap")
    bootstrap = compose(project, "ps", "-q", "bootstrap", capture_output=True).stdout.strip()
    wait_node(bootstrap, retries=0)
    address = subprocess.check_output(
        ["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", bootstrap],
        text=True).strip()
    if not address:
        raise RuntimeError("Bootstrap container has no network address")
    environment = dict(os.environ, KAD_BOOTSTRAP=address + ":8000")
    # Recover existing peers first; never temporarily scale healthy nodes down.
    totals = ([len(existing) + 1] if existing else [])
    current = len(existing) + 1
    while current < count:
        current = min(current + batch_size, count)
        totals.append(current)
    for total in totals:
        compose(project, "up", "-d", "--no-build", "--pull", "never",
                "--no-recreate", "--scale", f"node={total - 1}", env=environment)
        wait_ready(project, total, retries=retries)


def wait_ready(project, count, retries=3):
    ids = compose(project, "ps", "-a", "-q", capture_output=True).stdout.split()
    if len(ids) != count:
        raise RuntimeError(f"Expected {count} containers, found {len(ids)}")
    # Join each restarted peer before retrying the next failed peer.
    for container in ids:
        wait_node(container, retries=retries)
    for container in ids:
        info, logs = snapshot(container)
        if not info["State"]["Running"] or not re.search(r"^Ready ", logs, re.MULTILINE):
            raise RuntimeError(f"Node {info['Name']} lost readiness: {logs}")
    print(f"Ready: {count} nodes in project {project}", flush=True)

def smoke():
    project = "kadlab-smoke-" + uuid.uuid4().hex[:8]
    try:
        start(project, 3)
        # A temporary publisher and a separate reader join over the Docker network.
        # The publisher exits before the reader begins, so local-only storage
        # cannot make this test pass.
        publisher = compose(project, "run", "--rm", "-T", "--no-deps",
            "--entrypoint", "sh", "node", "-c",
            "printf 'container-smoke-value' > /tmp/value; "
            "exec kadlab -listen auto:0 -bootstrap bootstrap:8000",
            input="ping bootstrap:8000\nput /tmp/value\nexit\n", capture_output=True).stdout
        match = re.search(r"Stored ([0-9a-f]{64})", publisher)
        if not match or "RTT=" not in publisher or "Error:" in publisher:
            raise RuntimeError("Publisher failed: " + publisher)
        key = match.group(1)
        reader = compose(project, "run", "--rm", "-T", "--no-deps",
            "node", "-listen", "auto:0", "-bootstrap", "bootstrap:8000",
            input=f"get {key}\nshow rt\nexit\n", capture_output=True).stdout
        if "container-smoke-value" not in reader or "Received from " not in reader or "Error:" in reader:
            raise RuntimeError("Reader failed: " + reader)
        print("PASS: container PING, STORE, retrieval after publisher exit, and routing display", flush=True)
    finally:
        compose(project, "down", "--remove-orphans")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["up", "down", "smoke"])
    parser.add_argument("--nodes", type=int, default=50, help="total nodes including bootstrap")
    parser.add_argument("--project", default="kadlab")
    parser.add_argument('--batch-size', type=int, default=3, help='maximum new peers per startup batch')
    parser.add_argument('--startup-retries', type=int, default=3, help='retries per node for transient join failures')
    args = parser.parse_args()
    if args.action == "up":
        start(args.project, args.nodes, args.batch_size, args.startup_retries)
    elif args.action == "down":
        compose(args.project, "down", "--remove-orphans")
    else:
        smoke()


if __name__ == "__main__":
    main()
