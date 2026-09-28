#!/usr/bin/env python3
"""Start/stop a local Compose lab or run an isolated container smoke test."""
import argparse
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


def start(project, count):
    if count < 2:
        raise ValueError("Use at least two nodes (one bootstrap and its peers)")
    compose(project, "build", "bootstrap")
    existing = compose(project, "ps", "-a", "-q", capture_output=True).stdout.split()
    compose(project, "up", "-d", "--no-build", "--pull", "never", "bootstrap")
    bootstrap = compose(project, "ps", "-q", "bootstrap", capture_output=True).stdout.strip()
    address = subprocess.check_output(
        ["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", bootstrap],
        text=True).strip()
    if not address:
        raise RuntimeError("Bootstrap container has no network address")
    # Discover the assigned IP rather than depending on Docker DNS at scale.
    environment = dict(os.environ, KAD_BOOTSTRAP=address + ":8000")
    # Keep join traffic bounded. Never temporarily scale an existing lab down.
    stages = [count] if existing else [*range(11, count, 10), count]
    for total in stages:
        compose(project, "up", "-d", "--no-build", "--pull", "never",
                "--scale", f"node={total - 1}", env=environment)
        wait_ready(project, total)


def wait_ready(project, count):
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        ids = compose(project, "ps", "-a", "-q", capture_output=True).stdout.split()
        if len(ids) == count:
            ready = True
            for container in ids:
                state = subprocess.check_output(
                    ["docker", "inspect", "-f", "{{.State.Running}}", container], text=True).strip()
                logs = subprocess.check_output(["docker", "logs", container], text=True)
                if state != "true":
                    raise RuntimeError(f"Node {container} stopped during startup: {logs}")
                if not re.search(r"^Ready ", logs, re.MULTILINE):
                    ready = False
                    break
            if ready:
                print(f"Ready: {count} nodes in project {project}", flush=True)
                return
        time.sleep(1)
    compose(project, "logs", "--tail", "20")
    raise RuntimeError("Timed out waiting for every node to join")


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
    args = parser.parse_args()
    if args.action == "up":
        start(args.project, args.nodes)
    elif args.action == "down":
        compose(args.project, "down", "--remove-orphans")
    else:
        smoke()


if __name__ == "__main__":
    main()
