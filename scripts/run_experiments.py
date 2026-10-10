#!/usr/bin/env python3
"""Run both mandatory report experiments and generate validated tables/figures."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shlex
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def run_pipeline(output, queries=20, dry_run=False):
    # Relative output paths are consistently relative to the repository.
    output = (ROOT / output).resolve()
    commands = [
        ["go", "run", "./cmd/experiments", "-out", str(output / "raw"),
         "-queries", str(queries)],
        [sys.executable, str(ROOT / "scripts/analyze_experiments.py"),
         str(output / "raw"), "--out", str(output / "analysis")],
    ]
    print(f"Output: {output}", flush=True)
    print("Uses the matrix in experiments.DefaultSettings; "
          f"{queries} lookups per seed/condition. "
          "K=10, alpha=3, 100 ms RPC timeout, 2 retries, 2 s lookup deadline.", flush=True)
    for command in commands:
        print(shlex.join(command), flush=True)
    if dry_run:
        return output
    if output.exists():
        raise ValueError(f"Output already exists: {output}. Choose a fresh directory.")
    if shutil.which("go") is None:
        raise ValueError("Go must be installed and on PATH (use WSL for this repository).")
    # Fail before the expensive workload if this interpreter cannot draw figures.
    try:
        subprocess.run([sys.executable, "-c", "import matplotlib.pyplot"],
                       cwd=ROOT, check=True)
    except subprocess.CalledProcessError as exc:
        raise ValueError("Plotting dependencies are unavailable. Install "
                         "scripts/requirements-analysis.txt into this Python environment.") from exc

    settings = json.loads(subprocess.check_output(
        [*commands[0], "-describe"], cwd=ROOT, text=True))
    runs = len(settings["Seeds"]) * (len(settings["Sizes"]) + len(settings["Losses"]))
    print(f"{runs} seed/condition runs, {runs * queries} measured lookups; "
          f"sizes={settings['Sizes']}, losses={settings['Losses']}, "
          f"seeds={settings['Seeds']}", flush=True)

    def git_output(*args):
        try:
            return subprocess.check_output(["git", *args], cwd=ROOT,
                                           text=True, stderr=subprocess.DEVNULL).strip()
        except (OSError, subprocess.CalledProcessError):
            return None

    output.mkdir(parents=True, exist_ok=False)
    manifest = {
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "status": "running", "queries_per_condition_and_seed": queries,
        "git_commit": git_output("rev-parse", "HEAD"),
        "git_status": git_output("status", "--porcelain"),
        "python_version": sys.version, "commands": commands,
        "settings": settings,
    }
    manifest_path = output / "manifest.json"

    def save_manifest():
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    save_manifest()
    try:
        for command in commands:
            subprocess.run(command, cwd=ROOT, check=True)
    except (OSError, subprocess.CalledProcessError, KeyboardInterrupt) as exc:
        manifest.update(status="incomplete", error=str(exc))
        raise
    else:
        manifest["status"] = "complete"
    finally:
        manifest["finished_utc"] = datetime.now(timezone.utc).isoformat()
        save_manifest()
    print(f"Report tables: {output / 'analysis/tables.md'}", flush=True)
    print(f"Figures and CSV data: {output / 'analysis'}", flush=True)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path("results") /
                        datetime.now(timezone.utc).strftime("report-%Y%m%dT%H%M%S%fZ"),
                        help="new output directory (relative paths use repository root)")
    parser.add_argument("--queries", type=positive_int, default=20,
                        help="lookups per seed/condition (report default: 20)")
    parser.add_argument("--dry-run", action="store_true",
                        help="print commands without running or creating files")
    args = parser.parse_args()
    try:
        run_pipeline(args.out, args.queries, args.dry_run)
    except KeyboardInterrupt:
        print("Interrupted; partial output retained. Rerun in a fresh directory.", file=sys.stderr)
        return 130
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"Experiment pipeline failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
