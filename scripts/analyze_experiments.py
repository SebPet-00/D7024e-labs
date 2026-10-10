#!/usr/bin/env python3
"""Validate lookup JSONL and calculate statistics across independent seed runs."""
import argparse
import csv
import json
import math
import pathlib
import statistics
from collections import defaultdict


def read_run(path):
    metadata = None
    active = {}
    completed = None
    rows = []
    for line in path.read_text().splitlines():
        event = json.loads(line)
        kind = event["event"]
        if kind == "run_metadata":
            if metadata is not None:
                raise ValueError(f"{path}: duplicate metadata")
            metadata = event
        elif kind == "lookup_start":
            identity = event["lookup_id"]
            if active or completed is not None:
                raise ValueError(f"{path}: experiment must have one foreground lookup per trial")
            active[identity] = {"start": event, "requests": defaultdict(list)}
        elif kind == "probe":
            active[event["lookup_id"]]["requests"][event["request_id"]].append(event["attempt"])
        elif kind == "lookup_end":
            state = active.pop(event["lookup_id"])
            requests = state["requests"]
            for attempts in requests.values():
                if attempts != list(range(1, len(attempts) + 1)):
                    raise ValueError(f"{path}: retry attempt sequence is inconsistent")
            if event["probes"] != len(requests) or event["attempts"] != sum(map(len, requests.values())):
                raise ValueError(f"{path}: summary counts disagree with probe records")
            completed = event
        elif kind == "trial_result":
            if completed is None or active:
                raise ValueError(f"{path}: missing lookup outcome")
            if event["source"] != completed["node"] or event["target"] != completed["target"]:
                raise ValueError(f"{path}: result does not match the completed lookup")
            if event["correct"] and not completed["success"]:
                raise ValueError(f"{path}: correctness contradicts protocol failure")
            rows.append({
                "experiment": metadata["experiment"], "n": metadata["n"],
                "loss": metadata["loss"], "seed": metadata["seed"],
                "trial": event["trial"], "lookup_id": completed["lookup_id"],
                "correct": int(event["correct"]), "api_success": int(completed["success"]),
                "probes": completed["probes"], "attempts": completed["attempts"],
                "rounds": completed["rounds"], "error": event.get("error", ""),
            })
            completed = None
        else:
            raise ValueError(f"{path}: unknown event {kind}")
    if metadata is None or active or completed is not None or len(rows) != metadata["queries"]:
        raise ValueError(f"{path}: incomplete run; refusing to analyze partial data")
    return rows


def save_csv(path, rows):
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def summarize(rows):
    runs = defaultdict(list)
    for row in rows:
        runs[(row["experiment"], row["n"], row["loss"], row["seed"])].append(row)
    run_rows = []
    for (experiment, n, loss, seed), samples in sorted(runs.items()):
        row = {"experiment": experiment, "n": n, "loss": loss, "seed": seed, "queries": len(samples)}
        for metric in ["correct", "api_success", "probes", "attempts", "rounds"]:
            row[metric] = statistics.mean(sample[metric] for sample in samples)
        run_rows.append(row)
    groups = defaultdict(list)
    for row in run_rows:
        groups[(row["experiment"], row["n"], row["loss"])].append(row)
    summaries = []
    for (experiment, n, loss), samples in sorted(groups.items()):
        if len(samples) < 2:
            raise ValueError("At least two seed runs per condition are required for variance")
        row = {"experiment": experiment, "n": n, "loss": loss, "runs": len(samples),
               "queries": sum(sample["queries"] for sample in samples)}
        for metric in ["correct", "api_success", "probes", "attempts", "rounds"]:
            values = [sample[metric] for sample in samples]
            row[metric + "_mean"] = statistics.mean(values)
            row[metric + "_variance"] = statistics.variance(values)
        summaries.append(row)
    return run_rows, summaries


def figures(summary, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False})
    scale = sorted((row for row in summary if row["experiment"] == "scale"), key=lambda row: row["n"])
    loss = sorted((row for row in summary if row["experiment"] == "loss"), key=lambda row: row["loss"])

    def series(ax, rows, xname, metric, label, color):
        ax.errorbar([row[xname] for row in rows],
                    [row[metric + "_mean"] for row in rows],
                    yerr=[math.sqrt(row[metric + "_variance"]) for row in rows],
                    fmt="o-", capsize=4, label=label, color=color)
        ax.grid(alpha=0.2)

    if scale:
        fig, axes = plt.subplots(1, 2, figsize=(11, 4.3), layout="constrained")
        for ax, metric, label in zip(axes, ["probes", "rounds"], ["Logical probes", "Parallel rounds"]):
            series(ax, scale, "n", metric, label, "#176b9b")
            xs = [row["n"] for row in scale]
            baseline = scale[0][metric + "_mean"]
            ax.plot(xs, [baseline * math.log2(x) / math.log2(xs[0]) for x in xs],
                    "--", color="#a06c25", label="log2(N), anchored at first mean")
            ax.set_xscale("log", base=2)
            ax.set_xticks(xs, [str(x) for x in xs])
            ax.tick_params(axis="x", labelrotation=30)
            ax.set_xlabel("Number of nodes N")
            ax.set_ylabel(label + " per node lookup")
            ax.legend(fontsize=8)
        fig.suptitle("Lookup scalability: full joins, K=10, alpha=3; bars = SD of seed means")
        for suffix in ["png", "svg", "pdf"]:
            fig.savefig(output / ("scale." + suffix), dpi=180)
        plt.close(fig)
    if loss:
        fig, axes = plt.subplots(1, 2, figsize=(11, 4.3), layout="constrained")
        series(axes[0], loss, "loss", "correct", "Verified value retrieval", "#176b9b")
        axes[0].set_ylim(-0.03, 1.08)
        axes[0].set_ylabel("Lookup success fraction")
        series(axes[1], loss, "loss", "probes", "Logical probes", "#176b9b")
        series(axes[1], loss, "loss", "attempts", "Attempts including retries", "#a06c25")
        axes[1].set_ylabel("Mean per lookup, including failures")
        axes[1].legend(fontsize=8)
        for ax in axes:
            ax.set_xlabel("Independent packet-loss probability")
            # Dense 0.50--0.70 measurements should not create overlapping labels.
            ax.set_xticks([0, 0.1, 0.3, 0.5, 0.6, 0.7, 0.9])
            ax.set_xticks([row["loss"] for row in loss], minor=True)
        fig.suptitle("Value lookup reliability: N=100, 3 RPC attempts; bars = SD of seed means")
        for suffix in ["png", "svg", "pdf"]:
            fig.savefig(output / ("loss." + suffix), dpi=180)
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw", type=pathlib.Path)
    parser.add_argument("--out", type=pathlib.Path, default=pathlib.Path("results/analysis"))
    args = parser.parse_args()
    paths = sorted(args.raw.glob("*.jsonl"))
    if not paths:
        raise ValueError("No raw JSONL files found")
    rows = [row for path in paths for row in read_run(path)]
    runs, summary = summarize(rows)
    args.out.mkdir(parents=True, exist_ok=True)
    save_csv(args.out / "lookups.csv", rows)
    save_csv(args.out / "runs.csv", runs)
    save_csv(args.out / "summary.csv", summary)
    lines = [
        "| Experiment | N | Loss | Runs | Queries | Success mean | Success variance | Probes mean | Probes variance | Attempts mean | Attempts variance | Rounds mean | Rounds variance |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary:
        lines.append(
            f"| {row['experiment']} | {row['n']} | {row['loss']:.2f} | {row['runs']} | {row['queries']} | "
            f"{row['correct_mean']:.3f} | {row['correct_variance']:.6f} | "
            f"{row['probes_mean']:.3f} | {row['probes_variance']:.6f} | "
            f"{row['attempts_mean']:.3f} | {row['attempts_variance']:.6f} | "
            f"{row['rounds_mean']:.3f} | {row['rounds_variance']:.6f} |")
    (args.out / "tables.md").write_text("\n".join(lines) + "\n")
    figures(summary, args.out)
    print(f"Validated {len(paths)} runs and {len(rows)} lookups; wrote {args.out}")


if __name__ == "__main__":
    main()
