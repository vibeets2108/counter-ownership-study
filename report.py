"""Audit a completed storage study and regenerate summary.json and summary.csv."""

import argparse
from collections import Counter, defaultdict
from contextlib import closing
import csv
import hashlib
import json
from pathlib import Path
import sqlite3
import statistics

from storage_study import Settings, analyze, workload


METRICS = (
    "event_reads", "repeated_event_reads", "stable_visibility_p95_ms",
    "stable_visibility_max_ms", "oldest_missing_max_ms",
    "observer_max_gap_ms", "regressions", "rejected_writes",
    "snapshot_transactions", "writes", "lease_reads", "route_lease_queries",
    "lease_grant_transactions", "renewal_transactions", "renewed_rows",
    "fence_checks", "reconciliation_queries", "progress_checks", "superseded_writes",
)


def audit(root: Path) -> dict:
    results = json.loads((root / "results.json").read_text(encoding="utf-8"))
    if len({result["run"] for result in results}) != len(results):
        raise AssertionError("Duplicate run identifiers")
    environment = json.loads((root / "environment.json").read_text(encoding="utf-8"))
    source_hash = hashlib.sha256(
        Path(__file__).with_name("storage_study.py").read_bytes()
    ).hexdigest()
    if source_hash != environment["script_sha256"]:
        raise AssertionError("Study source does not match measured source")
    groups = defaultdict(list)
    fault_times = []
    polling_gaps = []
    prefixes = 0
    fingerprints = set()
    for result in results:
        directory = root / result["run"]
        raw = json.loads((directory / "result.json").read_text(encoding="utf-8"))
        if raw != {k: v for k, v in result.items() if k != "run"}:
            raise AssertionError("Per-run and combined results differ")
        trace = json.loads((directory / "trace.json").read_text(encoding="utf-8"))
        if not trace["successful"]:
            raise AssertionError("Incomplete run")
        computed = analyze(
            trace["samples"], trace["accepted"], result["settings"]["counters"]
        )
        if any(computed[key] != result["metrics"][key] for key in computed):
            raise AssertionError("Freshness metrics differ from raw samples")
        read_counts = Counter()
        for message in trace["messages"]:
            if message["type"] == "worker_metrics":
                read_counts.update({int(k): v for k, v in message["event_reads"].items()})
        if sum(read_counts.values()) != result["metrics"]["event_reads"]:
            raise AssertionError("Event-read accounting mismatch")
        if sum(max(0, n - 1) for n in read_counts.values()) != result["metrics"]["repeated_event_reads"]:
            raise AssertionError("Repeated-read accounting mismatch")
        with closing(sqlite3.connect(str(directory / "storage.sqlite"))) as db:
            if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise AssertionError("SQLite integrity failure")
            events = db.execute("SELECT id,counter FROM events ORDER BY id").fetchall()
            expected = [
                item for batch in workload(Settings(**result["settings"])) for item in batch
            ]
            if events != expected:
                raise AssertionError("Persisted input differs from configured input")
            fingerprints.add(hashlib.sha256(json.dumps(events).encode()).hexdigest())
            invalid = db.execute(
                "SELECT c.id FROM commits c WHERE c.total != "
                "(SELECT count(*) FROM events e WHERE e.counter=c.counter "
                "AND e.id<=c.checkpoint) LIMIT 1"
            ).fetchone()
            if invalid:
                raise AssertionError("A committed prefix is incorrect")
            finals = db.execute(
                "SELECT total,checkpoint,latest,accepted FROM rollups"
            ).fetchall()
            if any(total != accepted or checkpoint != latest
                   for total, checkpoint, latest, accepted in finals):
                raise AssertionError("Final totals are incomplete")
            commits = db.execute(
                "SELECT counter,checkpoint FROM commits ORDER BY id"
            ).fetchall()
            prefixes += len(commits)
            previous, regressions = {}, 0
            for counter, checkpoint in commits:
                regressions += checkpoint < previous.get(counter, 0)
                previous[counter] = checkpoint
            if regressions != result["metrics"].get("regressions", 0):
                raise AssertionError("Regression accounting mismatch")
        faults = [m for m in trace["messages"] if m["type"] == "fault"]
        if len(faults) != int(result["scenario"] != "normal"):
            raise AssertionError("Missing or unexpected fault")
        for fault in faults:
            if fault["counter"] != 0:
                raise AssertionError("Fault affected the wrong counter")
            fault_times.append(
                (fault["at_ns"] - trace["accepted"][0][2]) / 1_000_000
            )
        end_input = trace["accepted"][-1][2]
        for a, b in zip(trace["samples"], trace["samples"][1:]):
            if a["at_ns"] <= end_input:
                polling_gaps.append((b["at_ns"] - a["at_ns"]) / 1_000_000)
        groups[(result["scenario"], result["routing"], result["strategy"])].append(result)
    expected_groups = {
        (scenario, routing, strategy)
        for scenario in ("normal", "crash", "pause")
        for routing in ("sticky", "round_robin")
        for strategy in ("overlap", "owned", "guarded")
    }
    if set(groups) != expected_groups or len({len(rs) for rs in groups.values()}) != 1:
        raise AssertionError("Incomplete or unbalanced factorial comparison")
    if len(fingerprints) != 1:
        raise AssertionError("Trials used different input")
    rows = []
    for (scenario, routing, strategy), records in sorted(groups.items()):
        rows.append({
            "scenario": scenario, "routing": routing, "strategy": strategy,
            "runs": len(records),
            "metrics": {
                metric: {
                    "median": statistics.median(r["metrics"].get(metric, 0) for r in records),
                    "min": min(r["metrics"].get(metric, 0) for r in records),
                    "max": max(r["metrics"].get(metric, 0) for r in records),
                } for metric in METRICS
            },
        })
    return {
        "runs": len(results), "groups": rows, "verified_committed_prefixes": prefixes,
        "identical_workload_sha256": next(iter(fingerprints)),
        "fault_since_first_ack_ms": {"min": min(fault_times), "max": max(fault_times)},
        "active_polling_gap_ms": {
            "median": statistics.median(polling_gaps), "max": max(polling_gaps)
        },
        "environment": environment,
        "aggregation": "Each cell is median/min/max across independent runs in its group, not pooled percentiles",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    summary = audit(args.directory)
    (args.directory / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    with (args.directory / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["scenario", "routing", "strategy", "runs"] + [
            f"{metric}_{stat}" for metric in METRICS for stat in ("median", "min", "max")
        ])
        for row in summary["groups"]:
            writer.writerow(
                [row[key] for key in ("scenario", "routing", "strategy", "runs")] + [
                    row["metrics"][metric][stat]
                    for metric in METRICS for stat in ("median", "min", "max")
                ]
            )
    print(f"Audited {summary['runs']} trials and {summary['verified_committed_prefixes']} committed prefixes.")
    for row in summary["groups"]:
        metrics = row["metrics"]
        delay = metrics["stable_visibility_p95_ms"]
        print(
            row["scenario"], row["routing"], row["strategy"],
            f"reads={metrics['event_reads']['median']}",
            f"p95 median={delay['median']:.1f} [{delay['min']:.1f},{delay['max']:.1f}]ms",
            f"regressions={metrics['regressions']['min']}..{metrics['regressions']['max']}",
        )


if __name__ == "__main__":
    main()
