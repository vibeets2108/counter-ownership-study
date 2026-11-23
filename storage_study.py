"""Real SQLite / spawned-process study. See README.md for scope and reproduction."""

from __future__ import annotations

import argparse
import bisect
from collections import Counter
from dataclasses import asdict, dataclass
import hashlib
import json
import multiprocessing as mp
import os
from pathlib import Path
import platform
import queue
import random
import sqlite3
import statistics
import sys
import time
import traceback


@dataclass(frozen=True)
class Settings:
    counters: int = 8
    batches: int = 60
    batch_size: int = 8
    batch_ms: int = 20
    rollup_ms: int = 40
    reconcile_ms: int = 40
    lease_ms: int = 250
    renew_ms: int = 50
    fault_after_events: int = 160
    pause_ms: int = 600
    observe_ms: int = 5
    settle_ms: int = 1200
    seed: int = 42


def connect(path: str | Path) -> sqlite3.Connection:
    db = sqlite3.connect(str(path), timeout=10, isolation_level=None)
    db.execute("PRAGMA busy_timeout=10000")
    db.execute("PRAGMA synchronous=FULL")
    return db


def initialize(path: Path, counters: int) -> None:
    db = connect(path)
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript("""
        CREATE TABLE events (
            id INTEGER PRIMARY KEY, counter INTEGER NOT NULL
        );
        CREATE INDEX events_by_counter ON events(counter, id);
        CREATE TABLE rollups (
            counter INTEGER PRIMARY KEY, total INTEGER NOT NULL DEFAULT 0,
            checkpoint INTEGER NOT NULL DEFAULT 0,
            latest INTEGER NOT NULL DEFAULT 0,
            accepted INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE leases (
            counter INTEGER PRIMARY KEY, owner INTEGER NOT NULL,
            epoch INTEGER NOT NULL, expires INTEGER NOT NULL
        );
        CREATE TABLE commits (
            id INTEGER PRIMARY KEY, counter INTEGER NOT NULL,
            total INTEGER NOT NULL, checkpoint INTEGER NOT NULL,
            worker INTEGER NOT NULL, epoch INTEGER
        );
    """)
    db.executemany(
        "INSERT INTO rollups(counter) VALUES (?)",
        [(counter,) for counter in range(counters)],
    )
    db.close()


def ingest(db: sqlite3.Connection, events: list[tuple[int, int]]) -> set[int]:
    changed = set()
    db.execute("BEGIN IMMEDIATE")
    try:
        for identity, counter in events:
            inserted = db.execute(
                "INSERT OR IGNORE INTO events(id,counter) VALUES (?,?)",
                (identity, counter),
            ).rowcount
            if inserted:
                db.execute(
                    "UPDATE rollups SET latest=max(latest,?),accepted=accepted+1 "
                    "WHERE counter=?", (identity, counter),
                )
                changed.add(counter)
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise
    return changed


def lease(
    db: sqlite3.Connection, counter: int, worker: int, ttl_ns: int, metrics: Counter
) -> int | None:
    metrics["lease_reads"] += 1
    row = db.execute(
        "SELECT owner,epoch,expires FROM leases WHERE counter=?", (counter,)
    ).fetchone()
    now = time.perf_counter_ns()
    if row and row[2] > now:
        return row[1] if row[0] == worker else None
    metrics["lease_grant_transactions"] += 1
    db.execute("BEGIN IMMEDIATE")
    try:
        row = db.execute(
            "SELECT owner,epoch,expires FROM leases WHERE counter=?", (counter,)
        ).fetchone()
        now = time.perf_counter_ns()
        if row and row[2] > now:
            db.execute("COMMIT")
            return row[1] if row[0] == worker else None
        epoch = row[1] + 1 if row else 1
        db.execute(
            "INSERT INTO leases VALUES (?,?,?,?) ON CONFLICT(counter) DO UPDATE "
            "SET owner=excluded.owner,epoch=excluded.epoch,expires=excluded.expires",
            (counter, worker, epoch, now + ttl_ns),
        )
        db.execute("COMMIT")
        metrics["lease_grants"] += 1
        return epoch
    except BaseException:
        db.execute("ROLLBACK")
        raise


def snapshot(db: sqlite3.Connection, counter: int) -> tuple[int, int, list[int]]:
    db.execute("BEGIN")
    try:
        total, checkpoint, latest = db.execute(
            "SELECT total,checkpoint,latest FROM rollups WHERE counter=?", (counter,)
        ).fetchone()
        identities = [
            row[0] for row in db.execute(
                "SELECT id FROM events WHERE counter=? AND id>? AND id<=? ORDER BY id",
                (counter, checkpoint, latest),
            )
        ]
        db.execute("COMMIT")
        return total + len(identities), latest, identities
    except BaseException:
        db.execute("ROLLBACK")
        raise


def save(
    db: sqlite3.Connection, counter: int, total: int, checkpoint: int,
    worker: int, epoch: int | None, metrics: Counter, guard_progress: bool = False,
) -> bool:
    metrics["write_attempts"] += 1
    db.execute("BEGIN IMMEDIATE")
    try:
        if epoch is not None:
            metrics["fence_checks"] += 1
            valid = db.execute(
                "SELECT 1 FROM leases WHERE counter=? AND owner=? AND epoch=? "
                "AND expires>?", (counter, worker, epoch, time.perf_counter_ns()),
            ).fetchone()
            if not valid:
                metrics["rejected_writes"] += 1
                db.execute("ROLLBACK")
                return False
        previous = db.execute(
            "SELECT checkpoint FROM rollups WHERE counter=?", (counter,)
        ).fetchone()[0]
        if guard_progress:
            metrics["progress_checks"] += 1
            if checkpoint < previous:
                metrics["superseded_writes"] += 1
                db.execute("ROLLBACK")
                return False
        metrics["regressions"] += int(checkpoint < previous)
        db.execute(
            "UPDATE rollups SET total=?,checkpoint=? WHERE counter=?",
            (total, checkpoint, counter),
        )
        db.execute(
            "INSERT INTO commits(counter,total,checkpoint,worker,epoch) "
            "VALUES (?,?,?,?,?)", (counter, total, checkpoint, worker, epoch),
        )
        db.execute("COMMIT")
        metrics["writes"] += 1
        return True
    except BaseException:
        db.execute("ROLLBACK")
        raise


def worker_main(
    index, path, strategy, settings, inbox, output, ready, start, stop, inject
) -> None:
    metrics = Counter()
    event_reads = Counter()
    db = connect(path)
    ready.put(index)
    start.wait()
    pending = set()
    next_rollup = time.perf_counter() + settings.rollup_ms / 1000
    next_renew = time.perf_counter()
    try:
        while not stop.is_set():
            now = time.perf_counter()
            if strategy == "owned" and now >= next_renew:
                metrics["renewal_transactions"] += 1
                instant = time.perf_counter_ns()
                metrics["renewed_rows"] += db.execute(
                    "UPDATE leases SET expires=? WHERE owner=? AND expires>?",
                    (instant + settings.lease_ms * 1_000_000, index, instant),
                ).rowcount
                next_renew = time.perf_counter() + settings.renew_ms / 1000
            for _ in range(2048):
                try:
                    pending.add(inbox.get_nowait())
                except queue.Empty:
                    break
            if now < next_rollup or not pending:
                time.sleep(0.001)
                continue
            work, pending = sorted(pending), set()
            for counter in work:
                epoch = None
                if strategy == "owned":
                    epoch = lease(
                        db, counter, index, settings.lease_ms * 1_000_000, metrics
                    )
                    if epoch is None:
                        continue
                metrics["snapshot_transactions"] += 1
                total, checkpoint, identities = snapshot(db, counter)
                event_reads.update(identities)
                if not identities:
                    continue
                if counter == 0 and inject.is_set():
                    inject.clear()
                    output.put({
                        "type": "fault", "worker": index, "at_ns": time.perf_counter_ns(),
                        "counter": counter, "checkpoint": checkpoint,
                    })
                    if settings.scenario == "crash":
                        raise SystemExit(73)
                    time.sleep(settings.pause_ms / 1000)
                    output.put({
                        "type": "resume", "worker": index,
                        "at_ns": time.perf_counter_ns(),
                    })
                began = time.perf_counter_ns()
                save(
                    db, counter, total, checkpoint, index, epoch, metrics,
                    guard_progress=strategy == "guarded",
                )
                metrics["commit_path_ns"] += time.perf_counter_ns() - began
            next_rollup = time.perf_counter() + settings.rollup_ms / 1000
    except Exception:
        output.put({"type": "error", "worker": index, "error": traceback.format_exc()})
        raise
    finally:
        db.close()
        output.put({
            "type": "worker_metrics", "worker": index,
            "metrics": dict(metrics), "event_reads": dict(event_reads),
        })
        output.close()
        output.join_thread()


def observer_main(path, interval_ms, ready, start, stop, output):
    db = connect(path)
    ready.put("observer")
    start.wait()
    samples = []
    try:
        while not stop.is_set():
            rows = db.execute(
                "SELECT checkpoint FROM rollups ORDER BY counter"
            ).fetchall()
            samples.append({
                "at_ns": time.perf_counter_ns(),
                "checkpoints": [row[0] for row in rows],
            })
            time.sleep(interval_ms / 1000)
    except Exception:
        output.put({"type": "error", "worker": "observer", "error": traceback.format_exc()})
        raise
    finally:
        db.close()
        output.put({"type": "observations", "samples": samples})
        output.close()
        output.join_thread()


def percentile(values: list[float], percent: float) -> float:
    if not values:
        raise ValueError("No observations")
    ordered = sorted(values)
    return ordered[max(0, int((len(ordered) - 1) * percent))]


def collect_children(processes, output, timeout=15):
    messages = []
    deadline = time.perf_counter() + timeout
    # Queue producers cannot exit until their buffered reports have been drained.
    while any(process.is_alive() for process in processes):
        if time.perf_counter() >= deadline:
            raise AssertionError("Child process failed to stop")
        try:
            messages.append(output.get(timeout=0.05))
        except queue.Empty:
            pass
    for process in processes:
        process.join()
    while True:
        try:
            messages.append(output.get(timeout=0.1))
        except queue.Empty:
            return messages


def workload(settings: Settings) -> list[list[tuple[int, int]]]:
    rng = random.Random(settings.seed)
    batches = []
    for batch in range(settings.batches):
        items = []
        for item in range(settings.batch_size):
            counter = 0 if rng.random() < 0.75 else rng.randrange(settings.counters)
            items.append((batch * settings.batch_size + item + 1, counter))
        batches.append(items)
    return batches


def analyze(samples, accepted, counters) -> dict:
    delays = []
    for identity, counter, at_ns in accepted:
        relevant = [s for s in samples if s["at_ns"] >= at_ns]
        last_missing = -1
        for index, sample in enumerate(relevant):
            if sample["checkpoints"][counter] < identity:
                last_missing = index
        if last_missing + 1 >= len(relevant):
            raise AssertionError(f"Event {identity} never became stably visible")
        visible = relevant[last_missing + 1]["at_ns"]
        delays.append((visible - at_ns) / 1_000_000)
    by_counter = [[] for _ in range(counters)]
    times = {}
    for identity, counter, instant in accepted:
        by_counter[counter].append(identity)
        times[identity] = instant
    oldest_missing_ms = 0.0
    for sample in samples:
        for counter in range(counters):
            identities = by_counter[counter]
            position = bisect.bisect_right(identities, sample["checkpoints"][counter])
            if position < len(identities):
                instant = times[identities[position]]
                if instant <= sample["at_ns"]:
                    oldest_missing_ms = max(
                        oldest_missing_ms, (sample["at_ns"] - instant) / 1_000_000
                    )
    return {
        "stable_visibility_p50_ms": statistics.median(delays),
        "stable_visibility_p95_ms": percentile(delays, 0.95),
        "stable_visibility_max_ms": max(delays),
        "oldest_missing_max_ms": oldest_missing_ms,
        "observer_max_gap_ms": max(
            (b["at_ns"] - a["at_ns"]) / 1_000_000
            for a, b in zip(samples, samples[1:])
        ),
    }


def run_one(directory: Path, strategy: str, routing: str, scenario: str,
            settings: Settings) -> dict:
    directory.mkdir()
    path = directory / "storage.sqlite"
    initialize(path, settings.counters)
    ctx = mp.get_context("spawn")
    inboxes = [ctx.Queue() for _ in range(2)]
    output, ready = ctx.Queue(), ctx.Queue()
    start, stop = ctx.Event(), ctx.Event()
    injections = [ctx.Event(), ctx.Event()]
    # Keep scenario separate from the immutable workload settings in the report.
    from types import SimpleNamespace
    worker_settings = SimpleNamespace(**asdict(settings), scenario=scenario)
    workers = [
        ctx.Process(
            target=worker_main,
            args=(i, str(path), strategy, worker_settings, inboxes[i], output,
                  ready, start, stop, injections[i]),
        ) for i in range(2)
    ]
    observer = ctx.Process(
        target=observer_main,
        args=(str(path), settings.observe_ms, ready, start, stop, output),
    )
    processes = workers + [observer]
    messages, accepted, samples = [], [], []
    controller = Counter()
    batches = workload(settings)
    db = connect(path)
    successful = False
    try:
        for process in processes:
            process.start()
        for _ in processes:
            ready.get(timeout=20)
        began = time.perf_counter_ns()
        start.set()
        cursor, batch_index = 0, 0
        next_reconcile = 0.0
        requested = False
        end_ms = settings.batches * settings.batch_ms + settings.settle_ms

        def dispatch(counters):
            nonlocal cursor
            living = [i for i, process in enumerate(workers) if process.is_alive()]
            if not living:
                raise AssertionError("Both workers stopped")
            leases = {}
            now_ns = time.perf_counter_ns()
            if strategy == "owned":
                controller["route_lease_queries"] += 1
                leases = {
                    row[0]: row[1:] for row in db.execute(
                        "SELECT counter,owner,epoch,expires FROM leases"
                    )
                }
            for counter in sorted(counters):
                target = (
                    living[counter % len(living)] if routing == "sticky"
                    else living[cursor % len(living)]
                )
                cursor += 1
                row = leases.get(counter)
                if row:
                    owner, _, expires = row
                    if expires > now_ns:
                        target = owner
                    elif target == owner and len(living) > 1:
                        target = next(i for i in living if i != owner)
                inboxes[target].put(counter)
                controller["reminders"] += 1

        while True:
            elapsed = (time.perf_counter_ns() - began) / 1_000_000
            if elapsed >= end_ms:
                break
            if (
                scenario != "normal" and not requested
                and len(accepted) >= settings.fault_after_events
            ):
                target = 0
                if strategy == "owned":
                    current = db.execute(
                        "SELECT owner FROM leases WHERE counter=0"
                    ).fetchone()
                    if current is None:
                        continue
                    target = current[0]
                injections[target].set()
                requested = True
            changed = set()
            while batch_index < len(batches) and elapsed >= batch_index * settings.batch_ms:
                items = batches[batch_index]
                changed.update(ingest(db, items))
                acknowledged = time.perf_counter_ns()
                accepted.extend((identity, counter, acknowledged) for identity, counter in items)
                controller["ingest_transactions"] += 1
                batch_index += 1
            if changed:
                dispatch(changed)
            if elapsed >= next_reconcile:
                dirty = {
                    row[0] for row in db.execute(
                        "SELECT counter FROM rollups WHERE checkpoint<latest"
                    )
                }
                controller["reconciliation_queries"] += 1
                if dirty:
                    dispatch(dirty)
                next_reconcile = elapsed + settings.reconcile_ms
            while True:
                try:
                    messages.append(output.get_nowait())
                except queue.Empty:
                    break
            if any(message["type"] == "error" for message in messages):
                raise AssertionError("Worker error; inspect the saved trace")
            time.sleep(0.001)
        stop.set()
        messages.extend(collect_children(processes, output))
        observation_reports = [m for m in messages if m["type"] == "observations"]
        if len(observation_reports) != 1 or observer.exitcode != 0:
            raise AssertionError("Independent observer did not complete")
        samples = observation_reports[0]["samples"]
        messages = [m for m in messages if m["type"] != "observations"]
        final = db.execute(
            "SELECT counter,total,checkpoint,latest,accepted FROM rollups ORDER BY counter"
        ).fetchall()
        samples.append({
            "at_ns": time.perf_counter_ns(),
            "checkpoints": [row[2] for row in final],
        })
        invalid = db.execute(
            "SELECT c.id FROM commits c WHERE c.total != "
            "(SELECT count(*) FROM events e WHERE e.counter=c.counter "
            "AND e.id<=c.checkpoint) LIMIT 1"
        ).fetchone()
        if invalid:
            raise AssertionError(f"Incorrect committed prefix: {invalid}")
        if batch_index != settings.batches or any(
            total != count or checkpoint != latest
            for _, total, checkpoint, latest, count in final
        ):
            raise AssertionError("Inputs incomplete or final count did not converge")
        faults = [message for message in messages if message["type"] == "fault"]
        if len(faults) != int(scenario != "normal"):
            raise AssertionError("Fault scenario did not exercise its required fault")
        if any(fault["counter"] != 0 for fault in faults):
            raise AssertionError("Fault did not target the controlled hot counter")
        exits = [process.exitcode for process in workers]
        expected_exits = [0, 0]
        if scenario == "crash":
            expected_exits[faults[0]["worker"]] = 73
        if exits != expected_exits:
            raise AssertionError(f"Unexpected worker exits: {exits}")
        worker_reports = [m for m in messages if m["type"] == "worker_metrics"]
        if len(worker_reports) != 2:
            raise AssertionError("Incomplete operation accounting")
        metrics, reads = Counter(), Counter()
        for report in worker_reports:
            metrics.update(report["metrics"])
            reads.update({int(k): v for k, v in report["event_reads"].items()})
        if strategy == "owned" and scenario == "pause" and not metrics["rejected_writes"]:
            raise AssertionError("Pause case did not exercise fencing")
        if strategy in ("owned", "guarded") and metrics["regressions"]:
            raise AssertionError("Protected strategy regressed")
        if (
            strategy == "guarded" and routing == "round_robin"
            and scenario == "pause" and not metrics["superseded_writes"]
        ):
            raise AssertionError("Pause case did not exercise progress protection")
        result = {
            "strategy": strategy, "routing": routing, "scenario": scenario,
            "settings": asdict(settings),
            "worker_pids": [process.pid for process in workers],
            "observer_pid": observer.pid,
            "worker_exits": exits,
            "accepted_events": len(accepted),
            "input_span_ms": (accepted[-1][2] - accepted[0][2]) / 1_000_000,
            "metrics": {
                **dict(metrics), **dict(controller),
                "event_reads": sum(reads.values()),
                "repeated_event_reads": sum(max(0, n - 1) for n in reads.values()),
                **analyze(samples, accepted, settings.counters),
            },
            "final": final,
            "correctness": "All committed prefixes match event log; final totals converge",
        }
        (directory / "result.json").write_text(
            json.dumps(result, indent=2) + "\n", encoding="utf-8"
        )
        successful = True
        return result
    finally:
        stop.set()
        for process in processes:
            if process.pid is not None:
                process.join(timeout=3)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
        db.close()
        (directory / "trace.json").write_text(
            json.dumps({
                "successful": successful, "accepted": accepted,
                "samples": samples, "messages": messages,
            }, indent=2) + "\n", encoding="utf-8",
        )
        for channel in inboxes:
            channel.cancel_join_thread()
            channel.close()
        output.close()
        ready.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    args.output.mkdir(parents=True, exist_ok=False)
    environment = {
        "python": sys.version, "sqlite": sqlite3.sqlite_version,
        "platform": platform.platform(), "logical_cpus": os.cpu_count(),
        "start_method": "spawn", "journal_mode": "WAL", "synchronous": "FULL",
        "clock": "perf_counter_ns; shared high-resolution clock across Windows processes",
        "observer": "Separate spawned read-only sampling process",
        "scope": "Local shared SQLite, not a distributed database or throughput benchmark",
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    (args.output / "environment.json").write_text(
        json.dumps(environment, indent=2) + "\n", encoding="utf-8"
    )
    configs = [
        (strategy, routing, scenario)
        for scenario in ("normal", "crash", "pause")
        for routing in ("sticky", "round_robin")
        for strategy in ("overlap", "owned", "guarded")
    ]
    if args.smoke:
        configs = [("overlap", "round_robin", "normal"),
                   ("owned", "sticky", "pause"), ("owned", "sticky", "crash"),
                   ("guarded", "round_robin", "pause")]
    records = []
    for repeat in range(args.repeats):
        order = configs.copy()
        random.Random(100 + repeat).shuffle(order)
        for strategy, routing, scenario in order:
            label = f"r{repeat + 1}-{scenario}-{routing}-{strategy}"
            result = run_one(args.output / label, strategy, routing, scenario, Settings())
            records.append({"run": label, **result})
            (args.output / "results.json").write_text(
                json.dumps(records, indent=2) + "\n", encoding="utf-8"
            )
            metrics = result["metrics"]
            print(
                f"{label}: reads={metrics['event_reads']} "
                f"p95={metrics['stable_visibility_p95_ms']:.1f}ms "
                f"rejects={metrics.get('rejected_writes', 0)}", flush=True
            )
    print(f"Saved {len(records)} runs to {args.output}")


if __name__ == "__main__":
    main()
