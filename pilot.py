"""Mechanism pilot, not a performance benchmark.

Run: python pilot.py --output results.json
Test: python -m unittest -v

All times are simulated milliseconds. No external services or dependencies.
"""

from __future__ import annotations

import argparse
import heapq
import json
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class Config:
    events: int = 10
    arrival_ms: int = 20
    scan_base_ms: int = 30
    scan_per_event_ms: int = 5
    tick_ms: int = 25
    lease_ms: int = 100
    failure_ms: int = 65
    recovery_ms: int = 400
    horizon_ms: int = 600

    def __post_init__(self) -> None:
        if self.events < 1 or min(
            self.arrival_ms, self.scan_base_ms, self.tick_ms, self.lease_ms
        ) <= 0 or self.scan_per_event_ms < 0:
            raise ValueError("Event counts and timing values must be positive")
        if self.lease_ms <= self.tick_ms:
            raise ValueError("Lease must exceed the renewal interval")
        if not 0 < self.failure_ms < self.recovery_ms < self.horizon_ms:
            raise ValueError("Fault and recovery must fit within the horizon")
        if self.events * self.arrival_ms >= self.horizon_ms:
            raise ValueError("Observation horizon must extend past input")


@dataclass(frozen=True)
class Job:
    count: int
    checkpoint: int
    epoch: int


@dataclass
class Worker:
    status: str = "up"
    pending: bool = False
    job: Job | None = None
    deferred: Job | None = None


class Pilot:
    def __init__(
        self, strategy: str, routing: str, scenario: str, config: Config | None = None
    ) -> None:
        if strategy not in ("overlap", "owned"):
            raise ValueError("Unknown strategy")
        if routing not in ("sticky", "round_robin"):
            raise ValueError("Unknown routing")
        if scenario not in ("normal", "crash", "pause"):
            raise ValueError("Unknown scenario")
        self.strategy, self.routing, self.scenario = strategy, routing, scenario
        self.config = config or Config()
        self.now = 0
        self.serial = 0
        self.queue: list[tuple[int, int, Callable[[], None]]] = []
        self.workers = [Worker(), Worker()]
        self.events: list[str] = []
        self.identities: set[str] = set()
        self.count, self.checkpoint = 100, 0
        self.owner: int | None = None
        self.epoch, self.expires = 0, 0
        self.route_cursor = 0
        self.metrics: Counter[str] = Counter()
        self.reads: Counter[int] = Counter()
        self.trace: list[dict] = []
        self.last_observed = 0
        self.behind = False
        self.final_ready_since: int | None = None
        self.fault_worker: int | None = None
        self.executed = False

    def schedule(self, when: int, action: Callable[[], None]) -> None:
        if when < self.now:
            raise ValueError("Cannot schedule in the past")
        self.serial += 1
        heapq.heappush(self.queue, (when, self.serial, action))

    def observe(self) -> None:
        if self.behind:
            self.metrics["stale_counter_ms"] += self.now - self.last_observed
        self.last_observed = self.now
        self.behind = self.checkpoint < len(self.events)
        final_ready = (
            len(self.events) == self.config.events
            and self.checkpoint == self.config.events
        )
        if not final_ready:
            self.final_ready_since = None
        elif self.final_ready_since is None:
            self.final_ready_since = self.now

    def record(self, action: str, **details: object) -> None:
        self.trace.append({"at_ms": self.now, "action": action, **details})

    def accept(self, identity: str) -> None:
        if identity in self.identities:
            self.metrics["duplicate_inputs"] += 1
            return
        self.identities.add(identity)
        self.events.append(identity)
        self.record("accept", event=identity)
        self.observe()
        self.dispatch()

    def route(self) -> int | None:
        # A pause is not a known crash: reminders can still target that worker.
        available = [
            i for i, worker in enumerate(self.workers) if worker.status != "down"
        ]
        if not available:
            return None
        if self.routing == "sticky":
            return available[0]
        candidate = available[self.route_cursor % len(available)]
        self.route_cursor += 1
        return candidate

    def dispatch(self) -> None:
        candidate = self.route()
        if candidate is None:
            return
        if self.strategy == "owned":
            self.metrics["lease_lookups"] += 1
            if self.owner is not None and self.now < self.expires:
                candidate = self.owner
            else:
                candidates = [
                    i for i, worker in enumerate(self.workers)
                    if worker.status == "up"
                ]
                if not candidates:
                    return
                if candidate not in candidates:
                    candidate = candidates[0]
                self.owner = candidate
                self.epoch += 1
                self.expires = self.now + self.config.lease_ms
                self.metrics["lease_grants"] += 1
                self.record("grant", worker=candidate, epoch=self.epoch)
        worker = self.workers[candidate]
        worker.pending = True
        self.start(candidate)

    def start(self, index: int) -> None:
        worker = self.workers[index]
        if worker.status != "up" or worker.job is not None or not worker.pending:
            return
        if self.strategy == "owned" and (
            self.owner != index or self.now >= self.expires
        ):
            worker.pending = False
            return
        worker.pending = False
        self.metrics["rollup_reads"] += 1
        end = len(self.events)
        size = end - self.checkpoint
        if not size:
            return
        for sequence in range(self.checkpoint + 1, end + 1):
            self.reads[sequence] += 1
        self.metrics["event_reads"] += size
        job = Job(self.count + size, end, self.epoch)
        worker.job = job
        active = sum(w.job is not None for w in self.workers)
        self.metrics["max_active_jobs"] = max(self.metrics["max_active_jobs"], active)
        self.record("start", worker=index, checkpoint=end, epoch=job.epoch)
        duration = self.config.scan_base_ms + size * self.config.scan_per_event_ms
        self.schedule(self.now + duration, lambda: self.finish(index, job))

    def finish(self, index: int, job: Job) -> None:
        worker = self.workers[index]
        if worker.job is not job:
            return
        if worker.status == "paused":
            worker.deferred = job
            return
        if worker.status != "up":
            return
        worker.job = None
        self.metrics["rollup_write_attempts"] += 1
        if self.strategy == "owned" and (
            job.epoch != self.epoch or self.owner != index or self.now >= self.expires
        ):
            self.metrics["rejected_writes"] += 1
            self.record("reject", worker=index, epoch=job.epoch)
        else:
            if job.count != 100 + job.checkpoint:
                raise AssertionError("Rollup differs from reference event prefix")
            if job.checkpoint < self.checkpoint:
                self.metrics["checkpoint_regressions"] += 1
            self.count, self.checkpoint = job.count, job.checkpoint
            self.metrics["rollup_writes"] += 1
            self.record(
                "save", worker=index, count=self.count, checkpoint=self.checkpoint
            )
            self.observe()
        self.start(index)

    def tick(self) -> None:
        if self.strategy == "owned" and self.owner is not None:
            if (
                self.workers[self.owner].status == "up"
                and self.now < self.expires
            ):
                self.metrics["lease_renewals"] += 1
                self.expires = self.now + self.config.lease_ms
        self.metrics["reconciliation_checks"] += 1
        if self.checkpoint < len(self.events):
            self.dispatch()

    def fail(self) -> None:
        if self.strategy == "owned":
            target = self.owner
        else:
            target = next(
                (i for i, w in enumerate(self.workers) if w.job is not None), None
            )
        if target is None:
            raise AssertionError("Fault scenario did not interrupt an active worker")
        self.fault_worker = target
        worker = self.workers[target]
        worker.status = "down" if self.scenario == "crash" else "paused"
        if self.scenario == "crash":
            worker.job = None
            worker.pending = False
        self.record(self.scenario, worker=target)

    def recover(self) -> None:
        if self.fault_worker is None:
            raise AssertionError("No worker to recover")
        target = self.fault_worker
        worker = self.workers[target]
        worker.status = "up"
        self.record("recover", worker=target)
        if worker.deferred is not None:
            job, worker.deferred = worker.deferred, None
            self.finish(target, job)
        self.start(target)
        if self.checkpoint < len(self.events):
            self.dispatch()

    def run(self) -> dict:
        if self.executed:
            raise ValueError("Each pilot instance runs once")
        self.executed = True
        for sequence in range(1, self.config.events + 1):
            identity = f"E{sequence}"
            self.schedule(
                sequence * self.config.arrival_ms,
                lambda identity=identity: self.accept(identity),
            )
        for when in range(self.config.tick_ms, self.config.horizon_ms + 1,
                          self.config.tick_ms):
            self.schedule(when, self.tick)
        if self.scenario != "normal":
            self.schedule(self.config.failure_ms, self.fail)
            self.schedule(self.config.recovery_ms, self.recover)
        while self.queue and self.queue[0][0] <= self.config.horizon_ms:
            self.now, _, action = heapq.heappop(self.queue)
            action()
        self.now = self.config.horizon_ms
        self.observe()
        metric_names = (
            "event_reads", "rollup_reads", "rollup_write_attempts", "rollup_writes",
            "rejected_writes", "lease_lookups", "lease_grants", "lease_renewals",
            "checkpoint_regressions", "stale_counter_ms", "max_active_jobs",
            "reconciliation_checks", "duplicate_inputs",
        )
        result = {
            "strategy": self.strategy,
            "routing": self.routing,
            "scenario": self.scenario,
            "config": asdict(self.config),
            "metrics": {name: self.metrics[name] for name in metric_names},
            "final_count": self.count,
            "expected_count": 100 + len(self.events),
            "converged": self.checkpoint == len(self.events),
            "trace": self.trace,
        }
        result["metrics"]["repeated_event_reads"] = sum(
            max(0, count - 1) for count in self.reads.values()
        )
        result["metrics"]["final_catchup_ms"] = (
            None if self.final_ready_since is None else
            self.final_ready_since - self.config.events * self.config.arrival_ms
        )
        return result


def run_matrix() -> dict:
    return {
        "kind": "Deterministic model; supplied durations are not benchmark results",
        "runs": [
            Pilot(strategy, routing, scenario).run()
            for scenario in ("normal", "crash", "pause")
            for routing in ("sticky", "round_robin")
            for strategy in ("overlap", "owned")
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run_matrix()
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print("MODELED operations and times only; not a database benchmark")
    print("scenario routing      strategy reads repeated catchup_ms rejected regressions")
    for run in result["runs"]:
        metrics = run["metrics"]
        print(
            f"{run['scenario']:8} {run['routing']:12} {run['strategy']:8} "
            f"{metrics['event_reads']:5} {metrics['repeated_event_reads']:8} "
            f"{str(metrics['final_catchup_ms']):>10} "
            f"{metrics['rejected_writes']:8} {metrics['checkpoint_regressions']:11}"
        )
    if not all(run["converged"] for run in result["runs"]):
        raise SystemExit("Pilot did not converge within the observation horizon")


if __name__ == "__main__":
    main()
