# Counter ownership study

A small, reproducible experiment about three separate decisions:

1. Where should a counter's aggregation work be routed?
2. Who is allowed to publish a result?
3. Can an older result replace a newer checkpoint?

The study uses original Python code, synthetic inputs, two aggregation processes,
an independent observer process, and a shared local SQLite database. It is not a
distributed database benchmark, a new coordination algorithm, or a throughput
capacity claim.

## Reproduce

Python 3.12 or newer; standard library only. Run from this directory in PowerShell:

```powershell
$env:PYTHONDONTWRITEBYTECODE = '1'
python -m unittest -v
python report.py release-evidence
python storage_study.py --output reproduction --repeats 5
python report.py reproduction
```

The runner requires a new output directory and refuses to overwrite one.
The full matrix takes several minutes. It only starts and stops its own child
processes and creates databases under the specified output directory. No network
service, account, credentials, container, or package installation is needed.

Timing results will vary with the machine and background activity. Identical
input does not mean identical thread scheduling or latencies. Auditing the
included evidence does not rerun the workload.

For a short infrastructure smoke run:

```powershell
python storage_study.py --output quick-check --repeats 1 --smoke
```

The smoke selection is deliberately incomplete; `report.py` requires the full
balanced matrix.

## The three strategies

| Name | Aggregation | Rule for publishing |
| --- | --- | --- |
| `overlap` | Any worker may calculate a result | Replace the stored count and checkpoint together |
| `owned` | A worker acquires a lease for each counter | Verify owner, epoch, and unexpired lease inside the write transaction |
| `guarded` | Any worker may calculate a result | Reject a checkpoint older than the stored checkpoint, inside the write transaction |

The progress guard does not avoid the calculation that produced a rejected
result. It prevents that result from undoing newer progress.

All strategies calculate a replacement total, not another increment to the
rollup. Equal checkpoints describe the same immutable event prefix, so the
guard permits equal checkpoints. Each input is a distinct positive increment
with an integer identity. Resets, decrements, late event insertion, retention,
replication, and external side effects are outside this experiment.

## Controlled workload

Each trial receives the same 480 events across eight counters. Counter zero
receives 384 of them. A seeded generator creates the same event-to-counter
mapping in every trial. The controller offers eight events every 20 ms for
60 batches, then allows 1,200 ms for settling. The planned observation window
is about 2.4 seconds, excluding process startup and shutdown.

Transactions may delay acknowledgements and bunch later batches together.
Actual input spans are recorded, rather than claiming an exactly achieved
arrival rate. Event acceptance is timestamped after the ingestion transaction
commits.

All variants use two spawned worker processes, the same event log and rollup
calculation, and a 40 ms aggregation cadence. Each worker coalesces pending
counter identifiers in a set. A common reconciler checks for lagging counters
every 40 ms; lost in-memory reminders are therefore not a separate failure
advantage for one strategy.

Both routing patterns send a reminder to one worker, never broadcast it:

- `sticky` chooses a worker by counter identity among living processes.
- `round_robin` spreads reminders across living processes.

For `owned`, valid lease metadata redirects reminders to the owner. Once a
lease expires, the router can send work to the other process. This metadata
lookup is counted. Leases last 250 ms and are renewed at a requested 50 ms
interval in the worker's main loop, not in a thread that continues during a
pause.

The controller knows when a local process exits. It does not classify a paused
process as dead. That distinction is part of this local experiment, not a
general distributed failure detector.

## Faults

The matrix includes normal operation, a controlled process exit, and a pause.
After at least 160 events have been acknowledged, the controller arms one
worker. The actual interruption happens on its next nonempty snapshot of the
hot counter, after reading the event suffix but before opening the write
transaction.

For ownership, the armed worker is the hot counter's current owner. For the
other strategies, it is worker zero. The exact fault time and checkpoint are
recorded. Work scheduling can delay the interruption, so it is not claimed to
happen at an identical wall-clock instant or checkpoint.

The exit scenario ends that process with status 73 and leaves the other worker
running. This is a controlled application exit between transactions, not power
loss, a killed database server, or a database crash during commit.

The pause scenario sleeps for 600 ms at the same code boundary, without holding
a database transaction. The process then attempts its saved result. It does not
renew leases during the sleep. This exercises resumed stale work, not an
operating-system suspension or a garbage collector implementation.

## Measurements and correctness

`release-evidence` is the final evidence set: five repetitions of each of
18 configurations, run sequentially in a seeded shuffled order within each
repetition. The configurations vary strategy, routing, and fault type.

**Event reads** count rows returned to aggregation code, including abandoned,
repeated, and rejected work. They are not physical disk reads, bytes transferred,
SQL statement counts, or a dollar-cost estimate.

**Stable observed visibility delay** measures from an event's ingestion
acknowledgement to the first observer sample after the last sample that omitted
it. Thus an event that appears, disappears after a stale overwrite, and then
reappears is not credited at its first appearance. Stability is checked only
through the recorded observation horizon.

The observer is a separate spawned process. Its requested sampling interval is
5 ms; actual gaps are recorded. Sampling cannot prove that a transient
regression did not happen between samples. A separate transactional commit log
detects every committed checkpoint regression.

Per-run p95 uses the sorted event delays at index `floor((n - 1) * 0.95)`.
Group summaries report the median, minimum, and maximum of five per-run
statistics, not a pooled percentile or confidence interval.

Coordination counters include lease reads, grant transactions, renewal
transactions and rows, routing lookups, and write-time fencing checks. These are
reported separately: a renewal transaction is not assigned the same cost as an
event row read. Idle renewals during the shared observation period are included.

The audit verifies:

- All trials have identical input, preserved in SQLite.
- Every committed rollup equals the reference count for its counter and checkpoint.
- All counters converge by the end of each trial.
- Every configured fault actually targets counter zero.
- Worker operation counts agree with the raw reports.
- Freshness metrics regenerate from raw acknowledgement and observer traces.
- Each database passes `PRAGMA integrity_check`.
- The measured source hash matches `storage_study.py`.

The owned and guarded strategies also fail a run if they regress. Owned pause
runs must exercise a rejected write. Guarded pause runs with spread routing
must exercise the progress guard.

## What is not established

SQLite WAL allows concurrent readers but only one writer at a time. Both
ingestion and ownership management share that writer. This affects latency
and makes the setup unsuitable for extrapolating a distributed database cost
or capacity curve. SQLite, its OS, and the shared machine's scheduler remain
part of the results. There is no CPU pinning or isolation from other users'
workloads.

The clock is `perf_counter_ns`, shared across processes on the tested Windows
runtime. Lease time is evaluated on one host. The code is not a portable
multi-host lease protocol and does not test clock skew, partitions, consensus,
or lease-store unavailability.

The reference property is an immutable, ordered prefix. A progress guard on
that checkpoint would not by itself make arbitrary side effects, mutable
history, or resets safe. This study does not establish exactly-once execution.

Five short repetitions and one workload do not establish a universal winner
or a point where ownership becomes cheaper. Raw ranges and timing caveats are
part of the result.

## Files

| File | Purpose |
| --- | --- |
| `storage_study.py` | Real SQLite experiment and process lifecycle |
| `test_storage_study.py` | Focused storage, fencing, progress, and measurement checks |
| `report.py` | Independent regeneration and audit of the recorded evidence |
| `release-evidence/summary.json` | Full group medians and ranges |
| `release-evidence/summary.csv` | Machine-readable comparison |
| `release-evidence/results.json` | All final run summaries |
| `release-evidence/*/storage.sqlite` | Persisted event log, rollups, leases, and commit history |
| `release-evidence/*/trace.json` | Acknowledgements, observer samples, fault notices, worker counts |
| `article-draft.md` | Unpublished article draft for review |
| `pilot.py`, `test_pilot.py`, `pilot-results.json` | Earlier deterministic mechanism model |

Exploratory directories are excluded from Git by name, not silently mixed into
the final results. Earlier passes revealed an uncontrolled fault target and an
observer coupled to the ingestion loop. Those passes were superseded by the
final protocol. One observer smoke run exposed a shutdown queue-draining bug;
the source fixes it and includes a regression test. None of those runs are
counted among the final 90.

## Background

- [Amazon Builders' Library: leader election](https://aws.amazon.com/builders-library/leader-election-in-distributed-systems/)
- [Martin Kleppmann: distributed locking and fencing](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)
- [SQLite: WAL concurrency](https://sqlite.org/wal.html)

The implementation, inputs, and diagrams in this project are original.
Original code, tests, configuration, synthetic inputs, and generated experiment
data are covered by [the MIT license](LICENSE). The article text in
`article-draft.md` is excluded; its rights are retained by the author.
