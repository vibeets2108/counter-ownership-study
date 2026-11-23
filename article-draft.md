# Does This Counter Need an Owner?

An experiment separating duplicate work, worker takeover, and stale results.

A counter has a saved total and a log of events that have not yet been included.
Two workers can calculate the next total. Letting both run might waste work.
Assigning one owner seems like the obvious improvement, until that owner stops.

*Written with AI assistance.*

The question was whether exclusive ownership earns its coordination cost. A
small experiment revealed that this question bundles together three different
decisions: where work goes, who may publish, and whether an older result may
replace newer progress.

Separating those decisions changed the comparison. Protecting the saved
checkpoint prevented stale results without requiring an exclusive owner. But
that protection could not help a counter whose work remained routed to a
paused worker.

## What the workers actually do

Consider a saved count of 100 and ten more positive increment events.

```text
Saved count:       100
New events:        ten increments
Computed total:    110
```

An aggregation worker reads the saved count and checkpoint, reads the event
suffix after that checkpoint, and writes a replacement pair.

Two workers writing the same pair do not make the total 120. Both write 110.
The duplicate work is in reading and calculating the result.

The more troublesome case involves different checkpoints:

```text
Worker A calculates through event 100, then pauses.
Worker B calculates through event 150 and saves its result.
Worker A resumes and saves its older result.
```

If every write is accepted, the saved checkpoint moves backward. The event log
is still present, so later aggregation can repair the count. Readers can
nevertheless see a result lose progress.

That is a different problem from paying for the same calculation twice.

## Three ways to publish

The experiment compares three strategies over the same event log and
aggregation code.

**Unrestricted overlap:** Any worker may aggregate and replace the saved count
and checkpoint.

**Exclusive ownership:** A worker obtains a lease for a counter. A new grant
advances an ownership number. The database checks the owner, that number, and
lease expiry inside the transaction that saves the result. A resumed former
owner cannot bypass the check.

**Protected overlap:** Any worker may aggregate, but the write transaction
rejects a checkpoint older than the one already stored.

```text
Incoming checkpoint < saved checkpoint: reject
Incoming checkpoint >= saved checkpoint: allow
```

This last rule compares checkpoints, not count values. It relies on the
experiment's immutable event history: the same prefix always produces the
same total. It does not avoid the reads that produced a rejected result, and
it is not a solution for arbitrary external side effects.

These are established mechanisms, not new algorithms.
[Amazon's discussion of leader election](https://aws.amazon.com/builders-library/leader-election-in-distributed-systems/)
describes the tradeoffs around ownership and failures.
[Martin Kleppmann's explanation of fencing](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)
explains why storage must participate in rejecting stale owners.

## A deliberately small experiment

The implementation uses Python 3.12.10, SQLite 3.49.1 in WAL mode with
`synchronous=FULL`, and separate processes on one Windows machine. There are
two aggregation workers and an independent observer.

Each trial receives the same 480 positive increment events across eight
counters. The busiest counter receives 384 events. The controller offers
eight events every 20 ms for 60 batches. Actual acknowledgement spans across
the trials range from about 1,167 to 1,196 ms.

Workers coalesce reminders and use a 40 ms aggregation cadence. A common
reconciler revisits unfinished counters every 40 ms. Both mechanisms apply
to all three strategies.

Ownership uses a 250 ms lease with renewals requested every 50 ms. The
observation window includes about 1.2 seconds of settling time after the
planned input period.

Each strategy runs with two routing policies:

**Sticky routing** normally keeps a counter on the same worker.

**Spread routing** sends successive reminders to alternating living workers.

Neither broadcasts work to both workers. In the ownership strategy, lease
metadata redirects reminders to the owner.

The fault cases arm a worker after 160 events have been acknowledged. The
interruption occurs during its next aggregation of the busy counter, after
reading events and before opening the write transaction. One case exits that
process. The other pauses it for 600 ms and then lets it attempt its result.
No database transaction is held during the pause.

The controller can detect that a local process has exited. It does not treat a
paused process as dead. This is a local failure model, not a distributed
failure detector.

There are five repetitions of each strategy, routing policy, and fault case:
90 final trials. Their order is shuffled within each repetition. All 90
converged, and all 8,370 committed results matched their event prefixes.

The [repository](https://github.com/vibeets2108/counter-ownership-study) includes
the source, configuration, raw observations, saved databases, and reproduction
instructions. The recorded results are available for inspection, not just
the summary numbers below.

## What the numbers mean

An event read means a row returned to aggregation code. Repeated and abandoned
reads count too. This is not a count of physical disk reads or a cost estimate.

Freshness is measured from acknowledgement of an event to its stable observed
inclusion in the saved checkpoint. If a later write removes that event from
the visible prefix, the measurement waits for it to reappear and remain
included through the observation horizon.

The observer requests a sample every 5 ms. During input, the actual median
sample gap was 5.48 ms and the maximum was 8.46 ms. Brief changes between
samples can be missed, so a separate transactional history records every
committed checkpoint regression.

Each latency below is the median of five runs' p95 values. The accompanying
range is the minimum and maximum of those five p95 values, not a confidence
interval. Small latency differences should not be treated as a general
ranking.

## Routing already avoids much of the duplicate work

With sticky routing and no failure, all three strategies read exactly 480
events in every run. There was no duplicate event reading for ownership to
remove.

With spread routing, unrestricted overlap read a median of 481 events, with a
range of 480 to 521. Ownership read 480 in every run. Protected overlap read a
median of 494, with a range of 480 to 506.

The overlap results vary because processes race differently in each trial.
Their independent medians do not establish that adding a checkpoint guard
causes more reads.

Ownership was not free. In normal sticky runs, it added a median of eight
lease grant transactions and 90 renewal transactions over the observation
period. It also performed lease lookups and fencing checks. Renewals during
settling time are included.

A renewal transaction is not interchangeable with one returned event row.
Those counts expose the additional work; they do not establish a financial
break even point.

The narrower conclusion is useful: multiple workers do not automatically
produce substantial duplicate aggregation. Measure the routing behavior
before introducing ownership to solve a duplication problem.

## A process exit and a pause are different failures

When a process exited, spread unrestricted overlap had a median p95 visibility
delay of 49.8 ms, ranging from 46.1 to 51.8 ms. Protected overlap was 54.6 ms,
ranging from 48.4 to 58.1 ms. Ownership was 228.8 ms, ranging from 186.4 to
256.2 ms.

In this setup, the controller could route around the exited process without
waiting for a lease in the overlap variants. Ownership had to respect the
existing lease before another worker could publish.

The pause case exposed a different problem. With spread routing:

**Unrestricted overlap:** Median p95 delay was 569.3 ms, with a range of 538.4
to 577.4 ms. There was one committed checkpoint regression in each of the
five runs. Median event reads were 674, ranging from 662 to 674.

**Exclusive ownership:** Median p95 delay was 202.9 ms, with a range of 191.0
to 227.9 ms. Each run rejected the stale owner's write, and none regressed.
Median event reads were 492, ranging from 487 to 494.

**Protected overlap:** Median p95 delay was 82.4 ms, with a range of 55.0 to
95.6 ms. Each run rejected one superseded result, and none regressed. Median
event reads were 493, ranging from 485 to 499.

The unprotected counter could advance while a worker slept, then lose
progress when that worker returned. The guard removed that backward step
without waiting for exclusive ownership.

That does not mean the guard replaces routing or recovery. With sticky
routing, protected overlap had a median p95 delay of 617.4 ms, ranging from
602.0 to 622.8 ms. Its work remained directed at the paused process. Ownership
could transfer work after expiry; its corresponding median was 249.7 ms,
ranging from 199.5 to 259.9 ms.

A write condition cannot make an idle replacement discover work it never
receives.

## What this does not prove

This is a mechanism study on a shared local machine, not a production
performance claim. [SQLite WAL has one writer at a time](https://sqlite.org/wal.html).
Ingestion, lease updates, and rollup writes therefore share a serialization
point. A different storage system can change their relative costs.

The measurements include real process scheduling and database operations, but
not network partitions, clock skew across machines, database failure, or
consensus. The process exit is controlled and occurs between transactions;
it is not a durability test under power loss.

The workload contains only positive increments over immutable, ordered
history. It excludes resets, retention, late insertion into an older prefix,
and replication. A checkpoint guard needs reconsideration if those
assumptions change.

Five short repetitions at one workload cannot tell us when ownership becomes
cheaper at scale. They do provide a reproducible example where the reason to
add ownership needs to be more specific than "multiple workers might run."

## The decision I would make from this evidence

For this counter, I would separate three requirements before choosing an
owner.

If the problem is duplicate reads, first examine routing and coalescing.
The sticky baseline eliminated duplicate reads during normal operation here.

If the problem is a stale calculation overwriting progress, test whether an
atomic checkpoint condition is sufficient. It was sufficient for that
property in this immutable history experiment.

If the problem is deciding who should take over when a worker becomes
unresponsive, publication checks alone are not enough. Ownership provides one
way to make that handoff, with an expiry interval and coordination work to
account for.

The result is not that owners are unnecessary. It is that routing, permission
to publish, and protection of progress are separate controls. Choosing one
does not automatically solve the other two.
