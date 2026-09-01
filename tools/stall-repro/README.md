# Consensus stall repro

Reproduces this line from the chaos incidents:

    WARN Slow wait: removing a peer in handle_replica_changes took 66.71s

Why it happens: a node applies `drop_replica` for its *own* replica while that shard's
optimizer is healing an HNSW graph (rebuilding on top of an old index). Stopping the shard
joins the optimizer task, and the heal phase has no cancellation check, so the consensus
thread waits for it to finish. Measurements and the code path: [REPORT.md](REPORT.md).

## Run it

You need a qdrant binary (`cargo build --profile perf --bin qdrant`, or `QDRANT_BIN=...`),
docker (for the `qdrant/bfb:dev` load generator), `systemd-run --user` (Linux) and
python3 with `requests`.

    ./stall_repro.py

That is all. It starts 3 peers throttled to half a CPU each, creates a 1-shard collection
replicated on two of them, loads 100k points, keeps overwriting them so the index gets rebuilt
with healing, and each time a heal build starts it drops that peer's replica and measures how
long the node takes to apply the entry. Then it puts the replica back and repeats. Five trials
take about 10 minutes. It ends with a table like

    trial  healed  slow wait  applied
        0    8035      23.83    23.99
        1     601       0.66     0.72
        2   13905      25.33    25.40

`healed` is the number of points the heal had to process, `slow wait` the seconds from the WARN
above, `applied` seconds from sending `drop_replica` to the node applying it. A `-` in
`slow wait` means no stall (the heal finished before the entry arrived). While a trial runs it
prints the log lines that bracket the stall (`Stopping flush worker` … `Migrated in` …
`Optimization cancelled` … `Slow wait`). Full log excerpts per trial land in `trials/`, all
numbers in `results.jsonl`.

## Knobs

    ./stall_repro.py --trials 10 --min-heal 5000   # more trials, only big heals (= long stalls)
    CPU_QUOTA=800% ./stall_repro.py                # no CPU throttling: stalls get shorter, not gone
    ./stall_repro.py up      # step by step: start cluster + load + churn, leave it running
    ./stall_repro.py run     #   trials against the running cluster (logs in logs/)
    ./stall_repro.py down    #   stop everything

Ports: http 7303/7313/7323, grpc +1, p2p +2 (`BASE_PORT` moves them).

## Files

- `stall_repro.py` — the repro
- `REPORT.md` — what was measured, why it is not a lock and not tokio, fix directions
- `results/` — raw logs behind the report's numbers (older 3-shard harness;
  `results/analyze.py results/throttled` re-tabulates them)
