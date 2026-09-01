# Consensus stall repro

Reproduces this line from the chaos incidents:

    WARN Slow wait: removing a peer in handle_replica_changes took 66.71s

Cause: a node applies `drop_replica` for its own replica while that shard's optimizer is
healing an HNSW graph. Stopping the shard joins the optimizer task, the heal phase can't be
cancelled, so the consensus thread waits for it.

## Run

Needs a qdrant binary (`cargo build --profile perf --bin qdrant`, or `QDRANT_BIN=...`),
docker (`qdrant/bfb:dev`), `systemd-run --user`, python3 + `requests`.

    ./stall_repro.py

Starts 3 peers at half a CPU each, a 1-shard / RF-2 collection with 100k points, overwrites
them continuously so segments get rebuilt with healing, and on every heal build drops that
peer's replica and measures how long the entry takes to apply. 3 trials ≈ 8 min. Output:

    peer 0: reusing 36786 points, healing 3636
    drop_replica sent to peer 0: HTTP 500 after 3.82s
         +0.10s  Applying committed entry with index 17
         +0.16s  Stopping flush worker for shard ...
        +10.50s  Migrated in 10.601335724s                    <- heal phase ends
        +10.51s  Optimization cancelled - process cancelled by service
        +10.61s  WARN Slow wait: removing a peer in handle_replica_changes took 10.50s
        +10.61s  Successfully applied consensus operation entry. Index: 17
    ...
    trial  healed  slow wait  applied
        0   12733      35.42    35.56
        1    3636      10.50    10.61
        2    3534      10.12    10.28

`slow wait` = seconds from the WARN, `applied` = seconds from sending `drop_replica` until
applied, `-` = no stall. `rebuild without heal (65% of the segment changed)` while waiting is
normal: qdrant heals only if < 30% of a segment changed, otherwise it rebuilds from scratch
(cancellable, no stall). Per-trial logs in `trials/`, numbers in `results.jsonl`.

## Knobs

    ./stall_repro.py --trials 10 --min-heal 5000   # more trials, only big heals
    CPU_QUOTA=800% ./stall_repro.py                # unthrottled: shorter stalls, still there
    ./stall_repro.py up | run | down               # step by step

Ports: http 7303/7313/7323, grpc +1, p2p +2 (`BASE_PORT`).
