# MIT 6.5840 / 6.824 Distributed Systems — Lab Solutions (Go)

> Complete, from-skeleton solutions to the MIT 6.5840 (formerly 6.824) Distributed
> Systems labs in Go — MapReduce, an unreliable-network Key/Value server, **Raft**
> consensus, a fault-tolerant Key/Value store on Raft, and a sharded Key/Value
> system — passing the courses' own `go test` suites under the race detector.
> Part of a [csdiy.wiki](https://csdiy.wiki/) full-catalog build.

![status](https://img.shields.io/badge/status-complete-brightgreen)
![language](https://img.shields.io/badge/Go-1.22-informational)
![license](https://img.shields.io/badge/license-MIT-blue)

## Overview

MIT 6.5840 builds a fault-tolerant, sharded key/value store from the ground up. This
repository is an independent implementation of every lab, on top of the official
[`6.5840-golabs-2024`](https://pdos.csail.mit.edu/6.824/) skeleton (imported as the
base commit). Each lab is verified with the course's own test harness, run under the
Go race detector.

The centerpiece is a full **Raft** implementation (Ongaro & Ousterhout, *In Search of
an Understandable Consensus Algorithm*): leader election, log replication with the
accelerated log-backtracking optimization, crash-recovery persistence, and snapshot /
log compaction. The KV server, fault-tolerant KV-on-Raft, shard controller, and
sharded KV are all built on it.

## Results (measured on WSL2 Ubuntu, Go 1.22, `-race`)

All numbers below are real `go test` output captured to [`results/`](results/).

| Lab | What it implements | Result (measured) |
|---|---|---|
| **1 — MapReduce** | Coordinator + worker, crash-tolerant | `test-mr.sh`: **PASSED ALL TESTS** (wc, indexer, map/reduce parallelism, job count, early exit, crash) |
| **2 — KV server** | Linearizable KV over a lossy net, dup detection | `go test -race`: **PASS** — all functional + memory tests (incl. 100k-client stress) |
| **3A — Raft election** | Leader election | **3/3 pass** |
| **3B — Raft log** | Log replication + fast backup | **10/10 pass** |
| **3C — Raft persistence** | Crash recovery, Figure 8 (unreliable), churn | **8/8 pass** |
| **3D — Raft snapshots** | Log compaction, InstallSnapshot | **8/8 pass** |
| **4A — KV-Raft** | Fault-tolerant KV on Raft | **17/17 pass** (partitions, restarts, unreliable, linearizability) |
| **4B — KV-Raft snapshots** | Snapshotting KV service | **10/10 pass** |
| **5A — Shard controller** | Shard→group assignment, balanced rebalance | **12/12 pass** (incl. minimal-transfer checks) |
| **5B — Sharded KV** | Sharded store with live migration | **9/9 pass** + 3/3 challenge tests (shard deletion, unaffected/partial-migration access) |

Total: **80+ individual `go test` cases pass under `-race`.** Every table entry
is backed by a captured log in [`results/`](results/).

## Implemented labs

- [x] **Lab 1 — MapReduce** (`src/mr`) — a coordinator hands out map then reduce tasks,
  re-issues tasks from crashed workers, and the worker writes intermediate/output files
  atomically.
- [x] **Lab 2 — Key/Value Server** (`src/kvsrv`) — a linearizable single-server KV store
  over an unreliable network, with client-id/sequence-number duplicate suppression and
  bounded per-client state.
- [x] **Lab 3 — Raft** (`src/raft`) — leader election (3A), log replication (3B),
  persistence (3C), and snapshots (3D).
- [x] **Lab 4 — KV-Raft** (`src/kvraft`) — a fault-tolerant KV service replicated by
  Raft, with exactly-once semantics and Raft-log snapshotting (4A/4B).
- [x] **Lab 5 — Sharded KV** (`src/shardctrler`, `src/shardkv`) — a shard controller that
  balances shards across replica groups (5A) and a sharded KV store that migrates shards
  live as the configuration changes (5B).

## How to run

The labs are Go and run on Linux (this repo was developed and tested on WSL2 Ubuntu
with Go 1.22).

```bash
cd src

# Lab 1 — MapReduce
cd main && bash test-mr.sh          # add RACE=-race inside the script for the race detector

# Lab 2 — KV server
cd src/kvsrv && go test -race

# Lab 3 — Raft (run parts individually; they are long)
cd src/raft
go test -run 3A -race
go test -run 3B -race
go test -run 3C -race
go test -run 3D -race
# flaky-test confidence: run repeatedly, e.g.
go test -run 3A -race -count=10

# Lab 4 — KV on Raft
cd src/kvraft && go test -race

# Lab 5 — Sharded KV
cd src/shardctrler && go test -race
cd src/shardkv && go test -race
```

> **Note on WSL2:** the Windows-mounted `9p` filesystem has pathological performance for
> `os.CreateTemp`/`os.Remove` and can hang MapReduce and the 100k-client memory tests.
> The MapReduce worker therefore uses deterministic per-PID temp file names instead of
> `os.CreateTemp`. When reproducing the full suites on WSL2, run the tests from a native
> ext4 path (e.g. `~/`) — the code is identical; only the working directory changes.

## Verification

Every result in the table above comes from the course's unmodified test harness:

- MapReduce: `src/main/test-mr.sh` (the official script), race detector enabled.
- Every other lab: the provided `test_test.go` via `go test -race`.

Raw captured output lives in [`results/`](results/), one file per lab, including the
multi-iteration Raft runs used to gauge the flaky tests.

## Project structure

```
src/
├── mr/            Lab 1 — MapReduce coordinator + worker
├── main/          test drivers (test-mr.sh, mrcoordinator, mrworker, ...)
├── mrapps/        MapReduce plugin apps (wc, indexer, crash, ...)
├── kvsrv/         Lab 2 — single-server KV
├── raft/          Lab 3 — Raft consensus
├── kvraft/        Lab 4 — fault-tolerant KV on Raft
├── shardctrler/   Lab 5A — shard controller
├── shardkv/       Lab 5B — sharded KV store
├── labrpc/        provided RPC simulation (lossy network)
├── labgob/        provided gob wrapper
├── porcupine/     provided linearizability checker
└── models/        provided KV model for the checker
```

## Key ideas / what I learned

- **Raft correctness lives in the edge cases.** The commit rule (only commit
  current-term entries by majority, then earlier terms follow) and the "at least as
  up-to-date" vote check are what make Figure 8 pass.
- **Accelerated log backtracking** (conflict term + first index of that term) turns
  `O(log)` AppendEntries rejections into `O(#terms)`, which is what makes the "backs up
  quickly" test finish in time.
- **Exactly-once on top of at-least-once.** Client-id + monotonic sequence numbers,
  cached per client, give linearizable Put/Append despite dropped and duplicated RPCs.
- **Snapshots are a state-machine handshake.** Raft trims its log and the service
  restores from the snapshot; keeping `lastIncludedIndex`/`Term` consistent across the
  RPC, the persister, and the applier is the tricky part.
- **Live resharding is a per-shard state machine** — Serving → Pulling → GCing — driven
  entirely through the Raft log so every replica agrees on when a shard changes hands.

## Credits & license

Based on the labs of **MIT 6.5840 / 6.824 Distributed Systems** by Robert Morris, Frans
Kaashoek, and the MIT PDOS group. This repository is an independent educational
reimplementation; all course materials, the lab skeleton, and the provided test
harnesses (`labrpc`, `labgob`, `porcupine`, `test_test.go`, `config.go`, `test-mr.sh`)
belong to their original authors. Original solution code in this repo is released under
the [MIT License](LICENSE).
