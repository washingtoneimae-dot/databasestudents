# PerfectDB (MVM)

Memory that never forgets, never lies, always fast. Log-first embedded database proven by two identical engines.

Full design: see `technical.md`.

## What is this?

Minimal viable database proving the core thesis: **segmented WAL + MemTable + checksummed
immutable files + manifest + LSN versions = safe + fast + time-travel**, with one engine in
Node.js and an exact port in C# (byte-identical files, same `MANIFEST.json`).

- Durable commits (WAL + fsync before ack), crash recovery by replay, no loss verified.
- Transactions (buffered tx, single-batch commit), MVCC snapshots, `get(k, snapshotLsn)` time-travel.
- Auto-flush to SSTables with atomic publish (`write .tmp -> rename`).
- Same file format in both engines — outputs are byte-identical (380.7KB for 4000 rows).

## Quickstart

Students start here — one file, zero deps, 20 minutes:

```
node "student/demo.js"    # narrated tour: writes, time travel, flush, crash
```

then read `student/perfectdb-student.js` top to bottom, then `student/README.md`
for the break-it-yourself exercises. It reads the real engines' files.

Builders:

```
node "mvm/bench.js"
```

C# (.NET 10):

```
dotnet run -c Release --project "csharp-mvm/csharp-mvm.csproj"
```

## Results (measured 2026-09-13, Windows + OneDrive disk)

| test | Node 22 | C# Release |
|---|---|---|
| 2000 single-commit (1 fsync/op) | 71 ops/s | 73 ops/s — disk-bound |
| 2000 batched (1 tx) | 18868 ops/s | 32275 ops/s — C# 1.7x |
| 9600 commits / 64 writers, grouped (durable) | 2817 ops/s, avg batch 64.0 | 1222 ops/s, avg batch 27.6 |
| 800 commits / 8 writers, plain (durable) | — | 69 ops/s engine = 69 ops/s SQLite — fsync-bound tie |
| 5000 random reads | 555k ops/s | 508k ops/s |
| scan 4000 rows | 7ms | 6ms |
| recovery | 20ms OK no-loss | 27ms OK no-loss |

Takeaway: batching beats language (265–442x win); fsync latency dominates single writes.

## Layout

```
student/perfectdb-student.js  # teaching edition: single-file narrated core
student/demo.js               # guided demo, exits nonzero on failure
mvm/perfect_db_mvm.js   # Node engine
mvm/bench.js            # Node bench
csharp-mvm/PerfectDb.cs # C# engine (exact port)
csharp-mvm/Program.cs   # C# bench
technical.md            # full spec, formats, API, roadmap
```

## Next

Group-commit done, crash-safe compaction done, kill -9 suite green (`technical.md:7b–7d`),
SSTable checksums done (`technical.md:7e`) → secondary/vector index → replication.
