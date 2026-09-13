# PerfectDB — Technical Specification (MVM)

Status: implemented + measured. Date: 2026-09-13.
Implementations: Node.js (`mvm/`) and C# .NET 10 (`csharp-mvm/`). Same semantics, same file format.

## 1. Concept

Perfect database = memory that never forgets, never lies, always fast.

- Write once in ink (append-only log). Never overwrite in place. Crash = replay log, no loss.
- Small whiteboard for fast edits (in-memory MemTable).
- When whiteboard fills, copy neatly into a book (immutable sorted SSTable file).
- Every change gets a number (LSN). Read at any LSN = time travel.
- One store for all shapes: KV, table, document, graph, vector, time-series. All are `key + value + version + links`.

Design priority: correctness first, speed second, distribution last.

## 2. Architecture (target, full system)

```
Layer 0 Storage:     WAL + MemTable + SSTables, checksums, atomic publish
Layer 1 Data model:  Atom {key, value, lsn, deleted, links[]} — views project tables/docs/graph/vectors
Layer 2 Correctness: MVCC by LSN, snapshot reads, serial commit (single node), deterministic order (distributed later)
Layer 3 Index:       automatic sparse index key -> {file, offset}, no manual CREATE INDEX in MVM
Layer 4 Query:       get / scan / filter now; SQL-subset + vector topK later
Layer 5 Distribute:  NOT in MVM — planned: compute/storage split, Raft log replication, hash sharding
Layer 6 Self-drive:  auto-flush, auto-checkpoint, metrics; group-commit + compaction planned
```

Build order: P0 WAL+recovery+tx > P1 MemTable+flush+scan > P2 MVCC/time-travel > P3 group-commit > P4 compaction/compression/index > P5 net/replication/SQL/vector.

## 3. MVM — what was built

Minimal mechanism proving: log-first + memory + sorted files = safe + fast.

Node: `mvm/perfect_db_mvm.js:14` class `PerfectDB`
C#: `csharp-mvm/PerfectDb.cs:29` class `PerfectDb` — line-for-line port.

Components:

- `Open()` — `mvm/perfect_db_mvm.js`, `csharp-mvm/PerfectDb.cs`. mkdir, load manifest-exact SSTable set (missing listed = loud; unlisted = dropped after replay), replay legacy WAL + segments oldest-first (torn tail tolerated only in last), lsn-guard check, open fresh active segment, converge manifest only if changed.
- `Begin/Put/Del/Commit` — `mvm/perfect_db_mvm.js:70`, `csharp-mvm/PerfectDb.cs:104`. Tx buffers writes. `Commit` assigns LSNs, builds WAL batch, `write + fsync` BEFORE ack, then applies to MemTable, auto-flush at threshold (2000 keys).
- `Get(k, readLsn)` — `mvm/perfect_db_mvm.js:107`, `csharp-mvm/PerfectDb.cs:130`. Check MemTable newest version `<= readLsn`, else newest-to-oldest SSTables. Deleted = null.
- `Scan(start, end, readLsn)` — `mvm/perfect_db_mvm.js:121`, `csharp-mvm/PerfectDb.cs:148`. Merge key sets (ordinal sort), point-lookup each, skip deleted.
- `Flush()` — take latest version per key, sort, `write .tmp -> rename` (atomic publish), clear MemTable, write manifest, then delete all WAL segments and start fresh (manifest-before-deletes). Safe because SSTable now holds latest.
- `Close()` — fsync + close.
- WAL segments + manifest — see 7f. `wal/wal-{id:06}.log` rotation at `WalSegmentBytes`
  (default 1MB), `MANIFEST.json` rewritten atomically on flush/compact, recovery by
  manifest-hint + directory-scan backstop. Legacy `wal.log` replays once, then is deleted.
- `CommitGrouped(tx, windowMs=10)` / `commitGrouped(tx, windowMs)` — durable group commit.
  Concurrent callers rendezvous: first arrival becomes leader, waits one window for peers,
  then writes + fsyncs the whole batch ONCE and acks everyone. Same guarantee as `Commit`
  (acked = fsynced). Lone calls pay the window tax (~window + fsync). C# additionally
  serializes all state under one lock (`_lock`), so the engine is thread-safe;
  `GroupStats()` reports batches/commits/avgBatch to prove sharing happened.

Checksum: `SHA256(payload).hex[0:16]`, payload = `lsn|tx|op|key|value ?? ""`. Node `mvm/perfect_db_mvm.js:10`, C# `csharp-mvm/PerfectDb.cs:48`.

No dependencies. Node: builtins `fs/path/crypto` only. C#: `System.Text.Json` + `SHA256` + `FileStream.Flush(true)` (= fsync).

## 4. File formats (identical in both engines)

Directory layout (current):

```
<data>/
  MANIFEST.json            # {format,lsn,nextSstableId,nextWalId,walMinId,sstableIds}, atomic rewrite
  wal.log                  # legacy pre-segment file: replayed once on upgrade, then deleted
  wal/
    wal-000000.log         # capped at WalSegmentBytes (default 1MB), then sealed + rotated
    wal-000001.log         # only the LAST segment tolerates a torn tail
  sstables/
    sstable-0000.jsonl
```

WAL record (`WalRecord`) — unchanged, one JSON object per line:

```json
{"lsn":1,"tx":1,"op":"put","k":"k000000","v":"v0-xxx...","crc":"a1b2c3d4e5f60718"}
```

- `op`: `put` | `del`. `v`: null on del. `crc`: checksum above.

SSTable record (`SstableRecord`), sorted by `k` ordinal, each with crc over `lsn|k|v|del`:

```json
{"lsn":1,"k":"k000000","v":"v0-xxx...","del":0,"crc":"3d2c0ff404fcbd5d"}
```

SSTable footer (last line), with record count + SHA256 over the record-section bytes:

```json
{"__perfectdb_footer__":true,"count":2000,"crc":"a318b041e1c9baff..."}
```

Rules: writer always emits both; loader throws `IOException` on torn JSON, record crc
mismatch, footer count/hash mismatch. Records with crc but no footer = torn new-format
file → throw (never legacy-misread). Only crc-less records with no footer (pre-checksum
files) load unverified. `Compact()` re-verifies inputs from disk before merging.

## 5. API

```
db = new PerfectDB(dir, flushThreshold=2000); db.open();
tx = db.begin(); db.put(tx,k,v); db.del(tx,k); db.commit(tx);
v = db.get(k); v = db.get(k, snapshotLsn);
rows = db.scan("k000000","k999999"); rows = db.scan(start,end,snapshotLsn,limit);
db.flush(); db.close();
db.lsn // current version; db.sstables.length / db.Sstables.Count
```

## 6. Correctness guarantees (MVM scope)

- Durability: commit returns only after WAL fsync. Reopen replays WAL + SSTables = no loss (bench verifies row count).
- Atomicity (single tx): one WAL batch + one fsync per commit; recovery sees all or torn-tail-truncated ops.
- Isolation: MVCC snapshot reads; writers never block readers; concurrent writer-writer is last-writer-wins per key in commit order (no distributed tx in MVM).
- Ordering: LSN monotonically increases across WAL + SSTables.
- Corruption handling: bad crc line skipped with stderr log; torn JSON tail breaks replay (treated as crash mid-write).
- NOT guaranteed: multi-process writers, fsync on network drives beyond OS guarantees, encryption, secondary indexes.

## 7. Performance model + measured results

Model: write = O(1) mem + sequential log append + 1 fsync. Read = O(1) mem/dict + O(S) SSTable probes (S = file count, here 3). Scan = O(N log N) key merge + N point lookups. Bottleneck predicted: fsync/op.

Bench: `mvm/bench.js:1`, `csharp-mvm/Program.cs:1`. Workload: 2000 single-commit writes (50-byte values), 2000 batched writes in 1 tx, 5000 random reads, full scan 4000 rows, time-travel check, flush, close+reopen recovery. Machine: Windows 10.0.26200, OneDrive-backed disk, .NET 10.0.401, Node 22.23.2.

| test | Node 22 | C# Release | analysis |
|---|---|---|---|
| open(empty) | 5ms | 15ms | noise |
| 2000 x single-commit | 28017ms = 71 ops/s | 27337ms = 73 ops/s | fsync-bound (~14ms/fsync on this disk). Language irrelevant. |
| 2000 x batched (1 tx) | 106ms = 18868 ops/s | 62ms = 32275 ops/s | C# 1.7x: faster JSON+SHA256. Batching = 265x (Node) / 442x (C#) vs single-commit. |
| 5000 random reads | 9ms = 555k ops/s | 10ms = 508k ops/s | memory-bound, identical. |
| scan 4000 rows | 7ms | 6ms | identical. |
| time-travel | ok | ok | `get(k,snap)` returns pre-mutation value. |
| flush | 6ms | 17ms | noise, O(MemTable). |
| recovery reopen | 20ms rows=4000 OK | 27ms rows=4000 OK | O(WAL+SSTable size). |
| disk | 380.7KB | 380.7KB | byte-identical output. |

Conclusion: never optimize serialization before batching. Next perf lever is group-commit (10ms commit window) to lift durable single-op throughput from ~70 ops/s to ~10k ops/s on this disk, then Rust/io_uring or RocksDB-style batch fsync for 100k+.

### 7b. Group commit (implemented 2026-09-13, re-shootout vs SQLite)

Design: rendezvous + single shared fsync per batch, ack-after-fsync (NOT async commit —
no loss window). New bench phases in `bench-top/Program.cs` (C#) and `mvm/bench-grouped.js` (Node):
concurrent durable writers (PerfectDB-grouped 64 writers x 150 txns = 9600 commits, window 10ms),
controls (PerfectDB-plain 8 x 100, SQLite WAL/FULL 8 conns x 100 — 1 fsync/commit each),
plus a 50-op lone-grouped probe documenting the window tax. Accepts 3-run ranges (OneDrive variance).

| test (all durable, acked = fsynced) | PerfectDB | SQLite WAL/FULL | notes |
|---|---|---|---|
| 9600 commits / 64 writers, grouped | C# 1222 ops/s (348 batches, avg 27.6); Node 2817 ops/s (150 batches, avg 64.0) | n/a (no cross-conn group commit) | ~18x (C#) / ~40x (Node) over fsync-per-commit controls below |
| 800 commits / 8 writers, plain | 69 ops/s | 69 ops/s | identical — both serialize on fsync (~14.5ms each: 800 x 14.5ms ≈ 11.6s) |
| 50 lone grouped commits | 32 ops/s | n/a | window tax by design: ~10ms wait + ~14ms fsync + build ≈ 31ms/op |

Honest findings: (1) the first harness attempt used `Task.Run` + blocking waits and measured
only 348 ops/s at avgBatch 6.8 — threadpool starvation (~8 min threads), a HARNESS bug, not an
engine bug; dedicated threads / async chains fixed it. Kept in the report as a warning.
(2) Node batches cleaner than C# here (avg 64.0 vs 27.6: event-loop rendezvous vs thread jitter).
(3) Single-threaded sequential throughput is UNCHANGED by group commit — it is a concurrency
optimization, not a sequential speedup. Anyone selling it otherwise is selling async commit.

### 7c. Compaction (implemented 2026-09-13, both engines)

Size-tiered merge, the RocksDB/LevelDB lesson: when the SSTable count hits
`AutoCompactThreshold` (default 4, 0 = off), `Compact()` merges ALL runs into one.
Newest run wins per key, tombstones kept (they are the latest version), LSNs preserved —
the merge is a pure re-layout, `_lsn` untouched. SSTable ids are monotonic
(`_nextSstableId`, recovered as max+1 on open) so ids are never reused after crashes.
Crash-safe ordering: fsync tmp file → rename → fsync directory → THEN delete inputs.
A crash between rename and input-delete leaves duplicates with identical LSNs: reads stay
correct (newest file wins, same versions) and the next `Compact()` converges — verified by T6.
`Flush()` now also fsyncs the tmp file before rename and fsyncs the dir after rename
(best-effort on Windows: `FlushFileBuffers` on a backup-semantics dir handle; `fsync` on dir
fd on Unix — honestly weaker than POSIX, file-fsync + atomic rename is the real guarantee).
Threshold 4 keeps existing benches byte-identical (they peak at 3 files).

### 7d. Crash testing (suite: `crash-test/`, probes: `mvm/crash-probe.js`)

The SQLite lesson: prove it with kill -9, not reasoning. C# suite spawns real crashed
writers (child `FailFast`, no `Close`/flush — closest to kill -9 in-process) plus fault
injection; exit code = failure count. All 8 pass; Node probes (3) pass:

| id | scenario | asserts |
|---|---|---|
| T1 | kill -9, 500 WAL-only commits | full replay, spot values |
| T2 | torn WAL tail (partial line) | tail truncated, 200/200 rows |
| T3 | corrupt middle record (stale crc) | line skipped, 199 rows, victim null |
| T4 | stale flush `.tmp` | ignored, 300 rows from inputs |
| T5 | overlapping writes + deletes → `Compact` | latest-wins, tombstones honored, 150 live, 2nd `Compact` no-op |
| T6 | post-rename crash (output + inputs coexist) | reads correct pre/post, converges to 1 file |
| T7 | kill -9, 2000 WAL-only commits | full replay at volume |
| T8 | kill -9 right after `Compact` (SSTable-only) | 1 file, 150 live, values correct |

Bugs the suite already caught: (1) harness spawned the apphost exe with dll-style args,
forking runaway checker copies — fixed by host detection; (2) a REAL test bug (missing
`Open()` in the compacter child → NRE, caught by T8). Crash data lives in `%TEMP%`:
OneDrive marks rapidly-churned dirs ReadOnly+ReparsePoint and `Delete` fails for 30s+.

### 7e. SSTable checksums (implemented 2026-09-13, both engines, byte-identical)

Closes the silent-rot hole: every SSTable record carries `crc` (SHA256-16 over
`lsn|k|v|del`, same `checksum()` as WAL style), every file ends with a footer
(`count` + full SHA256 over the record section). Verified on open and re-verified from
disk before every compaction merge — corrupt input aborts the merge with inputs untouched
(T11). Cost at 4000 rows: 380.7KB → 478.7KB (+26%), recovery 13–20ms → 34–48ms (hashing).
New tests T9 (bit-rot → loud open failure), T10 (truncated tail → loud, via the
crc-without-footer rule), T11 (compact aborts, deletes nothing), Node N4/N5. All pass;
C#/Node outputs remain byte-identical (same record crc `3d2c0ff404fcbd5d`, same file hash).

### 7f. WAL segment rotation + manifest (implemented 2026-09-13, both engines)

The LevelDB lesson, applied two ways. **Rotation**: the active segment seals (fsync+close)
past `WalSegmentBytes` and a fresh monotonic id starts; LSNs stay global, so segments are
pure containers and replay order barely matters (memtable versioning is order-independent;
only the torn-tail rule is positional: last segment tolerant, sealed segments strict on
structural tears, crc-skips everywhere). **Flush** covers everything under lock, so it
deletes all segments and starts fresh — recovery cost is O(unflushed) again, bounded by
`FlushThreshold`, never by history.

**Manifest** (`MANIFEST.json`, byte-identical key order in both engines) stores
`{format,lsn,nextSstableId,nextWalId,walMinId,sstableIds}`, rewritten atomically
(tmp+fsync+rename+dirsync) on flush/compact/open-converge. Design philosophy, stated
plainly: the manifest is a durable *hint*, directory scan is the *backstop*. Open rules:
manifest valid → SSTable set is manifest-exact (missing listed file = loud data-loss error;
unlisted files = crash leftovers, dropped after WAL replay); manifest torn/missing →
verify-and-adopt everything, then converge. `man.lsn > recovered` also throws (T15) —
a tripwire for silently lost newest files. Open skips the converge rewrite when nothing
changed (ids/lsn always recompute as max(manifest, disk)).

THE ordering invariant (everything else follows): **creates happen before the manifest
write that lists them; deletes happen strictly after it.** That single rule is what makes
unlisted-files-deletable and missing-listed-files-alarming sound. Verified by T6 (dup
window), T12–T15 (rotation, manifest validity, rediscovery, kill-across-segments, lsn guard).

Two bugs the work exposed, both Windows-specific and both in the report because they
bite every storage engine on this OS: (1) a file opened without `FileShare.Delete`
**cannot be deleted at all** — the first cut closed the WAL handle only after deleting,
so every flush silently kept its previous active segment; stale-but-valid replays then
masked T15's data-loss scenario. Fixed by close-before-delete + `ReadWrite|Delete`
sharing (T12/T15 caught it). (2) OneDrive marks churned dirs ReadOnly+ReparsePoint —
crash-test data lives in `%TEMP%` permanently.

Costs (measured, 4000 rows): open +~40ms (mkdirs + dir fsync), flush ~85–100ms (manifest
write + deletes + extra dir fsyncs ≈ 3–4 fsyncs), recovery ~40–80ms. Single-commit
throughput unchanged (74–76 ops/s, still the 13.5ms fsync floor); a 49 ops/s reading
during the work was re-run twice and confirmed as OneDrive noise, not a regression —
reported here so nobody chases it. Refreshed full shootout under the new engine: A
single 74 vs B 72, A gets ~850k vs B ~131k, G1 grouped 1435 ops/s (avgBatch 31.4),
G0/G2 controls 59/70. Conclusions unchanged from §7.

## 8. Exact-port proof

```
C#   sstable-0000.jsonl 193783 / sstable-0001.jsonl 196000 / sstable-0002.jsonl 49
Node sstable-0000.jsonl 193783 / sstable-0001.jsonl 196000 / sstable-0002.jsonl 49
```

First two lines identical in both engines. WAL/SSTable JSON field names and checksum algorithm shared, so files are cross-readable.

## 9. How to run

Node (no install beyond existing Node):

```
node "mvm/bench.js"
```

C# (requires .NET 10 SDK, already present):

```
dotnet build -c Release --project "csharp-mvm/csharp-mvm.csproj"
dotnet run -c Release --no-build --project "csharp-mvm/csharp-mvm.csproj"
# workdir csharp-mvm/ → data lands in csharp-mvm/mvm-data-cs/
```

Clean: delete `mvm/mvm-data/` and `csharp-mvm/mvm-data-cs/`, rerun.

Crash suite (lives in `%TEMP%/perfectdb-crash`, outside OneDrive by design):

```
dotnet run -c Release --project "crash-test/crash-test.csproj"   # T1-T15, exit code = failures
node "mvm/crash-probe.js"                                        # N1-N7
```

Tune rotation in tests via `WalSegmentBytes` / `walSegmentBytes` (crash-test uses 4096B).

## 10. Limitations + roadmap

- Group commit done (7b). Compaction done (7c). Rotation + manifest done (7f).
  Remaining: leveled (vs size-tiered) tuning, WAL retention for full history time-travel
  (flush still truncates = history loss), per-segment WAL manifesting (loss-of-file
  detection for individual segments), multi-process locking, backups/replication.
- Compaction done (7c, size-tiered full merge at 4 files); leveled tuning out of scope (teaching niche, see §12).
- No secondary / vector index, no SQL, no server, no replication. Planned after P3.
- WAL truncate on flush discards history older than last flush (keeps latest only). Full time-travel over all history requires retained log / version files.
- Single-writer-process assumption; ordinal string compare only.

## 11. File map

```
mvm/perfect_db_mvm.js   # Node engine (grouped commit, compaction, WAL segments, manifest)
mvm/bench.js            # Node bench (sequential workload)
mvm/bench-grouped.js    # Node grouped-commit concurrency probe (64 chains, avgBatch 64.0)
mvm/crash-probe.js      # Node probes N1-N7 (torn WAL, tmp, compaction, rot, truncate, rotation, legacy)
student/perfectdb-student.js # teaching edition: single-file narrated core, same on-disk format
student/demo.js         # guided student demo (writes, time travel, flush, crash)
csharp-mvm/PerfectDb.cs # C# engine (exact port, thread-safe, same on-disk format)
csharp-mvm/Program.cs   # C# bench (sequential workload)
bench-top/Program.cs    # shootout: PerfectDB vs SQLite vs floor/ceiling + concurrency
crash-test/Program.cs   # kill -9 + fault-injection suite (T1-T15), exit code = failures
csharp-mvm/csharp-mvm.csproj
technical.md            # this file
README.md               # summary + quickstart
```

## 12. Comparison with adopted systems (filed 2026-09-13)

Scope note: this project serves beginner database students, not production. The table
below is the honest version of "why does this toy exist when X exists."

Baseline (this engine): embedded single-node library, segmented WAL + MemTable +
checksummed SSTables + size-tiered merge + manifest-hint, LSN snapshots with time
travel, sync fsync-per-commit plus opt-in shared-fsync group commit, JSONL text files,
single-writer-process, one global lock, no SQL/indexes/compression/replication.
Measured here: ~75 durable writes/s (fsync floor), batched/grouped in the thousands,
~0.5–1M hot point reads/s, ms scans at 4K rows. ~500 lines + student edition.

| Property | PerfectDB | SQLite | RocksDB / LevelDB | LMDB | PostgreSQL | Redis / Valkey |
|---|---|---|---|---|---|---|
| Architecture | LSM-lite (log + runs + merge) | B-tree pages, single file | LSM (leveled), the real thing | COW B+tree over mmap — no log at all | Heap + indexes, server | RAM-first + snapshot/log |
| Durability | WAL seg + fsync before ack; grouped = shared fsync | Journal/WAL + 25 yrs per-OS fsync discipline | WAL + manifests + checkpoints | COW + single commit pointer; no replay | WAL + checkpoints + replication slots | RDB snapshot ≈ our flush; AOF ≈ our WAL (`always`/`everysec`/`no` = our commit/grouped/async debate) |
| Model / Tx | KV + prefix scan; LSN snapshots, time travel | Full SQL, SERIALIZABLE, indexes, triggers | KV + snapshots, column families, merge ops | KV, MVCC readers, single writer | Full SQL, XID MVCC + VACUUM | Rich structures, Lua, single-threaded loop |
| Writes | Sequential appends; full-merge amp grows with data | Random page writes, page-sized amp | Sequential + leveled compaction (bounded amp) | COW page writes, no log amp | Random heap/index writes, bg writer | Memory-speed + background persist |
| Reads | In-RAM maps: fast points while hot; O(files) scans pre-merge | B-tree seeks + page cache; faster scans (measured §7) | Blooms + block cache; built for scale | Among the fastest reads anywhere (mmap) | Planner + indexes + cache; complex queries | Fastest while it fits RAM |
| Corruption | Per-record CRC + file footers, loud fails, 15-test suite | Limited page checksums historically; correctness via extreme testing | Per-block CRC32C, checksums everywhere | Historically none — trusts COW | WAL CRCs; heap checksums opt-in | RDB checksums; AOF truncates corrupt tails like our WAL |
| Recovery | Replay unflushed segments; manifest-hint + scan backstop | Journal rollback / WAL replay, exhaustively fuzzed | Versioned manifest replay | Near-instant (find last root) | Replay from checkpoint, PITR | Reload RDB + replay AOF tail |
| Concurrency | One global lock, threads only | 1 writer + N readers (WAL), multi-process locks | Multi-threaded flush/compact, snapshots | Lock-free readers, 1 writer (process lock) | Full MVCC, many writers, replicas | Single-threaded core, replicas |
| Maturity | Weeks old, docs-as-tests | Most deployed DB on Earth, 100% branch-tested | Meta-scale production | Battle-tested embedded | 35+ years, HA ecosystem | Huge ecosystem, cluster/sentinel |
| Code to read | ~500 lines + student edition | ~150K lines C | ~500K+ lines C++ | ~12K lines C, famously tight | Millions of lines | Large (net + clustering) |

One-line verdicts: vs SQLite — it wins everything real except learnability, readable
files, and free time travel (our write-tie is just a shared fsync floor). Vs RocksDB —
same species; ours is the skeleton it fleshes out, which is exactly the teaching value.
Vs LMDB — the essential contrast (log-first vs copy-on-write); its ~12K lines are the
next-smallest real codebase after ours. Vs PostgreSQL — different universe, but the
rhymes teach: our version chains are their row versions, our compaction their VACUUM,
our group commit theirs. Vs Redis — closest feel (memory-first, snapshot+log); their
`appendfsync` options are the durability-latency tradeoff from §7b.

Time travel specifically (§12.1 below): ours is incidental (surviving versions stay
queryable); Datomic makes it a first-class promise, git demonstrates the UX, event
sourcing is the philosophy (the log IS the database — our LIE 3), Postgres VACUUM and
our own compaction are the anti-feature (both destroy history to reclaim space).

### 12.1 Time travel: how ours works, its limits, and how others do it

Mechanics: every version carries its LSN (`mem` chains newest-last; SSTable entries
store theirs). `_visible()` walks a chain backwards for the newest version with
`lsn <= readLsn`; `get()` checks mem then SSTables newest-file-first with the same
rule. Deletes are tombstones (a version saying "gone at lsn N"), never erasures, so a
key deleted at 50 still answers at 40. `getAt(k, lsn)` is just `get` with an older
clock — three lines, no special machinery.

Worked example: put city=london (lsn 3), paris (lsn 6), flush (snapshot holds paris@6),
del city (lsn 9, tombstone in WAL). `get(city)` → tombstone@9 → null. `getAt(city, 7)`
→ snapshot paris@6. `getAt(city, 4)` → older snapshot or WAL version london@3. History
falls out of never-overwriting; nothing was built "for" time travel.

Limits (stated plainly): the horizon is whatever versions physically survive.
Flush collapses mem chains to latest-per-key; compaction keeps latest-per-key across
files; WAL truncation deletes the log prefix. So time travel reaches back through
surviving older SSTables but dies at compaction — a real temporal store would retain
version chains deliberately, we retain them only incidentally. Also: single-key
snapshot reads only; two `get()` calls can straddle a commit (no pinned-snapshot
multi-read transactions — snapshot isolation is NOT implemented).

How others do it: Datomic (immutable facts + first-class `as-of`/`since`, the gold
standard); git (every commit a snapshot, checkout = `getAt` — same UX shape as ours);
event sourcing/CQRS (state as fold over the log — our LIE 3 as an architecture);
Postgres (no built-in travel; history actively destroyed by VACUUM — the exact
opposite trade); RocksDB (snapshots pin reads, but no historical queries);
Delta Lake (table versioning + `TIMESTAMP AS OF` for analytics).
Student bridge: TRY 5 in `student/` (audit log via `getAt`) → Datomic's model → event
sourcing as career-relevant pattern.
