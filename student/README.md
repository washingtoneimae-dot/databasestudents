# PerfectDB — Student Edition

One file. Zero dependencies. Twenty minutes. A database you can hold in your head.

## Run it

```
cd student
node demo.js        # narrated tour: writes, time travel, flush, simulated crash
```

`demo.js` uses a temp directory (outside OneDrive, so no sync locks) and exits
nonzero if any promise breaks.

## Read it

`perfectdb-student.js` is written to be read top to bottom. Follow the READING ORDER
note at the top: checksums → boot → the write path → reads → checkpointing. Every
weird-looking line answers one of the Three Lies (also at the top of the file):

| OS truth | Where in the code |
|---|---|
| `write()` is just page cache; `fsync()` is the real save | `commit()`, step 2 |
| Never overwrite: append + `rename()` a `.tmp` file | `commit()` step 2, `flush()` |
| The log is the truth; snapshots are a cache | `open()` — delete `sstables/` and see |
| Torn bytes must fail loudly, never silently | `checksum()`, footer check in `open()` |
| History is free if you never overwrite | `getAt()` — three lines |

## Break it (the actual curriculum)

The TRY prompts at the bottom of `perfectdb-student.js` are the course. In order:
1. Delete `sstables/`, reopen — feel LIE 3 die.
2. Comment out `fsyncSync`, rerun 20× — feel why benchmarks lie about durability.
3. Corrupt vs truncate `wal.log` — two detectors, two lessons.
4. Write `compact()` yourself (~30 lines; reference in `../mvm/perfect_db_mvm.js`).
5. Audit-log exercise with `getAt()`.

## Same files as the real thing

The student engine reads and writes the **identical on-disk format** as the full
engines (`mvm/`, `csharp-mvm/`). Point it at real bench data and inspect it. The
things it skips (manifest, WAL rotation, group commit) are each labeled with what
they cost and where to read the real version. When you're done here,
`../technical.md` is the next textbook — it documents every tradeoff with numbers.
