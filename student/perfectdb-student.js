// ============================================================================
// PerfectDB — STUDENT EDITION. One file, zero dependencies, plain Node.js.
// Read it top to bottom; it is a short story about how databases keep promises.
//
// THE THREE LIES every beginner believes (and what this file does instead):
//
//   LIE 1: "write() means saved."
//     Truth: write() only hands bytes to the KERNEL's page cache (RAM). Pull the
//     plug and it evaporates. Only fsync() makes the drive controller confirm
//     ink-on-paper persistence. See commit(), step 2. That wait is physics, not code.
//
//   LIE 2: "to update a file, overwrite it."
//     Truth: a crash halfway through an overwrite leaves a TORN file — half old,
//     half new, fully useless. So we NEVER overwrite: every change is APPENDED to
//     a log (the WAL), and bulk copies are written to a .tmp file then rename()d
//     into place. rename() swaps one directory pointer: readers see the whole old
//     file or the whole new file, never a mixture. See flush().
//
//   LIE 3: "the database file is the truth."
//     Truth: the truth is the LOG. The sorted files are just a cache of what the
//     log already said. Delete every .jsonl file here and reopen: all committed
//     data comes back from wal.log alone. Try it — that's Exercise 1 below.
//
// READING ORDER (about 20 minutes):
//   1. checksum()              — how we detect torn bytes (2 min)
//   2. constructor + open()    — boot = load files, replay log (5 min)
//   3. begin/put/commit        — the write path, the heart of the story (5 min)
//   4. get/scan/getAt        — reads, plus free time travel (3 min)
//   5. flush()/close()         — checkpointing: turning log into sorted files (5 min)
//
// WHAT'S DELIBERATELY MISSING (training wheels removed — each is a lesson):
//   - manifest     : we rediscover files by scanning the directory every open.
//                    Slower at scale, but there is NOTHING to go stale. The real
//                    engines add MANIFEST.json as a hint (see ../technical.md:7f).
//   - WAL rotation : one wal.log forever, truncated on flush. Real engines split
//                    it into segments so recovery stays bounded. Same tradeoff.
//   - group commit : every commit pays its own fsync (~14ms here). Sharing one
//                    fsync across concurrent commits is 40x faster — but needs
//                    threads. Read csharp-mvm/PerfectDb.cs CommitGrouped one day.
//   - compaction   : sorted files pile up; reads check each one. Merging them is
//                    YOUR exercise (TRY 4). Reference: compact() in mvm/perfect_db_mvm.js.
//
// FILE FORMATS (identical to the real engines — inspect their data with this file):
//   wal.log          one {"lsn","tx","op","k","v","crc"} per line, crc over lsn|tx|op|k|v
//   sstable-NNNN     sorted {"lsn","k","v","del","crc"} lines + footer {count, sha256}
// ============================================================================

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// A checksum is a fingerprint. Change even one byte of the input and the
// fingerprint changes completely — so a torn write (half a line on disk after
// a crash) can never pass as good data. It fails LOUDLY instead of lying.
function checksum(s) {
  return crypto.createHash('sha256').update(s).digest('hex').slice(0, 16);
}

class StudentDB {
  constructor(dir) {
    this.dir = dir;
    this.walPath = path.join(dir, 'wal.log');
    this.sstableDir = path.join(dir, 'sstables');
    // mem: the whiteboard. key -> list of versions, newest last.
    // EVERYTHING the database knows lives here after open(); the files on
    // disk are just how the whiteboard survives a reboot.
    this.mem = new Map();
    this.sstables = []; // sorted, immutable snapshots of older whiteboards
    this.lsn = 0;       // the clock: every change gets the next number, forever
    this.txId = 0;
    this.walFd = null;
  }

  // BOOT: rebuild the whiteboard from disk. Two sources, oldest first:
  // snapshots, then the log of everything since the last snapshot.
  open() {
    fs.mkdirSync(this.sstableDir, { recursive: true });
    const files = fs.existsSync(this.sstableDir)
      ? fs.readdirSync(this.sstableDir).filter(f => f.endsWith('.jsonl')).sort() : [];
    for (const f of files) {
      const map = new Map();
      const lines = fs.readFileSync(path.join(this.sstableDir, f), 'utf8')
        .split('\n').filter(l => l.trim() !== '');
      const last = lines[lines.length - 1];
      const hasFooter = last && last.includes('__perfectdb_footer__');
      const records = hasFooter ? lines.slice(0, -1) : lines;
      if (hasFooter) {
        const foot = JSON.parse(last);
        const section = records.map(l => l + '\n').join('');
        if (records.length !== foot.count ||
            crypto.createHash('sha256').update(section).digest('hex') !== foot.crc)
          throw new Error(`SSTable ${f} failed verification (truncated or rotted?)`);
      }
      for (const line of records) {
        const r = JSON.parse(line);
        // New-format lines carry their own fingerprint; old ones load as-is.
        if (r.crc != null && checksum(`${r.lsn}|${r.k}|${r.v ?? ''}|${r.del}`) !== r.crc)
          throw new Error(`SSTable ${f} record failed checksum (key=${r.k})`);
        map.set(r.k, { lsn: r.lsn, value: r.v, deleted: !!r.del });
        if (r.lsn > this.lsn) this.lsn = r.lsn;
      }
      this.sstables.push(map);
    }
    // Now the log: every committed change, in order. A crash mid-write can
    // leave a TORN FINAL LINE (half a JSON object) — JSON.parse throws, and
    // that is our torn-write detector: stop there, everything before is intact.
    if (fs.existsSync(this.walPath)) {
      for (const line of fs.readFileSync(this.walPath, 'utf8').split('\n').filter(Boolean)) {
        let r;
        try { r = JSON.parse(line); }
        catch { break; } // torn tail: the crash happened HERE, keep all of before
        if (checksum(`${r.lsn}|${r.tx}|${r.op}|${r.k}|${r.v ?? ''}`) !== r.crc) continue; // rot: skip
        if (r.lsn > this.lsn) this.lsn = r.lsn;
        this._remember(r.k, r.v, r.op === 'del', r.lsn);
      }
    }
    this.walFd = fs.openSync(this.walPath, 'a');
  }

  _remember(k, v, deleted, lsn) {
    if (!this.mem.has(k)) this.mem.set(k, []);
    this.mem.get(k).push({ lsn, value: v, deleted });
  }

  // Transactions are just a scratch buffer. The magic is ALL in commit():
  // a whole transaction becomes durable in ONE fsync, or none of it does.
  begin() { return { id: ++this.txId, writes: new Map() }; }
  put(tx, k, v) { tx.writes.set(k, { v, deleted: false }); }
  del(tx, k) { tx.writes.set(k, { v: null, deleted: true }); }

  commit(tx) {
    if (tx.writes.size === 0) return 0;
    // Step 1: give every change the next clock number + fingerprint it.
    let batch = '';
    const ops = [];
    for (const [k, w] of tx.writes) {
      const lsn = ++this.lsn;
      const op = w.deleted ? 'del' : 'put';
      const rec = { lsn, tx: tx.id, op, k, v: w.v,
        crc: checksum(`${lsn}|${tx.id}|${op}|${k}|${w.v ?? ''}`) };
      batch += JSON.stringify(rec) + '\n';
      ops.push([k, w, lsn]);
    }
    // Step 2: append to the log and fsync BEFORE telling anyone. If we crash
    // AFTER this line, recovery replays it: committed means durable. If we
    // crash BEFORE it, the transaction never happened: atomicity, for free.
    fs.writeSync(this.walFd, batch);
    fs.fsyncSync(this.walFd);
    // Step 3: only now update the whiteboard (readers never see un-durable data).
    for (const [k, w, lsn] of ops) this._remember(k, w.v, w.deleted, lsn);
    // Step 4: whiteboard too big? snapshot it to disk (see flush).
    if (this.mem.size >= 2000) this.flush();
    return ops.length;
  }

  // Reads walk history backwards: the newest version you are ALLOWED to see wins.
  _visible(versions, readLsn) {
    for (let i = versions.length - 1; i >= 0; i--)
      if (versions[i].lsn <= readLsn) return versions[i];
    return null;
  }

  get(k, readLsn = this.lsn) {
    const mv = this.mem.get(k);
    if (mv) {
      const v = this._visible(mv, readLsn);
      if (v) return v.deleted ? null : v.value;
    }
    for (let i = this.sstables.length - 1; i >= 0; i--) {
      const e = this.sstables[i].get(k);
      if (e && e.lsn <= readLsn) return e.deleted ? null : e.value;
    }
    return null;
  }

  scan(start = '', end = '\uffff') {
    const keys = new Set();
    for (const s of this.sstables) for (const k of s.keys()) if (k >= start && k < end) keys.add(k);
    for (const k of this.mem.keys()) if (k >= start && k < end) keys.add(k);
    return [...keys].sort().map(k => [k, this.get(k)]).filter(([, v]) => v !== null);
  }

  // TIME TRAVEL, free of charge: every version carries its clock number, so
  // "what did k hold at lsn 3?" is just a read with an older clock. This is why
  // real databases never overwrite in place — history IS the feature.
  getAt(k, lsn) { return this.get(k, lsn); }

  // CHECKPOINT: copy the whiteboard's latest version per key into a new,
  // sorted, immutable snapshot file — then throw away the log prefix it covers.
  // The .tmp + rename dance is LIE 2's antidote: a crash leaves either the old
  // directory entry or the complete new file, never a half-written snapshot.
  flush() {
    if (this.mem.size === 0) return null;
    const latest = new Map();
    for (const [k, vers] of this.mem) latest.set(k, vers[vers.length - 1]);
    const keys = [...latest.keys()].sort();
    let section = '';
    for (const k of keys) {
      const v = latest.get(k);
      const del = v.deleted ? 1 : 0;
      section += JSON.stringify({ lsn: v.lsn, k, v: v.value, del,
        crc: checksum(`${v.lsn}|${k}|${v.value ?? ''}|${del}`) }) + '\n';
    }
    const n = this.sstables.length;
    const fp = path.join(this.sstableDir, `sstable-${String(n).padStart(4, '0')}.jsonl`);
    const fd = fs.openSync(fp + '.tmp', 'w');
    const content = section + JSON.stringify({ __perfectdb_footer__: true, count: keys.length,
      crc: crypto.createHash('sha256').update(section).digest('hex') }) + '\n';
    fs.writeSync(fd, content);
    fs.fsyncSync(fd); // the NEW file's bytes durable BEFORE it becomes visible
    fs.closeSync(fd);
    fs.renameSync(fp + '.tmp', fp); // atomic publish
    const map = new Map([...latest].map(([k, v]) => [k, { lsn: v.lsn, value: v.value, deleted: v.deleted }]));
    this.sstables.push(map);
    this.mem.clear();
    // The log prefix up to here is now redundant: every version lives in a snapshot.
    fs.closeSync(this.walFd);
    fs.writeFileSync(this.walPath, '');
    this.walFd = fs.openSync(this.walPath, 'a');
    return fp;
  }

  close() {
    if (this.walFd !== null) { try { fs.fsyncSync(this.walFd); fs.closeSync(this.walFd); } catch {} this.walFd = null; }
  }
}

module.exports = { StudentDB, checksum };

// ----------------------------------------------------------------------------
// TRY IT YOURSELF (no test harness — a terminal and curiosity):
//   TRY 1 (LIE 3): after the demo, DELETE the whole sstables/ dir, reopen.
//           Everything committed before the last flush comes back from wal.log.
//   TRY 2 (LIE 1): comment out the fsyncSync in commit(), rerun the demo 20x.
//           (Don't pull the plug; just notice nothing breaks — fsync only matters
//           when the OS dies. Then think about what that MEANS for benchmarks.)
//   TRY 3 (LIE 2): open wal.log, corrupt one character in the MIDDLE of a line,
//           reopen. The line is skipped, the rest survives. Now truncate the file
//           mid-line instead: the tail is dropped. Two detectors, two lessons.
//   TRY 4 (the missing piece): write compact() — merge this.sstables into ONE
//           sorted file, newest version per key wins, keep tombstones. ~30 lines.
//           Reference solution: compact() in ../mvm/perfect_db_mvm.js.
//   TRY 5 (time travel): debit/credit one key across 5 transactions, then getAt()
//           every lsn. You just built an audit log. Banks pay well for these.
// ----------------------------------------------------------------------------
