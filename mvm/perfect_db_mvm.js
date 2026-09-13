// PerfectDB MVM - Minimal Viable Mechanism
// Goal: prove the core thesis: log-first + memory + sorted files = safe + fast.
// Single-file, no deps, Node.js only.
// Features: WAL + fsync, MemTable, SSTable flush, MVCC-lite (lsn versions), tx, recovery, time-travel read.

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

function fsyncDirSync(dir) {
  try {
    const fd = fs.openSync(dir, 'r');
    try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  } catch {} // best effort: Windows throws on dir fsync
}

function writeFileDurableSync(p, s) {
  const fd = fs.openSync(p, 'w');
  try { fs.writeSync(fd, s); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
}

function sstableRecordCrc(lsn, k, v, del) {
  return checksum(`${lsn}|${k}|${v ?? ''}|${del}`);
}

// Canonical bytes, must match C# BuildSstableFile exactly:
// records in order as {"lsn","k","v","del","crc"}, then footer with count + SHA256.
function buildSstableFile(rows) {
  let section = '';
  let n = 0;
  for (const [k, lsn, v, deleted] of rows) {
    const del = deleted ? 1 : 0;
    section += JSON.stringify({ lsn, k, v, del, crc: sstableRecordCrc(lsn, k, v, del) }) + '\n';
    n++;
  }
  const fileCrc = crypto.createHash('sha256').update(section).digest('hex');
  return section + JSON.stringify({ __perfectdb_footer__: true, count: n, crc: fileCrc }) + '\n';
}

// Throws on ANY anomaly: torn JSON, record crc mismatch, footer count/hash mismatch.
// crc-carrying records without a footer = torn new-format file -> throw.
// Only crc-less records with no footer (pre-checksum legacy) load unverified.
function verifySstableFile(fullPath) {
  let lines;
  try {
    lines = fs.readFileSync(fullPath, 'utf8').split('\n').filter(l => l.trim() !== '');
  } catch (e) { throw new Error(`SSTable ${fullPath} unreadable: ${e.message}`); }
  let end = lines.length, expectFileCrc = null, expectCount = -1;
  if (lines.length > 0 && lines[lines.length - 1].includes('__perfectdb_footer__')) {
    let f;
    try { f = JSON.parse(lines[lines.length - 1]); }
    catch (e) { throw new Error(`SSTable ${fullPath} torn footer`); }
    expectFileCrc = f.crc; expectCount = f.count; end = lines.length - 1;
  }
  const records = [];
  let seenCrc = false, section = '';
  for (let i = 0; i < end; i++) {
    let r;
    try { r = JSON.parse(lines[i]); }
    catch (e) { throw new Error(`SSTable ${fullPath} torn record at line ${i}`); }
    if (r.crc != null) {
      seenCrc = true;
      if (sstableRecordCrc(r.lsn, r.k, r.v, r.del) !== r.crc)
        throw new Error(`SSTable ${fullPath} record crc mismatch at line ${i} (key=${r.k})`);
    }
    records.push(r);
    section += lines[i] + '\n';
  }
  if (expectFileCrc != null) {
    if (records.length !== expectCount)
      throw new Error(`SSTable ${fullPath} footer count ${expectCount} != ${records.length} records`);
    if (crypto.createHash('sha256').update(section).digest('hex') !== expectFileCrc)
      throw new Error(`SSTable ${fullPath} file hash mismatch (truncated or rotted)`);
  } else if (seenCrc) {
    throw new Error(`SSTable ${fullPath} has checksummed records but no footer (truncated?)`);
  }
  return records;
}

function checksum(s) {
  return crypto.createHash('sha256').update(s).digest('hex').slice(0, 16);
}

class PerfectDB {
  constructor(dir, opts = {}) {
    this.dir = dir;
    this.walPath = path.join(dir, 'wal.log'); // legacy pre-segment file (upgrade replay only)
    this.walDir = path.join(dir, 'wal');
    this.manifestPath = path.join(dir, 'MANIFEST.json');
    this.sstableDir = path.join(dir, 'sstables');
    this.flushThreshold = opts.flushThreshold || 2000;
    this.walSegmentBytes = (opts.walSegmentBytes != null) ? opts.walSegmentBytes : (1 << 20);
    this.mem = new Map(); // key -> array of {lsn, value, deleted} newest last
    this.sstables = []; // array of {id, map: Map(key -> {lsn,value,deleted}), sortedKeys: []}
    this.lsn = 0;
    this.txId = 0;
    this.walFd = null;
    this._gPending = []; // group-commit rendezvous
    this._gFlushing = false;
    this._gBatches = 0;
    this._gGroupedCommits = 0;
    this.groupBacklogGraceMs = 0; // backlog grace; Node's loop bursts need none (see technical.md:7b)
    this.autoCompactThreshold = 4;
    this._nextSstableId = 0;
    this._walActiveId = 0;
    this._walActiveBytes = 0;
    this._walMinId = 0;
    this._nextWalId = 0;
  }

  open() {
    fs.mkdirSync(this.sstableDir, { recursive: true });
    // load sstables (sorted by id asc = oldest first)
    const files = fs.existsSync(this.sstableDir) ? fs.readdirSync(this.sstableDir).filter(f => f.endsWith('.jsonl')).sort() : [];
    for (const f of files) {
      const id = parseInt(f.split('-')[1], 10);
      const full = path.join(this.sstableDir, f);
      const map = new Map();
      for (const r of verifySstableFile(full)) { // throws loudly on rot/truncation
        map.set(r.k, { lsn: r.lsn, value: r.v, deleted: !!r.del });
        if (r.lsn > this.lsn) this.lsn = r.lsn;
      }
      this.sstables.push({ id, map, sortedKeys: [...map.keys()].sort() });
    }
    this._nextSstableId = 0;
    for (const s of this.sstables) if (s.id >= this._nextSstableId) this._nextSstableId = s.id + 1;
    fs.mkdirSync(this.walDir, { recursive: true });
    fsyncDirSync(this.dir);
    // Manifest is a hint; directory scan is the backstop (same rules as C# port).
    const man = this.readManifest();
    const wanted = man ? new Set(man.sstableIds) : null;
    const orphans = [];
    if (wanted) {
      for (const s of this.sstables) if (!wanted.has(s.id)) orphans.push(s.id);
      for (const id of wanted)
        if (!this.sstables.some(s => s.id === id))
          throw new Error(`MANIFEST lists sstable-${String(id).padStart(4, '0')}.jsonl but it is missing on disk (data loss?)`);
    }
    if (fs.existsSync(this.walPath)) this.replayWalFile(this.walPath, true); // legacy upgrade path
    const segIds = this.listWalSegmentIds();
    segIds.forEach((id, i) => this.replayWalFile(this.walSegmentPath(id), i === segIds.length - 1));
    if (man && man.lsn > this.lsn)
      throw new Error(`MANIFEST lsn ${man.lsn} exceeds recovered ${this.lsn} (newest files lost?)`);
    this._nextSstableId = man ? man.nextSstableId : 0;
    for (const s of this.sstables) if (s.id >= this._nextSstableId) this._nextSstableId = s.id + 1;
    this._nextWalId = man ? man.nextWalId : 0;
    for (const id of segIds) if (id >= this._nextWalId) this._nextWalId = id + 1;
    this._walMinId = man ? man.walMinId : 0;
    this.openActiveSegment(this._nextWalId++);
    this.sstables = this.sstables.filter(s => !orphans.includes(s.id));
    for (const id of orphans) {
      console.error(`MANIFEST: removing unlisted sstable-${String(id).padStart(4, '0')}.jsonl (covered by WAL)`);
      try { fs.unlinkSync(path.join(this.sstableDir, `sstable-${String(id).padStart(4, '0')}.jsonl`)); } catch {}
    }
    if (orphans.length > 0) fsyncDirSync(this.sstableDir);
    if (segIds.length > 0) this._walMinId = segIds[0];
    // Rewrite only when state changed: ids/lsn recompute as max(manifest, disk) anyway.
    if (!man || orphans.length > 0) this.writeManifest();
  }

  walSegmentPath(id) { return path.join(this.walDir, `wal-${String(id).padStart(6, '0')}.log`); }

  listWalSegmentIds() {
    if (!fs.existsSync(this.walDir)) return [];
    return fs.readdirSync(this.walDir).filter(f => f.startsWith('wal-') && f.endsWith('.log')).sort()
      .map(f => parseInt(f.split('-')[1], 10)).filter(n => !isNaN(n));
  }

  openActiveSegment(id) {
    const p = this.walSegmentPath(id);
    this.walFd = fs.openSync(p, 'a');
    this._walActiveId = id;
    this._walActiveBytes = fs.existsSync(p) ? fs.statSync(p).size : 0;
  }

  createActiveSegmentFile(id) {
    const p = this.walSegmentPath(id);
    if (!fs.existsSync(p)) writeFileDurableSync(p, '');
    fsyncDirSync(this.walDir);
  }

  rotateWal() {
    fs.fsyncSync(this.walFd);
    fs.closeSync(this.walFd);
    this.walFd = null;
    const id = this._nextWalId++;
    this.createActiveSegmentFile(id);
    this.openActiveSegment(id);
  }

  replayWalFile(fullPath, isLast) {
    let lineNo = 0;
    for (const line of fs.readFileSync(fullPath, 'utf8').split('\n')) {
      lineNo++;
      if (line.trim() === '') continue;
      let r;
      try { r = JSON.parse(line); }
      catch (e) {
        if (isLast) { console.error('WAL torn write at tail, truncated: ' + e.message); break; }
        throw new Error(`WAL ${fullPath} torn record at line ${lineNo} in sealed segment`);
      }
      if (r == null) {
        if (isLast) break;
        throw new Error(`WAL ${fullPath} null record at line ${lineNo} in sealed segment`);
      }
      const payload = `${r.lsn}|${r.tx}|${r.op}|${r.k}|${r.v ?? ''}`;
      if (checksum(payload) !== r.crc) { console.error(`WAL corrupt at lsn=${r.lsn}, skipped`); continue; }
      if (r.lsn > this.lsn) this.lsn = r.lsn;
      this._applyToMem(r.k, r.v, r.op === 'del', r.lsn);
    }
  }

  writeManifest() {
    const man = { format: 1, lsn: this.lsn, nextSstableId: this._nextSstableId,
      nextWalId: this._nextWalId, walMinId: this._walMinId,
      sstableIds: this.sstables.map(s => s.id).sort((a, b) => a - b) };
    writeFileDurableSync(this.manifestPath + '.tmp', JSON.stringify(man));
    fs.renameSync(this.manifestPath + '.tmp', this.manifestPath);
    fsyncDirSync(this.dir);
  }

  readManifest() {
    try {
      const m = JSON.parse(fs.readFileSync(this.manifestPath, 'utf8'));
      return (m && m.format === 1) ? m : null;
    } catch { return null; } // torn/missing -> discovery mode
  }

  _applyToMem(k, v, deleted, lsn) {
    if (!this.mem.has(k)) this.mem.set(k, []);
    this.mem.get(k).push({ lsn, value: v, deleted });
  }

  begin() { return { id: ++this.txId, writes: new Map() }; }

  put(tx, k, v) { tx.writes.set(k, { v, deleted: false }); }
  del(tx, k) { tx.writes.set(k, { v: null, deleted: true }); }

  commit(tx) {
    // 1. assign LSNs + build WAL batch
    let batch = '';
    const ops = [];
    for (const [k, w] of tx.writes) {
      const lsn = ++this.lsn;
      const op = w.deleted ? 'del' : 'put';
      const payload = `${lsn}|${tx.id}|${op}|${k}|${w.v ?? ''}`;
      const crc = checksum(payload);
      const rec = { lsn, tx: tx.id, op, k, v: w.v, crc };
      batch += JSON.stringify(rec) + '\n';
      ops.push({ k, ...w, lsn });
    }
    if (!batch) return 0;
    // 2. sequential append + fsync BEFORE ack (durability)
    fs.writeSync(this.walFd, batch);
    fs.fsyncSync(this.walFd);
    this._walActiveBytes += Buffer.byteLength(batch);
    // 3. apply to memtable
    for (const o of ops) this._applyToMem(o.k, o.v, o.deleted, o.lsn);
    // 4. auto-flush, else rotate a full segment
    if (this.mem.size >= this.flushThreshold) this.flush();
    else if (this._walActiveBytes >= this.walSegmentBytes) this.rotateWal();
    return ops.length;
  }

  // Durable group commit: concurrent commitGrouped calls share ONE fsync.
  // Same guarantee as commit() — acked = fsynced. Lone calls pay the window tax.
  commitGrouped(tx, windowMs = 10) {
    if (tx.writes.size === 0) return Promise.resolve(0);
    return new Promise((resolve, reject) => {
      this._gPending.push({ tx, resolve, reject });
      if (!this._gFlushing) { this._gFlushing = true; this._runGroupFlusher(windowMs); }
    });
  }

  groupStats() {
    return { batches: this._gBatches, commits: this._gGroupedCommits,
      avgBatch: this._gBatches === 0 ? 0 : this._gGroupedCommits / this._gBatches };
  }

  _runGroupFlusher(windowMs) {
    const step = (first) => {
      setTimeout(() => {
        if (this._gPending.length === 0) { this._gFlushing = false; return; }
        const batch = this._gPending; this._gPending = [];
        try {
          let b = '';
          const ops = [];
          const counts = [];
          for (const { tx } of batch) {
            let n = 0;
            for (const [k, w] of tx.writes) {
              const lsn = ++this.lsn;
              const op = w.deleted ? 'del' : 'put';
              const payload = `${lsn}|${tx.id}|${op}|${k}|${w.v ?? ''}`;
              const crc = checksum(payload);
              b += JSON.stringify({ lsn, tx: tx.id, op, k, v: w.v, crc }) + '\n';
              ops.push([k, w, lsn]); n++;
            }
            counts.push(n);
          }
          fs.writeSync(this.walFd, b);
          fs.fsyncSync(this.walFd); // single fsync BEFORE acking the whole batch
          this._walActiveBytes += Buffer.byteLength(b);
          for (const [k, w, lsn] of ops) this._applyToMem(k, w.v, w.deleted, lsn);
          if (this.mem.size >= this.flushThreshold) this.flush();
          else if (this._walActiveBytes >= this.walSegmentBytes) this.rotateWal();
          this._gBatches++; this._gGroupedCommits += batch.length;
          batch.forEach((r, i) => r.resolve(counts[i]));
        } catch (e) {
          batch.forEach(r => r.reject(e));
        }
        step(false); // backlog flushes after a grace wait (0 = immediately)
      }, first ? windowMs : this.groupBacklogGraceMs);
    };
    step(true);
  }

  _visible(versions, readLsn) {
    // newest version with lsn <= readLsn
    for (let i = versions.length - 1; i >= 0; i--) {
      if (versions[i].lsn <= readLsn) return versions[i];
    }
    return null;
  }

  get(k, readLsn = this.lsn) {
    const mv = this.mem.get(k);
    if (mv) {
      const v = this._visible(mv, readLsn);
      if (v) return v.deleted ? null : v.value;
      // else fall through to sstables (older version)
    }
    for (let i = this.sstables.length - 1; i >= 0; i--) {
      const e = this.sstables[i].map.get(k);
      if (e && e.lsn <= readLsn) return e.deleted ? null : e.value;
    }
    return null;
  }

  scan(start = '', end = '\uffff', readLsn = this.lsn, limit = 100000) {
    const keys = new Set();
    for (const s of this.sstables) for (const k of s.sortedKeys) if (k >= start && k < end) keys.add(k);
    for (const k of this.mem.keys()) if (k >= start && k < end) keys.add(k);
    const sorted = [...keys].sort();
    const out = [];
    for (const k of sorted) {
      const v = this.get(k, readLsn);
      if (v !== null) out.push([k, v]);
      if (out.length >= limit) break;
    }
    return out;
  }

  flush() {
    if (this.mem.size === 0) return null;
    const id = this._nextSstableId++;
    const latest = new Map();
    for (const [k, vers] of this.mem) {
      const v = vers[vers.length - 1];
      latest.set(k, v);
    }
    const sortedKeys = [...latest.keys()].sort();
    const fp = path.join(this.sstableDir, `sstable-${String(id).padStart(4, '0')}.jsonl`);
    let buf = buildSstableFile(sortedKeys.map(k => { const v = latest.get(k); return [k, v.lsn, v.value, v.deleted]; }));
    writeFileDurableSync(fp + '.tmp', buf);
    fs.renameSync(fp + '.tmp', fp); // atomic publish
    fsyncDirSync(this.sstableDir);
    this.sstables.push({ id, map: new Map([...latest].map(([k, v]) => [k, { lsn: v.lsn, value: v.value, deleted: v.deleted }])), sortedKeys });
    this.mem.clear();
    this.maybeCompact(); // writes its own manifest first (manifest-before-deletes)
    const freshWal = this._nextWalId++;
    this.createActiveSegmentFile(freshWal);
    this._walMinId = freshWal;
    this.writeManifest(); // manifest BEFORE deletes: old segments droppable only now
    // Close the active handle FIRST: on Windows an open file cannot always be deleted.
    fs.fsyncSync(this.walFd);
    fs.closeSync(this.walFd);
    this.walFd = null;
    for (const id of this.listWalSegmentIds())
      if (id !== freshWal) { try { fs.unlinkSync(this.walSegmentPath(id)); } catch {} }
    if (fs.existsSync(this.walPath)) { try { fs.unlinkSync(this.walPath); } catch {} } // legacy migration
    fsyncDirSync(this.walDir); fsyncDirSync(this.dir);
    this.openActiveSegment(freshWal);
    return fp;
  }

  // Size-tiered merge: same crash-safe ordering as the C# port
  // (fsync file + rename + fsync dir BEFORE deleting inputs; dup LSNs self-heal).
  compact() {
    if (this.sstables.length < 2) return null;
    const ordered = [...this.sstables].sort((a, b) => a.id - b.id);
    const merged = new Map();
    for (const s of ordered) for (const [k, v] of s.map) merged.set(k, v);
    const sortedKeys = [...merged.keys()].sort();
    const id = this._nextSstableId++;
    const fp = path.join(this.sstableDir, `sstable-${String(id).padStart(4, '0')}.jsonl`);
    // Re-verify inputs from disk first: never merge (and thereby bless) corrupt data.
    for (const s of ordered) verifySstableFile(path.join(this.sstableDir, `sstable-${String(s.id).padStart(4, '0')}.jsonl`));
    let buf = buildSstableFile(sortedKeys.map(k => { const v = merged.get(k); return [k, v.lsn, v.value, v.deleted]; }));
    writeFileDurableSync(fp + '.tmp', buf);
    fs.renameSync(fp + '.tmp', fp);
    fsyncDirSync(this.sstableDir);
    this.sstables = [{ id, map: new Map([...merged].map(([k, v]) => [k, { lsn: v.lsn, value: v.value, deleted: v.deleted }])), sortedKeys }];
    this.writeManifest(); // manifest BEFORE deletes: inputs droppable only now
    for (const s of ordered) {
      try { fs.unlinkSync(path.join(this.sstableDir, `sstable-${String(s.id).padStart(4, '0')}.jsonl`)); } catch {}
    }
    fsyncDirSync(this.sstableDir);
    return fp;
  }

  maybeCompact() {
    if (this.autoCompactThreshold > 0 && this.sstables.length >= this.autoCompactThreshold) this.compact();
  }

  close() {
    if (this.walFd !== null) { try { fs.fsyncSync(this.walFd); fs.closeSync(this.walFd); } catch {} this.walFd = null; }
  }
}

module.exports = { PerfectDB };
