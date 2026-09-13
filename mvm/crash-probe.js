// Node crash probe: torn tail, bad-crc middle, stale flush .tmp, compaction correctness.
const fs = require('fs');
const os = require('os');
const path = require('path');
const { PerfectDB } = require('./perfect_db_mvm');

const ROOT = fs.mkdtempSync(path.join(os.tmpdir(), 'perfectdb-node-')); // outside OneDrive
function clean(d) {
  for (let a = 0; ; a++) {
    try { if (fs.existsSync(d)) fs.rmSync(d, { recursive: true, force: true }); break; }
    catch (e) { if (a >= 60) throw e; Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 500); }
  }
}
let fails = 0;
function check(name, f) {
  try { const ok = f(); console.log(`[${ok ? 'PASS' : 'FAIL'}] ${name}`); if (!ok) fails++; }
  catch (e) { console.log(`[FAIL] ${name}: ${e.message}`); fails++; }
}
const K = i => `k${String(i).padStart(6, '0')}`;
const V = i => `v${i}-` + 'x'.repeat(50);
const C = i => `c${String(i).padStart(4, '0')}`;

check('N1 torn WAL tail truncated', () => {
  const d = path.join(ROOT, 'n1'); clean(d);
  const db = new PerfectDB(d, { flushThreshold: 1000000 }); db.open();
  for (let i = 0; i < 200; i++) { const tx = db.begin(); db.put(tx, K(i), V(i)); db.commit(tx); }
  db.close();
  fs.appendFileSync(path.join(d, 'wal.log'), '\n{"lsn":99999,"tx');
  const db2 = new PerfectDB(d, { flushThreshold: 1000000 }); db2.open();
  const n = db2.scan('k000000', 'k999999').length; db2.close(); return n === 200;
});

check('N2 stale flush .tmp ignored', () => {
  const d = path.join(ROOT, 'n2'); clean(d);
  const db = new PerfectDB(d, { flushThreshold: 100 }); db.autoCompactThreshold = 0; db.open();
  for (let i = 0; i < 300; i++) { const tx = db.begin(); db.put(tx, K(i), V(i)); db.commit(tx); }
  db.close();
  fs.writeFileSync(path.join(d, 'sstables', 'sstable-9999.jsonl.tmp'), 'GARBAGE\n');
  const db2 = new PerfectDB(d, { flushThreshold: 100 }); db2.autoCompactThreshold = 0; db2.open();
  const n = db2.scan('k000000', 'k999999').length; db2.close(); return n === 300;
});

check('N3 compact latest-wins + tombstones + idempotent', () => {
  const d = path.join(ROOT, 'n3'); clean(d);
  const db = new PerfectDB(d, { flushThreshold: 100 }); db.autoCompactThreshold = 0; db.open();
  { const tx = db.begin(); for (let i = 0; i < 100; i++) db.put(tx, C(i), `A${i}`); db.commit(tx); }
  { const tx = db.begin(); for (let i = 50; i < 150; i++) db.put(tx, C(i), `B${i}`); db.commit(tx); }
  { const tx = db.begin(); for (let i = 100; i < 200; i++) db.put(tx, C(i), `C${i}`); for (let i = 0; i < 50; i++) db.del(tx, C(i)); db.commit(tx); }
  db.flush();
  if (db.sstables.length !== 3) return false;
  if (db.compact() === null || db.sstables.length !== 1) return false;
  const ok = db.scan('c', 'd').length === 150
    && db.get(C(0)) === null && db.get(C(49)) === null
    && db.get(C(50)) === 'B50' && db.get(C(199)) === 'C199';
  if (db.compact() !== null || db.sstables.length !== 1) return false;
  db.close();
  const db2 = new PerfectDB(d, { flushThreshold: 100 }); db2.autoCompactThreshold = 0; db2.open();
  const n = db2.scan('c', 'd').length; db2.close();
  return ok && n === 150;
});

check('N4 bit-rot fails loudly', () => {
  const d = path.join(ROOT, 'n4'); clean(d);
  const db = new PerfectDB(d, { flushThreshold: 100 }); db.autoCompactThreshold = 0; db.open();
  for (let i = 0; i < 250; i++) { const tx = db.begin(); db.put(tx, K(i), V(i)); db.commit(tx); }
  db.close();
  const f = path.join(d, 'sstables', 'sstable-0000.jsonl');
  const lines = fs.readFileSync(f, 'utf8').split('\n');
  lines[10] = rot1(lines[10]);
  fs.writeFileSync(f, lines.join('\n'));
  try { const bad = new PerfectDB(d, { flushThreshold: 100 }); bad.open(); return false; }
  catch (e) { return /crc|mismatch/i.test(e.message); }
});

check('N5 truncated tail fails loudly', () => {
  const d = path.join(ROOT, 'n5'); clean(d);
  const db = new PerfectDB(d, { flushThreshold: 100 }); db.autoCompactThreshold = 0; db.open();
  for (let i = 0; i < 250; i++) { const tx = db.begin(); db.put(tx, K(i), V(i)); db.commit(tx); }
  db.close();
  const f = path.join(d, 'sstables', 'sstable-0000.jsonl');
  const lines = fs.readFileSync(f, 'utf8').split('\n').filter(l => l.trim() !== '');
  fs.writeFileSync(f, lines.slice(0, lines.length - 3).join('\n') + '\n');
  try { const bad = new PerfectDB(d, { flushThreshold: 100 }); bad.open(); return false; }
  catch (e) { return /footer|truncat/i.test(e.message); }
});

function rot1(line) {
  for (let i = 0; i < line.length; i++)
    if (line[i] >= '0' && line[i] <= '9')
      return line.slice(0, i) + (line[i] === '9' ? '0' : String.fromCharCode(line[i].charCodeAt(0) + 1)) + line.slice(i + 1);
  throw new Error('no digit to rotate');
}

check('N6 rotation + flush collapses to one segment', () => {
  const d = path.join(ROOT, 'n6'); clean(d);
  const db = new PerfectDB(d, { flushThreshold: 1000000, walSegmentBytes: 4096 }); db.open();
  for (let i = 0; i < 300; i++) { const tx = db.begin(); db.put(tx, K(i), V(i)); db.commit(tx); }
  const segs = fs.readdirSync(path.join(d, 'wal')).filter(f => f.endsWith('.log'));
  if (segs.length < 3 || db.scan('k000000', 'k999999').length !== 300) return false;
  db.close();
  const db2 = new PerfectDB(d, { flushThreshold: 1000000 }); db2.open();
  if (db2.scan('k000000', 'k999999').length !== 300) return false;
  db2.flush();
  const after = fs.readdirSync(path.join(d, 'wal')).filter(f => f.endsWith('.log'));
  const n = db2.scan('k000000', 'k999999').length;
  db2.close(); return after.length === 1 && n === 300;
});

check('N7 legacy wal.log upgrade replay', () => {
  const crypto = require('crypto');
  const crc16 = s => crypto.createHash('sha256').update(s).digest('hex').slice(0, 16);
  const d = path.join(ROOT, 'n7'); clean(d); fs.mkdirSync(d, { recursive: true });
  // hand-write a pre-segment legacy wal.log with two valid records
  const rec = (lsn, k, v) => JSON.stringify({ lsn, tx: 1, op: 'put', k, v, crc: crc16(`${lsn}|1|put|${k}|${v}`) }) + '\n';
  fs.writeFileSync(path.join(d, 'wal.log'), rec(1, 'k000001', 'hello') + rec(2, 'k000002', 'world'));
  const db = new PerfectDB(d, { flushThreshold: 1000000 }); db.open();
  const ok = db.get('k000001') === 'hello' && db.get('k000002') === 'world' && db.lsn === 2;
  db.close(); return ok;
});

console.log(fails === 0 ? 'ALL NODE PROBES PASSED' : `${fails} FAILED`);
process.exit(fails === 0 ? 0 : 1);
