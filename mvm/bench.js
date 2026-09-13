// Bench for PerfectDB MVM
const fs = require('fs');
const path = require('path');
const { PerfectDB } = require('./perfect_db_mvm');

const DATA = path.join(__dirname, 'mvm-data');
if (fs.existsSync(DATA)) fs.rmSync(DATA, { recursive: true, force: true });

const db = new PerfectDB(DATA, { flushThreshold: 2000 });
let t0 = Date.now();
db.open();
console.log(`open(empty) ${Date.now() - t0}ms`);

// 1. single-commit writes (worst case: 1 fsync per op)
t0 = Date.now();
for (let i = 0; i < 2000; i++) {
  const tx = db.begin();
  db.put(tx, `k${String(i).padStart(6, '0')}`, `v${i}-` + 'x'.repeat(50));
  db.commit(tx);
}
const singleMs = Date.now() - t0;
console.log(`write 2000 x single-commit: ${singleMs}ms = ${(2000 / (singleMs / 1000)).toFixed(0)} ops/s`);

// 2. batched writes (1 tx = 2000 ops, 1 fsync)
t0 = Date.now();
{
  const tx = db.begin();
  for (let i = 2000; i < 4000; i++) db.put(tx, `k${String(i).padStart(6, '0')}`, `v${i}-` + 'x'.repeat(50));
  db.commit(tx);
}
const batchMs = Date.now() - t0;
console.log(`write 2000 x batched(1 tx): ${batchMs}ms = ${(2000 / (batchMs / 1000)).toFixed(0)} ops/s`);

// 3. random reads
t0 = Date.now();
let hits = 0;
for (let i = 0; i < 5000; i++) {
  const k = `k${String(Math.floor(Math.random() * 4000)).padStart(6, '0')}`;
  if (db.get(k) !== null) hits++;
}
const readMs = Date.now() - t0;
console.log(`read 5000 random: ${readMs}ms = ${(5000 / (readMs / 1000)).toFixed(0)} ops/s hits=${hits}`);

// 4. range scan
t0 = Date.now();
const rows = db.scan('k000000', 'k999999');
console.log(`scan ${rows.length} rows: ${Date.now() - t0}ms`);

// 5. time travel
const snap = db.lsn;
{
  const tx = db.begin();
  db.put(tx, 'k000001', 'MUTATED');
  db.commit(tx);
}
const nowVal = db.get('k000001');
const oldVal = db.get('k000001', snap);
console.log(`time-travel: now=${String(nowVal).slice(0, 12)} snap@${snap}=${String(oldVal).slice(0, 12)} ok=${oldVal !== 'MUTATED'}`);

// 6. flush + recovery
t0 = Date.now();
db.flush();
console.log(`flush: ${Date.now() - t0}ms sstables=${db.sstables.length}`);
db.close();

t0 = Date.now();
const db2 = new PerfectDB(DATA, { flushThreshold: 2000 });
db2.open();
const recMs = Date.now() - t0;
const check = db2.scan('k000000', 'k999999').length;
console.log(`recovery(reopen): ${recMs}ms rows=${check} ${check === rows.length ? 'OK no-loss' : 'LOSS!'}`);
db2.close();

// sizes
let total = 0;
(function walk(d) {
  for (const f of fs.readdirSync(d)) {
    const p = path.join(d, f);
    const s = fs.statSync(p);
    if (s.isDirectory()) walk(p); else total += s.size;
  }
})(DATA);
console.log(`disk ${(total / 1024).toFixed(1)}KB for ${check} rows`);
