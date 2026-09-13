// Grouped-commit probe for Node PerfectDB: 64 concurrent chains x 150 txns.
const fs = require('fs');
const path = require('path');
const { PerfectDB } = require('./perfect_db_mvm');

function clean(d) {
  for (let a = 0; ; a++) {
    try { if (fs.existsSync(d)) fs.rmSync(d, { recursive: true, force: true }); break; }
    catch (e) { if (a >= 60) throw e; Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 500); }
  }
}

async function main() {
  const DATA = path.join(__dirname, 'mvm-data-grouped');
  clean(DATA);
  const db = new PerfectDB(DATA, { flushThreshold: 200000 });
  db.open();
  const W = 64, N = 150;
  const t0 = Date.now();
  await Promise.all(Array.from({ length: W }, (_, t) => (async () => {
    for (let i = 0; i < N; i++) {
      const tx = db.begin();
      db.put(tx, `g${String(t).padStart(2, '0')}-${String(i).padStart(6, '0')}`, `v${i}-` + 'x'.repeat(50));
      await db.commitGrouped(tx, 10);
    }
  })()));
  const ms = Date.now() - t0;
  const s = db.groupStats();
  console.log(`grouped ${W * N} commits/${W} chains: ${ms}ms = ${(W * N / (ms / 1000)).toFixed(0)} ops/s batches=${s.batches} avgBatch=${s.avgBatch.toFixed(1)}`);
  const n = db.scan('g', 'h').length;
  console.log(`rows=${n} ${n === W * N ? 'OK' : 'LOSS!'}`);
  db.close();
  const t1 = Date.now();
  const db2 = new PerfectDB(DATA, { flushThreshold: 200000 });
  db2.open();
  const n2 = db2.scan('g', 'h').length;
  console.log(`recovery(reopen): ${Date.now() - t1}ms rows=${n2} ${n2 === W * N ? 'OK no-loss' : 'LOSS!'}`);
  db2.close();
}

main().catch(e => { console.error(e); process.exit(1); });
