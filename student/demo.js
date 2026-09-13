// Guided demo: run with `node demo.js` from the student/ directory.
// It narrates every step and asserts the promises. Exit code = failures.
const fs = require('fs');
const os = require('os');
const path = require('path');
const { StudentDB } = require('./perfectdb-student');

const DATA = fs.mkdtempSync(path.join(os.tmpdir(), 'student-demo-')); // outside OneDrive
let fails = 0;
const say = (m) => console.log(m);
const check = (name, cond) => { console.log(`  [${cond ? 'PASS' : 'FAIL'}] ${name}`); if (!cond) fails++; };

say('--- 1. WRITE: two transactions, five keys ---');
let db = new StudentDB(DATA); db.open();
{ const tx = db.begin(); db.put(tx, 'name', 'ada'); db.put(tx, 'role', 'analyst'); db.commit(tx); }
{ const tx = db.begin(); db.put(tx, 'city', 'london'); db.put(tx, 'year', '1843'); db.put(tx, 'lang', 'notes'); db.commit(tx); }
say(`  wal.log holds ${fs.readFileSync(path.join(DATA, 'wal.log'), 'utf8').split('\n').filter(Boolean).length} log lines, clock at lsn=${db.lsn}`);
check('get latest', db.get('name') === 'ada');
check('scan finds 5', db.scan().length === 5);

say('--- 2. TIME TRAVEL: overwrite, then read the past ---');
{ const tx = db.begin(); db.put(tx, 'city', 'paris'); db.commit(tx); }
check('city is now paris', db.get('city') === 'paris');
check('city at lsn=5 was london', db.getAt('city', 5) === 'london');

say('--- 3. CHECKPOINT: flush snapshots the whiteboard ---');
const snap = db.flush();
say(`  wrote ${path.basename(snap)}, wal.log truncated to ${fs.statSync(path.join(DATA, 'wal.log')).size} bytes`);
check('data survives flush', db.get('lang') === 'notes' && db.scan().length === 5);
db.close();

say('--- 4. CRASH: torn final line in wal.log, then reopen ---');
{ const tx2db = new StudentDB(DATA); tx2db.open();
  const tx = tx2db.begin(); tx2db.put(tx, 'ship', 'gondola'); tx2db.commit(tx); tx2db.close(); }
fs.appendFileSync(path.join(DATA, 'wal.log'), '{"lsn":999,"tx":99,"op":"put","k":"gho'); // power cut mid-byte
db = new StudentDB(DATA); db.open(); // must not throw; torn tail dropped
check('committed ship survives', db.get('ship') === 'gondola');
check('all 6 keys present', db.scan().length === 6);
check('clock kept moving (lsn>=6)', db.lsn >= 6);
db.close();

say(fails === 0 ? 'ALL DEMO CHECKS PASSED' : `${fails} CHECK(S) FAILED`);
process.exit(fails === 0 ? 0 : 1);
