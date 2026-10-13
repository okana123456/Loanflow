const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const {performance}=require('node:perf_hooks');
const R=require('../bripta-reporting.js');
const html=fs.readFileSync(require('node:path').join(__dirname,'../index.html'),'utf8');
const source=html.slice(html.indexOf('function kenyaDateISO('),html.indexOf('\nfunction shiftISODate('));
let constructions=0;
const context=vm.createContext({Intl:{DateTimeFormat:function(...args){constructions++;return new Intl.DateTimeFormat(...args)}}});
vm.runInContext(source,context);
const original=(value)=>{
  if(!value)return '';
  const raw=String(value);
  if(/^\d{4}-\d{2}-\d{2}$/.test(raw))return raw;
  if(/^\d{4}-\d{2}-\d{2}[T ]/.test(raw)&&!/(Z|[+-]\d{2}:?\d{2})$/.test(raw))return raw.slice(0,10);
  const dt=value instanceof Date?value:new Date(value);
  if(isNaN(dt))return raw.slice(0,10);
  const parts=new Intl.DateTimeFormat('en-CA',{timeZone:'Africa/Nairobi',year:'numeric',month:'2-digit',day:'2-digit'}).formatToParts(dt);
  const pick=type=>parts.find(part=>part.type===type)?.value;
  return `${pick('year')}-${pick('month')}-${pick('day')}`;
};
const samples=[null,'','invalid','2026-10-01','2026-10-01T23:59:00',
  '2026-10-01T20:59:59Z','2026-10-01T21:00:00Z','2026-12-31T22:00:00Z',
  '2026-10-01T23:00:00+03:00','2026-10-01T23:00:00-04:00'];
for(const value of samples)assert.equal(context.kenyaDateISO(value),original(value));
const timestamps=Array.from({length:14000},(_,i)=>new Date(Date.UTC(2026,9,1)+i*61000).toISOString());
let start=performance.now();
const expected=timestamps.map(original);
const oldMs=performance.now()-start;
start=performance.now();
const actual=timestamps.map(context.kenyaDateISO);
const newMs=performance.now()-start;
assert.deepEqual(actual,expected);
assert.equal(constructions,1,'reuse one formatter across all repayment dates');

// Aggregating precomputed loan quality must preserve officer-level BQ/PAR,
// including shared clients, inactive loans and irregular schedules.
const loans=Array.from({length:40},(_,i)=>({id:String(i),client_id:String(i%9),loan_officer_id:String(i%4),
  status:i%7?'active':'completed',outstanding_balance:80,total_payable:100,total_paid:20,arrears_amount:35,overdue_days:5}));
const schedules=loans.filter((_,i)=>i%3).map(l=>({id:l.id,loan_id:l.id,due_date:'2026-10-01',total_due:100,total_paid:20}));
const all=R.portfolioQuality(loans,schedules,'2026-10-13');
for(let i=0;i<4;i++){
  const owned=l=>l.loan_officer_id===String(i);
  assert.deepEqual(R.qualityTotals(all.active.filter(owned)),R.portfolioQuality(loans.filter(owned),schedules,'2026-10-13'));
}
console.log(`PASS: identical dates and officer totals; 14,000-date local benchmark ${oldMs.toFixed(0)}ms -> ${newMs.toFixed(0)}ms (not a network benchmark).`);
