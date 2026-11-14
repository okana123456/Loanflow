const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const html = fs.readFileSync('index.html', 'utf8');
const between = (start, end) => html.slice(html.indexOf(start), html.indexOf(end, html.indexOf(start)));
const context = vm.createContext({
  selectedBranchId: null,
  currentUser: { business_id: 'BIZ-B3F5E5D9' },
  BRIPTA_OFFICER_REDIRECTS: {},
  canonicalLoanOfficerId: id => id,
  branchName: id => ({ mbita: 'Mbita', migori: 'Migori' })[id] || id,
});
vm.runInContext(between('function officerPortfolioBranchLabel(', '\n// ═══ KEEP ALIVE PING'), context);

const officer = { id: 'george', branch_id: 'mbita' };
const loans = [
  { id: 'former', loan_officer_id: 'george', branch_id: 'migori', status: 'closed' },
  { id: 'current', loan_officer_id: 'george', branch_id: 'mbita', status: 'active' },
  { id: 'other', loan_officer_id: 'james', branch_id: 'mbita', status: 'active' },
];
const current = [loans[1]];
assert.deepEqual(Array.from(context.officerPerformanceLoans(officer, loans, current), l => l.id), ['current']);
assert.equal(context.officerPortfolioBranchLabel(officer, current), 'Mbita');
context.selectedBranchId = 'mbita';
assert.equal(context.officerPortfolioBranchLabel(officer, current), 'Mbita');
console.log('PASS: transferred officer performance uses only branches with an active portfolio');
