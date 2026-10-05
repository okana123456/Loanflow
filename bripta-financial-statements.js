// Source-based P&L and cumulative recorded journal balance sheet. Read-only.
function briptaBalanceSheet(accounts){
  const groups={asset:[],liability:[],equity:[]};let retained=0;
  for(const row of accounts||[]){
    const signed=Number(row.signed_balance||0);
    if(row.account_type==='income'||row.account_type==='expense')retained-=signed;
    else if(groups[row.account_type])groups[row.account_type].push({...row,balance:row.account_type==='asset'?signed:-signed});
  }
  const sum=rows=>rows.reduce((total,row)=>total+row.balance,0);
  const assets=sum(groups.asset),liabilities=sum(groups.liability),capital=sum(groups.equity);
  return {groups,assets,liabilities,capital,retained,equity:capital+retained,difference:assets-liabilities-capital-retained};
}
function briptaStatementHtml(report,income){
  if(report.error)return `<div class="card"><h3>Profit &amp; Loss / Balance Sheet</h3><p>Could not load financial statements: ${escapeHtml(report.error.message)}. Reload after installing the financial statements SQL.</p></div>`;
  const expenses=Number(report.approved_expenses||0),net=income.total-expenses;
  const line=(label,value,bold=false)=>`<div style="display:flex;justify-content:space-between;gap:12px;padding:8px 0;${bold?'font-weight:700;border-top:1px solid var(--border)':''}"><span>${escapeHtml(label)}</span><strong style="white-space:nowrap">${fmtMoney(value)}</strong></div>`;
  const sheet=briptaBalanceSheet(report.accounts);
  const section=(title,rows,total)=>`<h4 style="margin:14px 0 5px">${title}</h4>${rows.map(row=>line(row.account_name,row.balance)).join('')||'<p>No recorded balances</p>'}${line('Total '+title,total,true)}`;
  const missing=Object.entries(report.missing_source_postings||{}).filter(([,count])=>Number(count)>0);
  const labels={loans:'loan disbursements',repayments:'repayments',approved_expenses:'approved expenses',asset_purchases:'asset purchases'};
  const notes=[];
  if(!report.opening_balances_identified)notes.push('Opening cash, M-Pesa/bank, capital and debt balances have not been identified in the ledger.');
  if(missing.length)notes.push('Source records without journal postings: '+missing.map(([name,count])=>`${count} ${labels[name]||name}`).join(', ')+'.');
  if(Number(report.unbalanced_sources)>0)notes.push(`${report.unbalanced_sources} journal transaction(s) need balancing.`);
  if(Math.abs(sheet.difference)>0.01)notes.push('Assets and liabilities plus equity differ by '+fmtMoney(sheet.difference)+'.');
  const status=notes.length?'Incomplete — review the items below':'Recorded entries reconcile';
  return `<div class="charts-grid" style="margin-top:20px">
    <div class="card"><h3>Profit &amp; Loss</h3><p style="font-size:12px;color:var(--text-secondary)">${escapeHtml(report.period_start)} to ${escapeHtml(report.as_of)} · Collected income less approved expenses</p>
      ${line('Registration fees',income.registration)}${line('Processing fees',income.processing)}${line('Interest collected',income.interest)}${line('Penalties collected',income.penalties)}
      ${line('Total income',income.total,true)}<h4 style="margin-top:14px">Approved expenses</h4>
      ${(report.expense_categories||[]).map(row=>line(row.category,Number(row.amount))).join('')||'<p>No approved expenses in this period</p>'}
      ${line('Total approved expenses',expenses,true)}${line(net>=0?'Net profit':'Net loss',net,true)}
      <p style="font-size:12px;color:var(--text-secondary)">Expenses use their expense date. Draft, pending, rejected and cancelled expenses are excluded. Loan principal movements and owner capital are excluded from profit.</p>
    </div>
    <div class="card"><h3>Balance Sheet</h3><p style="font-size:12px;color:var(--text-secondary)">Recorded ledger balances as of ${escapeHtml(report.as_of)} · Cumulative through this date</p>
      ${section('Assets',sheet.groups.asset,sheet.assets)}${section('Liabilities',sheet.groups.liability,sheet.liabilities)}
      <h4 style="margin:14px 0 5px">Equity</h4>${sheet.groups.equity.map(row=>line(row.account_name,row.balance)).join('')}
      ${line('Retained profit / loss recorded in ledger',sheet.retained)}${line('Total equity',sheet.equity,true)}
      ${line('Total liabilities + equity',sheet.liabilities+sheet.equity,true)}
      <div style="margin-top:12px;padding:12px;border:1px solid var(--border);border-radius:8px"><strong>${status}</strong>
        ${notes.length?`<ul style="margin:8px 0;padding-left:18px;font-size:12px">${notes.map(note=>`<li>${escapeHtml(note)}</li>`).join('')}</ul>`:''}
        <p style="font-size:12px;color:var(--text-secondary);margin-top:6px">This statement uses posted journal entries. Historical income and expenses in the Profit &amp; Loss report can differ from the ledger until missing postings are reviewed. No missing balances are estimated.</p>
      </div>
    </div></div>`;
}
