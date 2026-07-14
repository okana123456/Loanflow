export default async function handler(req, res) {
  // ── MANDATORY: Safaricom handshake (GET or registration ping) ──────────────
  if (req.method !== 'POST') {
    return res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted' });
  }

  const body = req.body;
  const transId = body?.TransID;

  // Safaricom sends a POST without TransID during C2B URL registration
  if (!transId) {
    return res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted' });
  }

  // ── Daraja C2B Confirmation Payload ────────────────────────────────────────
  const transAmount    = Number(body?.TransAmount || 0);
  const billRefNumber  = (body?.BillRefNumber || '').trim().toUpperCase(); // National ID
  const businessShortCode = (body?.BusinessShortCode || '').trim();
  const msisdn         = body?.MSISDN || '';
  const transTime      = body?.TransTime || '';
  const firstName      = body?.FirstName || '';

  const SUPABASE_URL         = 'https://nngscmpsxtqqjzcnsrbi.supabase.co';
  const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
  const cleanRef = (v) => String(v || '').trim();
  const refUpper = cleanRef(billRefNumber).toUpperCase();
  const refDigits = refUpper.replace(/\D/g, '');
  const phoneCandidates = (v) => {
    const d = String(v || '').replace(/\D/g, '');
    const out = new Set();
    if (!d) return [];
    out.add(d);
    if (d.length >= 9) out.add(d.slice(-9));
    if (d.length === 9 && /^[17]/.test(d)) {
      out.add('0' + d);
      out.add('254' + d);
    }
    if (d.length === 10 && d.startsWith('0')) out.add('254' + d.slice(1));
    if (d.length === 12 && d.startsWith('254')) out.add('0' + d.slice(3));
    return [...out];
  };
  const mpesaPaymentDate = (v) => {
    const s = String(v || '').trim();
    if (/^\d{14}$/.test(s)) {
      const d = new Date(
        Number(s.slice(0, 4)),
        Number(s.slice(4, 6)) - 1,
        Number(s.slice(6, 8)),
        Number(s.slice(8, 10)),
        Number(s.slice(10, 12)),
        Number(s.slice(12, 14))
      );
      if (!Number.isNaN(d.getTime())) return d.toISOString();
    }
    return new Date().toISOString();
  };

  if (!SUPABASE_SERVICE_KEY) {
    console.error('[Daraja] Missing SUPABASE_SERVICE_KEY');
    return res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted' });
  }

  // ── Supabase REST helper ────────────────────────────────────────────────────
  const db = async (table, method, payload = null, query = '') => {
    const opts = {
      method,
      headers: {
        'apikey': SUPABASE_SERVICE_KEY,
        'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
        'Content-Type': 'application/json',
        'Prefer': 'return=representation',
      },
    };
    if (payload) opts.body = JSON.stringify(payload);
    const r = await fetch(`${SUPABASE_URL}/rest/v1/${table}${query}`, opts);
    if (!r.ok) {
      const text = await r.text();
      throw new Error(`Supabase ${method} ${table}: ${r.status} — ${text}`);
    }
    return r.json();
  };

  try {
    // 1. LOG raw payload immediately ─────────────────────────────────────────
    const queueRows = await db('mpesa_callback_queue', 'POST', {
      transaction_type:    body?.TransactionType || 'C2B',
      trans_id:            transId,
      trans_time:          transTime,
      trans_amount:        transAmount,
      business_short_code: businessShortCode,
      bill_ref_number:     billRefNumber,
      msisdn,
      first_name:          firstName,
      raw_payload:         body,
      confirmed:           false,
    });
    const queueId = queueRows[0]?.id;

    // 2. MULTI-TENANT ROUTING — find business by shortcode ───────────────────
    const settingsRows = await db(
      'loan_settings', 'GET', null,
      `?mpesa_shortcode=eq.${encodeURIComponent(businessShortCode)}&select=*&limit=1`
    );
    if (!settingsRows || settingsRows.length === 0) {
      console.error(`[Daraja] No business found for shortcode ${businessShortCode}`);
      return res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted' });
    }
    const settings   = settingsRows[0];
    const businessId = settings.business_id;
    const autoConfirm = !!settings.mpesa_auto_confirm;

    // 3. CLIENT MATCHING — BillRefNumber is the National ID ──────────────────
    let clientId = null;

    const phoneRefs = [...new Set([
      ...phoneCandidates(billRefNumber),
      ...phoneCandidates(msisdn),
    ])];
    for (const ph of phoneRefs) {
      const rows = await db(
        'loan_clients', 'GET', null,
        `?phone=eq.${encodeURIComponent(ph)}&business_id=eq.${businessId}&select=id&limit=1`
      );
      if (rows && rows.length) { clientId = rows[0].id; break; }
    }

    if (!clientId && phoneRefs.length) {
      const tails = [...new Set(phoneRefs.map((ph) => ph.replace(/\D/g, '').slice(-9)).filter(Boolean))];
      for (const tail of tails) {
        const rows = await db(
          'loan_clients', 'GET', null,
          `?phone=ilike.*${encodeURIComponent(tail)}&business_id=eq.${businessId}&select=id,phone&limit=1`
        );
        if (rows && rows.length) { clientId = rows[0].id; break; }
      }
    }

    if (!clientId) {
      console.warn(`[Daraja] No client found for phone/account reference ${billRefNumber} in business ${businessId}`);
      if (queueId) {
        await db('mpesa_callback_queue', 'PATCH',
          { unmatched: true, unmatched_reason: 'no_client_phone_found' },
          `?id=eq.${queueId}`
        ).catch(() => {});
      }
      return res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted' });
    }

    // 4. ACTIVE LOANS — FIFO (oldest disbursement first) ─────────────────────
    let activeLoans = await db(
      'loans', 'GET', null,
      `?client_id=eq.${clientId}&business_id=eq.${businessId}&status=eq.active&order=created_at.desc&select=*`
    );
    if (!activeLoans || activeLoans.length === 0) {
      console.warn(`[Daraja] No active loans for client ${clientId}`);
      if (queueId) {
        await db('mpesa_callback_queue', 'PATCH',
          { loan_id: null, unmatched: true, unmatched_reason: 'no_active_loan' },
          `?id=eq.${queueId}`
        ).catch(() => {});
      }
      return res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted' });
    }

    // 5. CASCADE PAYMENT (FIFO) ───────────────────────────────────────────────
    let remainingAmount = transAmount;
    const repaymentIds  = [];
    let   firstLoanId   = null;

    // Helper: generate receipt number (business-scoped)
    const makeReceiptNo = async () => {
      const prefix = settings.receipt_no_prefix || 'RCP';
      const existing = await db('loan_repayments', 'GET', null,
        `?business_id=eq.${businessId}&select=id`
      );
      return `${prefix}-${new Date().getFullYear()}-${String((existing.length || 0) + 1).padStart(5, '0')}`;
    };

    for (const loan of activeLoans) {
      if (remainingAmount <= 0) break;

      const loanBalance  = Number(loan.outstanding_balance || 0);
      const appliedAmount = Math.min(remainingAmount, loanBalance);
      remainingAmount    = parseFloat((remainingAmount - appliedAmount).toFixed(2));

      // Proportional principal/interest split from loan's own ratio
      const totalPayable  = Number(loan.total_payable || 0);
      const totalInterest = Number(loan.total_interest || 0);
      const interestRatio = (totalPayable > 0 && totalInterest > 0) ? (totalInterest / totalPayable) : 0.3;
      const interestPortion  = parseFloat((appliedAmount * interestRatio).toFixed(2));
      const principalPortion = parseFloat((appliedAmount - interestPortion).toFixed(2));

      const newBalance = parseFloat(Math.max(0, loanBalance - appliedAmount).toFixed(2));
      const newPaid    = parseFloat((Number(loan.total_paid || 0) + appliedAmount).toFixed(2));
      const newStatus  = newBalance <= 0 ? 'completed' : loan.status;

      if (autoConfirm) {
        const receiptNo = await makeReceiptNo();

        // a) Record repayment
        const repRows = await db('loan_repayments', 'POST', {
          loan_id:           loan.id,
          business_id:       businessId,
          receipt_no:        receiptNo,
          amount:            appliedAmount,
          payment_method:    'mpesa_c2b',
          payment_reference: transId,
          payment_date:      mpesaPaymentDate(transTime),
          principal_portion: principalPortion,
          interest_portion:  interestPortion,
          penalty_portion:   0,
          mpesa_confirmed:   true,
          notes:             `Auto Daraja C2B - Account Ref: ${billRefNumber} - TransID: ${transId}`,
        });
        const repId = repRows[0]?.id;
        if (repId) repaymentIds.push(repId);
        if (!firstLoanId) firstLoanId = loan.id;

        // b) Update loan balances & status
        await db('loans', 'PATCH', {
          total_paid:          newPaid,
          outstanding_balance: newBalance,
          status:              newStatus,
        }, `?id=eq.${loan.id}`);

        // c) Update loan_schedules — mark instalments paid up to this amount
        //    Strategy: mark the earliest unpaid/partial instalment(s) as paid
        const scheduleRows = await db(
          'loan_schedules', 'GET', null,
          `?loan_id=eq.${loan.id}&status=neq.paid&order=installment_no.asc&select=*`
        );
        let schedRemainder = appliedAmount;
        for (const sched of (scheduleRows || [])) {
          if (schedRemainder <= 0) break;
          const schedDue     = Number(sched.total_due || sched.amount_due || sched.installment || 0);
          const schedPaid    = Number(sched.total_paid || sched.amount_paid || 0);
          const schedBalance = schedDue - schedPaid;
          if (schedBalance <= 0) continue;

          const schedApply  = Math.min(schedRemainder, schedBalance);
          schedRemainder    = parseFloat((schedRemainder - schedApply).toFixed(2));
          const newSchedPaid = parseFloat((schedPaid + schedApply).toFixed(2));
          const schedStatus  = newSchedPaid >= schedDue ? 'paid' : 'partial';

          await db('loan_schedules', 'PATCH', {
            total_paid:   newSchedPaid,
            status:       schedStatus,
            paid_at:      schedStatus === 'paid' ? mpesaPaymentDate(transTime) : null,
          }, `?id=eq.${sched.id}`);
        }
      }
    }

    // 6. UPDATE QUEUE ENTRY ───────────────────────────────────────────────────
    if (queueId) {
      await db('mpesa_callback_queue', 'PATCH', {
        confirmed:   autoConfirm,
        loan_id:     firstLoanId,
        repayment_id: repaymentIds[0] || null,
        business_id: businessId,
      }, `?id=eq.${queueId}`);
    }

    return res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted' });

  } catch (error) {
    console.error('[Daraja] Callback error:', error);
    // Always return Accepted to stop Safaricom retries
    return res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted' });
  }
}
