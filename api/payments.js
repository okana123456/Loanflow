export default async function handler(req, res) {
  // Always accept standard pings to keep Safaricom happy
  if (req.method !== 'POST') {
    return res.status(200).json({ ResultCode: 0, ResultDesc: 'Ready' });
  }

  const body = req.body;
  console.log("Incoming M-Pesa Data:", body);

  const transId = body?.TransID;
  const transAmount = Number(body?.TransAmount || 0);
  const billRef = (body?.BillRefNumber || '').trim().toUpperCase(); // Client's Loan ID or National ID

  if (!transId) {
    return res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted Verification' });
  }

  // Your Supabase Database Details
  const SUPABASE_URL = "https://nngscmpsxtqqjzcnsrbi.supabase.co";
  const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY; // Hidden securely in Vercel

  if (!SUPABASE_SERVICE_KEY) {
    console.error("Missing Service Key in Vercel!");
    return res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted' });
  }

  // Helper to talk to Supabase Database
  const db = async (table, method, payload = null, query = '') => {
    const options = {
      method,
      headers: {
        'apikey': SUPABASE_SERVICE_KEY,
        'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
        'Content-Type': 'application/json',
        'Prefer': 'return=representation'
      }
    };
    if (payload) options.body = JSON.stringify(payload);
    const response = await fetch(`${SUPABASE_URL}/rest/v1/${table}${query}`, options);
    return response.json();
  };

  try {
    // 1. Log the incoming money into the M-Pesa Queue instantly
    const queueData = await db('mpesa_callback_queue', 'POST', {
      transaction_type: body?.TransactionType || 'C2B',
      trans_id: transId,
      trans_time: body?.TransTime,
      trans_amount: transAmount,
      business_short_code: body?.BusinessShortCode,
      bill_ref_number: billRef,
      msisdn: body?.MSISDN,
      first_name: body?.FirstName || '',
      raw_payload: body,
      confirmed: false
    });
    
    const queueId = queueData[0]?.id;
    let targetLoan = null;

    // 2. Try to match the Account Number to a Loan Number (e.g., LN-1234)
    let loans = await db('loans', 'GET', null, `?loan_no=eq.${billRef}&status=eq.active&select=*`);
    if (loans && loans.length > 0) {
      targetLoan = loans[0];
    } else {
      // 3. SMART MATCH: If Loan Number fails, check if the client entered their National ID!
      let clients = await db('loan_clients', 'GET', null, `?id_number=eq.${billRef}&select=id`);
      if (clients && clients.length > 0) {
        let clientLoans = await db('loans', 'GET', null, `?client_id=eq.${clients[0].id}&status=eq.active&select=*`);
        if (clientLoans && clientLoans.length > 0) targetLoan = clientLoans[0];
      }
    }

    // 4. Auto-Reconciliation Process
    if (targetLoan && queueId) {
      let settings = await db('loan_settings', 'GET', null, `?business_id=eq.${targetLoan.business_id}&select=mpesa_auto_confirm`);
      let autoConfirm = settings.length > 0 ? settings[0].mpesa_auto_confirm : false;

      if (autoConfirm) {
        // Generate a Receipt Number
        let reps = await db('loan_repayments', 'GET', null, '?select=id');
        let receiptNo = 'RCP-' + new Date().getFullYear() + '-' + String((reps.length || 0) + 1).padStart(5, '0');

        // Insert the Repayment Record
        let repData = await db('loan_repayments', 'POST', {
          loan_id: targetLoan.id,
          receipt_no: receiptNo,
          amount: transAmount,
          payment_method: 'mpesa',
          payment_reference: transId,
          payment_date: new Date().toISOString().slice(0, 10),
          principal_portion: transAmount * 0.7,
          interest_portion: transAmount * 0.3,
          penalty_portion: 0,
          mpesa_confirmed: true,
          business_id: targetLoan.business_id,
          notes: 'Auto M-Pesa mapped from Ref/ID: ' + billRef
        });

        // Update Loan Balance
        let newPaid = Number(targetLoan.total_paid) + transAmount;
        let newBalance = Math.max(0, Number(targetLoan.outstanding_balance) - transAmount);
        let newStatus = newBalance <= 0 ? 'completed' : targetLoan.status;

        await db('loans', 'PATCH', {
          total_paid: newPaid,
          outstanding_balance: newBalance,
          status: newStatus
        }, `?id=eq.${targetLoan.id}`);

        // Mark Queue as fully confirmed
        await db('mpesa_callback_queue', 'PATCH', { 
          confirmed: true, loan_id: targetLoan.id, repayment_id: repData[0]?.id 
        }, `?id=eq.${queueId}`);

      } else {
        // Auto-confirm is OFF. Link to loan, but leave for Cashier to approve manually.
        await db('mpesa_callback_queue', 'PATCH', { loan_id: targetLoan.id }, `?id=eq.${queueId}`);
      }
    }

    return res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted' });

  } catch (error) {
    console.error('Callback error:', error);
    return res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted' });
  }
}
