export default async function handler(req, res) {
  // 1. MANDATORY: The 'Accepted' handshake for Safaricom registration
  // This handles GET requests (from browsers) and test pings
  if (req.method !== 'POST') {
    return res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted' });
  }

  const body = req.body;
  const transId = body?.TransID;

  // 2. REGISTRATION PING: Safaricom sends a POST without a TransID during registration
  // We MUST return 'Accepted' here or registration will fail
  if (!transId) {
    return res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted' });
  }

  // --- START OF BUSINESS LOGIC ---
  
  const transAmount = Number(body?.TransAmount || 0);
  const billRef = (body?.BillRefNumber || '').trim().toUpperCase(); 

  // Your Supabase Details
  const SUPABASE_URL = "https://nngscmpsxtqqjzcnsrbi.supabase.co";
  const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

  // Emergency fallback if your environment variables aren't set yet
  if (!SUPABASE_SERVICE_KEY) {
    console.error("Missing Service Key in Vercel!");
    return res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted' });
  }

  // Helper function for Supabase communication
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
    // 3. LOG TO QUEUE: Record the transaction immediately
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

    // 4. LOAN MATCHING: Try to match the bill reference to an active loan
    let loans = await db('loans', 'GET', null, `?loan_no=eq.${billRef}&status=eq.active&select=*`);
    if (loans && loans.length > 0) {
      targetLoan = loans[0];
    } else {
      // Check if client entered National ID instead of Loan Number
      let clients = await db('loan_clients', 'GET', null, `?id_number=eq.${billRef}&select=id`);
      if (clients && clients.length > 0) {
        let clientLoans = await db('loans', 'GET', null, `?client_id=eq.${clients[0].id}&status=eq.active&select=*`);
        if (clientLoans && clientLoans.length > 0) targetLoan = clientLoans[0];
      }
    }

    // 5. AUTO-RECONCILIATION
    if (targetLoan && queueId) {
      let settings = await db('loan_settings', 'GET', null, `?business_id=eq.${targetLoan.business_id}&select=mpesa_auto_confirm`);
      let autoConfirm = settings.length > 0 ? settings[0].mpesa_auto_confirm : false;

      if (autoConfirm) {
        let reps = await db('loan_repayments', 'GET', null, '?select=id');
        let receiptNo = 'RCP-' + new Date().getFullYear() + '-' + String((reps.length || 0) + 1).padStart(5, '0');

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

        let newPaid = Number(targetLoan.total_paid) + transAmount;
        let newBalance = Math.max(0, Number(targetLoan.outstanding_balance) - transAmount);
        let newStatus = newBalance <= 0 ? 'completed' : targetLoan.status;

        await db('loans', 'PATCH', {
          total_paid: newPaid,
          outstanding_balance: newBalance,
          status: newStatus
        }, `?id=eq.${targetLoan.id}`);

        await db('mpesa_callback_queue', 'PATCH', { 
          confirmed: true, loan_id: targetLoan.id, repayment_id: repData[0]?.id 
        }, `?id=eq.${queueId}`);

      } else {
        await db('mpesa_callback_queue', 'PATCH', { loan_id: targetLoan.id }, `?id=eq.${queueId}`);
      }
    }

    // FINAL RESPONSE: Success acknowledgment to Safaricom
    return res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted' });

  } catch (error) {
    console.error('Callback error:', error);
    // Even on error, we tell Safaricom 'Accepted' to stop them from retrying the ping
    return res.status(200).json({ ResultCode: 0, ResultDesc: 'Accepted' });
  }
}
