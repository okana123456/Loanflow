import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.38.4";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

function phoneVariants(value: unknown) {
  const digits = String(value || "").replace(/\D/g, "");
  const out = new Set<string>();
  if (!digits) return [];
  out.add(digits);
  if (digits.length >= 9) out.add(digits.slice(-9));
  if (digits.length === 12 && digits.startsWith("254")) out.add("0" + digits.slice(3));
  if (digits.length === 10 && digits.startsWith("0")) out.add("254" + digits.slice(1));
  if (digits.length === 9 && /^[17]/.test(digits)) {
    out.add("0" + digits);
    out.add("254" + digits);
  }
  return [...out];
}

function digitsOnly(value: unknown) {
  return String(value || "").replace(/\D/g, "");
}

function mpesaDate(value: unknown) {
  const s = String(value || "").trim();
  if (/^\d{14}$/.test(s)) {
    return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}T${s.slice(8, 10)}:${s.slice(10, 12)}:${s.slice(12, 14)}`;
  }
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}T${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}:${String(d.getSeconds()).padStart(2, "0")}`;
}

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  const accepted = new Response(JSON.stringify({ ResultCode: 0, ResultDesc: "Accepted" }), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
    status: 200,
  });

  try {
    const body = await req.json();
    const transId = body?.TransID;
    if (!transId) return accepted;

    const shortcode = String(body?.BusinessShortCode || "").trim();
    const accountNumber = String(body?.BillRefNumber || "").trim();
    const amount = Number(body?.TransAmount || 0);
    const payerPhone = String(body?.MSISDN || "").trim();
    const payerName = `${body?.FirstName || ""} ${body?.MiddleName || ""} ${body?.LastName || ""}`.replace(/\s+/g, " ").trim();
    const paymentDate = mpesaDate(body?.TransTime);

    if (!shortcode || !amount || amount <= 0) return accepted;

    const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
    const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
    const supabase = createClient(supabaseUrl, supabaseKey);

    const { data: allSettingsRows } = await supabase
      .from("loan_settings")
      .select("business_id, mpesa_auto_confirm, mpesa_shortcode");

    const shortcodeDigits = digitsOnly(shortcode);
    const settingsRows = (allSettingsRows || []).filter((row) =>
      digitsOnly(row.mpesa_shortcode) === shortcodeDigits
    );

    const configuredBusinesses = [...new Set((settingsRows || [])
      .map((row) => String(row.business_id || "").trim())
      .filter(Boolean))];
    let businessId = configuredBusinesses.length === 1 ? configuredBusinesses[0] : null;
    let autoConfirm = configuredBusinesses.length === 1 && !!settingsRows?.[0]?.mpesa_auto_confirm;

    const { data: existingRepayment } = await supabase
      .from("loan_repayments")
      .select("id, loan_id, business_id")
      .or(`payment_reference.eq.${transId},receipt_no.eq.${transId}`)
      .limit(1)
      .maybeSingle();

    if (existingRepayment) {
      return accepted;
    }

    const candidates = [...new Set([
      ...phoneVariants(accountNumber),
      ...phoneVariants(payerPhone),
    ])];

    let client: { id: string; business_id: string; full_name: string | null; account_credit?: number | null } | null = null;
    for (const candidateBusiness of configuredBusinesses) {
      for (const phone of candidates) {
        const { data } = await supabase
          .from("loan_clients")
          .select("id, business_id, full_name, account_credit")
          .eq("business_id", candidateBusiness)
          .eq("phone", phone)
          .limit(1)
          .maybeSingle();
        if (data) {
          client = data;
          break;
        }
      }
      if (client) break;
    }

    if (!client && candidates.length) {
      const tails = [...new Set(candidates.map((candidate) => candidate.replace(/\D/g, "").slice(-9)).filter(Boolean))];
      for (const candidateBusiness of configuredBusinesses) {
        for (const tail of tails) {
          if (client) break;
          if (!tail) continue;
          const { data } = await supabase
            .from("loan_clients")
            .select("id, business_id, full_name, phone, account_credit")
            .eq("business_id", candidateBusiness)
            .ilike("phone", `%${tail}`)
            .limit(1)
            .maybeSingle();
          if (data) client = data;
        }
        if (client) break;
      }
    }

    const accountDigits = digitsOnly(accountNumber);
    if (!client && accountDigits) {
      for (const candidateBusiness of configuredBusinesses) {
        const { data } = await supabase
          .from("loan_clients")
          .select("id, business_id, full_name, phone, id_number, account_credit")
          .eq("business_id", candidateBusiness)
          .eq("id_number", accountDigits)
          .limit(1)
          .maybeSingle();
        if (data) {
          client = data;
          break;
        }
      }
    }

    if (client) {
      businessId = client.business_id;
      autoConfirm = !!(settingsRows || []).find((row) => row.business_id === businessId)?.mpesa_auto_confirm;
    }

    const { data: queue } = await supabase
      .from("mpesa_callback_queue")
      .insert({
        transaction_type: body?.TransactionType || "C2B",
        trans_id: transId,
        trans_time: body?.TransTime,
        trans_amount: amount,
        business_short_code: businessId || shortcode,
        bill_ref_number: accountNumber,
        msisdn: payerPhone,
        first_name: body?.FirstName || "",
        middle_name: body?.MiddleName || "",
        last_name: body?.LastName || "",
        raw_payload: body,
        confirmed: false,
      })
      .select("id")
      .maybeSingle();

    const queueId = queue?.id;

    if (!client || !businessId) {
      if (!businessId) {
        console.error("Could not identify the Bripta business for payment", transId, shortcode, payerPhone);
        return accepted;
      }
      await supabase.from("unmatched_payments").insert({
        amount,
        account_number: accountNumber,
        business_id: businessId,
        mpesa_reference: transId,
        payer_phone: payerPhone,
        payer_name: payerName,
        raw_payload: body,
        resolved: false,
      }).catch(() => {});
      return accepted;
    }

    const { data: loan } = await supabase
      .from("loans")
      .select("id, loan_no, client_id, outstanding_balance, total_paid, total_payable, total_interest, status, business_id, registration_fee_due, registration_fee_paid, processing_fee, processing_fee_paid")
      .eq("business_id", businessId)
      .eq("client_id", client.id)
      .eq("status", "active")
      .gt("outstanding_balance", 0)
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (!loan) {
      const { data: pendingCharge } = await supabase
        .from("bripta_charges")
        .select("id, loan_id, amount, amount_paid, charge_type, status")
        .eq("business_id", businessId)
        .eq("client_id", client.id)
        .eq("charge_type", "processing_fee")
        .in("status", ["pending", "partially_paid"])
        .order("charge_date", { ascending: true })
        .limit(1)
        .maybeSingle();

      if (pendingCharge?.loan_id && autoConfirm) {
        const { data: feeResult, error: feeError } = await supabase.rpc("bripta_record_callback_charge_payment", {
          p_business_id: businessId,
          p_client_id: client.id,
          p_loan_id: pendingCharge.loan_id,
          p_charge_id: pendingCharge.id,
          p_amount: amount,
          p_payment_reference: transId,
          p_payment_date: paymentDate,
          p_payer_name: payerName,
          p_payer_phone: payerPhone,
        });
        if (feeError) throw feeError;
        if (queueId) {
          await supabase
            .from("mpesa_callback_queue")
            .update({
              confirmed: true,
              loan_id: pendingCharge.loan_id,
              repayment_id: feeResult?.repayment_id || null,
              business_short_code: businessId,
            })
            .eq("id", queueId);
        }
        return accepted;
      }

      await supabase.from("unmatched_payments").insert({
        amount,
        account_number: accountNumber,
        business_id: businessId,
        mpesa_reference: transId,
        payer_phone: payerPhone,
        payer_name: payerName,
        raw_payload: body,
        resolved: false,
      }).catch(() => {});
      return accepted;
    }
    if (!autoConfirm) {
      if (queueId) {
        await supabase
          .from("mpesa_callback_queue")
          .update({ business_short_code: businessId, loan_id: loan.id })
          .eq("id", queueId);
      }
      return accepted;
    }

    // Every payment made using the client's registered phone reduces the loan
    // in full. Registration and processing fees are never diverted here.
    const registrationFeePortion = 0;
    const processingFeePortion = 0;
    const currentBalance = Math.max(0, Number(loan.outstanding_balance || 0));
    const loanPortion = Number(Math.min(amount, currentBalance).toFixed(2));
    const creditPortion = Number(Math.max(0, amount - loanPortion).toFixed(2));
    const totalPayable = Number(loan.total_payable || 0);
    const totalInterest = Number(loan.total_interest || 0);
    const interestRatio = totalPayable > 0 && totalInterest > 0 ? totalInterest / totalPayable : 0;
    const interestPortion = Number((loanPortion * interestRatio).toFixed(2));
    const principalPortion = Number((loanPortion - interestPortion).toFixed(2));
    const receiptNo = transId;
    const allocationNote = [
      loanPortion > 0 ? `loan KES ${loanPortion.toFixed(2)}` : "",
      creditPortion > 0 ? `excess KES ${creditPortion.toFixed(2)}` : "",
    ].filter(Boolean).join(", ");

    const { data: repayment, error: repErr } = await supabase
      .from("loan_repayments")
      .insert({
        amount,
        business_id: businessId,
        loan_id: loan.id,
        payment_method: "M-Pesa",
        payment_reference: transId,
        receipt_no: receiptNo,
        mpesa_confirmed: true,
        payment_date: paymentDate,
        sender_name: payerName,
        sender_phone: payerPhone,
        interest_portion: interestPortion,
        principal_portion: principalPortion,
        penalty_portion: 0,
        registration_fee_portion: registrationFeePortion,
        processing_fee_portion: processingFeePortion,
        loan_portion: loanPortion,
        credit_portion: creditPortion,
        notes: `Auto-confirmed via Daraja C2B. Matched by phone. Allocation: ${allocationNote || "none"}. Payer: ${payerName}`,
      })
      .select("id")
      .single();

    if (repErr) throw repErr;

    const { data: schedules } = await supabase
      .from("loan_schedules")
      .select("id, due_date, total_due, total_paid, status")
      .eq("loan_id", loan.id)
      .in("status", ["pending", "partial", "overdue"])
      .order("due_date", { ascending: true });

    let remaining = loanPortion;
    const today = new Date().toISOString().slice(0, 10);
    for (const sched of schedules || []) {
      if (remaining <= 0) break;
      const due = Number(sched.total_due || 0);
      const paid = Number(sched.total_paid || 0);
      const owed = Math.max(0, due - paid);
      if (owed <= 0) continue;

      const apply = Math.min(remaining, owed);
      const newPaid = Number((paid + apply).toFixed(2));
      const newStatus = newPaid >= due ? "paid" : (sched.due_date < today ? "overdue" : "partial");

      await supabase
        .from("loan_schedules")
        .update({
          total_paid: newPaid,
          status: newStatus,
          paid_at: newStatus === "paid" ? paymentDate : null,
        })
        .eq("id", sched.id);

      remaining = Number((remaining - apply).toFixed(2));
    }

    const { data: overdueScheds } = await supabase
      .from("loan_schedules")
      .select("total_due, total_paid, due_date, status")
      .eq("loan_id", loan.id)
      .lt("due_date", today)
      .neq("status", "paid");

    let arrears = 0;
    let oldestDueDate: string | null = null;
    for (const s of overdueScheds || []) {
      const unpaid = Math.max(0, Number(s.total_due || 0) - Number(s.total_paid || 0));
      if (unpaid > 0.01) {
        arrears += unpaid;
        if (!oldestDueDate || s.due_date < oldestDueDate) oldestDueDate = s.due_date;
      }
    }

    const overdueDays = oldestDueDate
      ? Math.max(0, Math.floor((Date.now() - new Date(oldestDueDate).getTime()) / 86400000))
      : 0;
    const newTotalPaid = Number((Number(loan.total_paid || 0) + loanPortion).toFixed(2));
    const newBalance = Math.max(0, Number((Number(loan.outstanding_balance || 0) - loanPortion).toFixed(2)));

    await supabase
      .from("loans")
      .update({
        total_paid: newTotalPaid,
        outstanding_balance: newBalance,
        status: newBalance <= 0 ? "completed" : loan.status,
        arrears_amount: newBalance <= 0 ? 0 : Number(arrears.toFixed(2)),
        overdue_days: newBalance <= 0 ? 0 : overdueDays,
      })
      .eq("id", loan.id);

    if (queueId) {
      await supabase
        .from("mpesa_callback_queue")
        .update({
          confirmed: true,
          loan_id: loan.id,
          repayment_id: repayment.id,
          business_short_code: businessId,
        })
        .eq("id", queueId);
    }

    return accepted;
  } catch (error) {
    console.error("Webhook System Error:", error);
    return accepted;
  }
});
