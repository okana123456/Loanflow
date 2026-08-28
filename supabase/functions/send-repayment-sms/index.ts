import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.38.4";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

function json(body: Record<string, unknown>, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

function kenyaPhone(value: unknown) {
  const digits = String(value || "").replace(/\D/g, "");
  const tail = digits.slice(-9);
  if (!/^[17]\d{8}$/.test(tail)) return null;
  return `254${tail}`;
}

async function callerBusinessId(
  req: Request,
  admin: ReturnType<typeof createClient>,
  serviceKey: string,
) {
  const authorization = req.headers.get("Authorization") || "";
  const token = authorization.replace(/^Bearer\s+/i, "").trim();
  if (!token) return null;
  if (token === serviceKey) return "__service_role__";

  const { data: userData, error: userError } = await admin.auth.getUser(token);
  if (userError || !userData.user) return null;

  let query = admin
    .from("loan_staff")
    .select("business_id")
    .eq("auth_user_id", userData.user.id)
    .eq("is_active", true)
    .limit(1)
    .maybeSingle();
  let { data: staff } = await query;
  if (!staff && userData.user.email) {
    const fallback = await admin
      .from("loan_staff")
      .select("business_id")
      .ilike("email", userData.user.email)
      .eq("is_active", true)
      .limit(1)
      .maybeSingle();
    staff = fallback.data;
  }
  return staff?.business_id || null;
}

function providerUid(payload: any) {
  return String(
    payload?.data?.uid || payload?.data?.id || payload?.data?.message_id ||
    payload?.uid || payload?.id || payload?.message_id || "",
  ).trim() || null;
}

async function deliverClaim(
  admin: ReturnType<typeof createClient>,
  talksasaToken: string,
  sendUrl: string,
  claim: any,
) {
  const recipient = kenyaPhone(claim.recipient);
  if (!recipient) {
    await admin.rpc("bripta_complete_repayment_sms", {
      p_outbox_id: claim.outbox_id,
      p_status: "failed",
      p_error: "Recipient phone number is invalid",
      p_refund: true,
    });
    return { ok: false, code: "invalid_phone" };
  }

  let response: Response;
  try {
    response = await fetch(sendUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${talksasaToken}`,
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify({
        recipient,
        sender_id: claim.sender_id || "BRIPTA",
        type: "plain",
        message: claim.message,
      }),
      signal: AbortSignal.timeout(15000),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await admin.rpc("bripta_complete_repayment_sms", {
      p_outbox_id: claim.outbox_id,
      p_status: "delivery_unknown",
      p_error: `TalkSasa did not return a definite result: ${message}`,
      p_refund: false,
    });
    return { ok: false, code: "delivery_unknown" };
  }

  const responseText = await response.text();
  let providerResponse: any = { raw: responseText };
  try { providerResponse = responseText ? JSON.parse(responseText) : {}; } catch (_) { /* keep raw response */ }
  const providerError = String(providerResponse?.message || providerResponse?.error || `HTTP ${response.status}`);
  const accepted = response.ok && String(providerResponse?.status || "success").toLowerCase() !== "error";

  await admin.rpc("bripta_complete_repayment_sms", {
    p_outbox_id: claim.outbox_id,
    p_status: accepted ? "sent" : "failed",
    p_provider_uid: accepted ? providerUid(providerResponse) : null,
    p_provider_response: providerResponse,
    p_error: accepted ? null : providerError,
    p_refund: !accepted,
  });
  return {
    ok: accepted,
    status: accepted ? "sent" : "failed",
    provider_uid: accepted ? providerUid(providerResponse) : null,
  };
}

serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });
  if (req.method !== "POST") return json({ ok: false, message: "Method not allowed" }, 405);

  const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
  const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
  const talksasaToken = Deno.env.get("TALKSASA_API_TOKEN")?.trim() || "";
  const sendUrl = Deno.env.get("TALKSASA_SEND_URL")?.trim() ||
    "https://bulksms.talksasa.com/api/v3/sms/send";
  if (!supabaseUrl || !serviceKey) return json({ ok: false, message: "Supabase secrets are missing" }, 500);
  if (!talksasaToken) return json({ ok: false, code: "sms_not_configured", message: "TalkSasa token is not configured" }, 503);

  const admin = createClient(supabaseUrl, serviceKey, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
  const callerBusiness = await callerBusinessId(req, admin, serviceKey);
  if (!callerBusiness) return json({ ok: false, message: "Unauthorized" }, 401);

  const body = await req.json().catch(() => ({}));
  const testPhone = String(body?.test_phone || "").trim();
  if (testPhone) {
    if (callerBusiness !== "__service_role__") {
      return json({ ok: false, message: "SMS tests require service-role authorization" }, 403);
    }
    const { data: claim, error: claimError } = await admin.rpc("bripta_claim_test_sms", {
      p_business_id: "BIZ-B3F5E5D9",
      p_phone: testPhone,
    });
    if (claimError) return json({ ok: false, message: claimError.message }, 500);
    if (!claim?.ok || claim?.already_sent) return json(claim || { ok: false }, claim?.ok ? 200 : 400);

    const result = await deliverClaim(admin, talksasaToken, sendUrl, claim);
    return json({ ...result, test: true, recipient: kenyaPhone(testPhone) });
  }

  const requestedRepaymentId = String(body?.repayment_id || "").trim();
  let repaymentIds: string[] = [];

  if (requestedRepaymentId) {
    const { data: outbox } = await admin
      .from("bripta_sms_outbox")
      .select("repayment_id,business_id")
      .eq("repayment_id", requestedRepaymentId)
      .maybeSingle();
    if (!outbox) return json({ ok: false, code: "not_queued", message: "SMS outbox row was not found" }, 404);
    if (callerBusiness !== "__service_role__" && outbox.business_id !== callerBusiness) {
      return json({ ok: false, message: "Forbidden" }, 403);
    }
    repaymentIds = [requestedRepaymentId];
  } else {
    if (callerBusiness === "__service_role__") {
      return json({ ok: false, message: "A business-scoped repayment ID is required" }, 400);
    }
    const { data: pending } = await admin
      .from("bripta_sms_outbox")
      .select("repayment_id")
      .eq("business_id", callerBusiness)
      .in("status", ["queued", "failed", "blocked_no_credit"])
      .not("repayment_id", "is", null)
      .order("queued_at", { ascending: true })
      .limit(10);
    repaymentIds = (pending || []).map((row) => row.repayment_id).filter(Boolean);
  }

  const results: Record<string, unknown>[] = [];
  for (const repaymentId of repaymentIds) {
    const { data: claim, error: claimError } = await admin.rpc("bripta_claim_repayment_sms", {
      p_repayment_id: repaymentId,
    });
    if (claimError) {
      results.push({ repayment_id: repaymentId, ok: false, error: claimError.message });
      continue;
    }
    if (!claim?.ok || claim?.already_sent) {
      results.push({ repayment_id: repaymentId, ...claim });
      continue;
    }

    const result = await deliverClaim(admin, talksasaToken, sendUrl, claim);
    results.push({ repayment_id: repaymentId, ...result });
  }

  return json({ ok: true, processed: results.length, results });
});
