import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.4";

const BUSINESS_ID = "BIZ-B3F5E5D9";
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, apikey, content-type, x-client-info",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};
const roles = new Set(["admin", "branch_manager", "loan_officer", "cashier"]);
const permissionNames = [
  "view_accounting", "view_expenses", "initiate_expenses", "approve_expenses",
  "manage_assets", "manage_branches", "transfer_portfolios", "adjust_charges_excess",
];

function json(body: Record<string, unknown>, status = 200) {
  return new Response(JSON.stringify(body), {
    status, headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });
  if (req.method !== "POST") return json({ ok: false, message: "Use POST." }, 405);

  const supabaseUrl = Deno.env.get("SUPABASE_URL") || "";
  const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "";
  const token = /^Bearer (.+)$/i.exec(req.headers.get("authorization") || "")?.[1];
  if (!supabaseUrl || !serviceKey) return json({ ok: false, message: "Supabase secrets are missing." }, 500);
  if (!token) return json({ ok: false, message: "Sign in again before creating staff." }, 401);

  const admin = createClient(supabaseUrl, serviceKey, { auth: { persistSession: false, autoRefreshToken: false } });
  let createdAuthId: string | null = null;
  let createdStaffId: string | null = null;

  try {
    const { data: { user }, error: userError } = await admin.auth.getUser(token);
    if (userError || !user) return json({ ok: false, message: "Your login session has expired. Sign in again." }, 401);

    let { data: actor, error: actorError } = await admin.from("loan_staff")
      .select("id, business_id, role, is_active")
      .eq("business_id", BUSINESS_ID).eq("auth_user_id", user.id).maybeSingle();
    if (actorError) throw actorError;
    if (!actor && user.email) {
      const fallback = await admin.from("loan_staff")
        .select("id, business_id, role, is_active, auth_user_id")
        .eq("business_id", BUSINESS_ID).ilike("email", user.email).maybeSingle();
      if (fallback.error) throw fallback.error;
      if (!fallback.data?.auth_user_id) actor = fallback.data;
    }
    if (!actor?.is_active || !String(actor.role || "").split(",").map((r: string) => r.trim()).includes("admin")) {
      return json({ ok: false, message: "Only an active Bripta administrator can create staff." }, 403);
    }

    const body = await req.json();
    if (body.action === "retire_staff") {
      const staffId = String(body.staff_id || "");
      if (!/^[0-9a-f]{8}-[0-9a-f-]{27,}$/i.test(staffId) || staffId === actor.id) {
        return json({ ok: false, message: "Choose another Bripta staff member to remove." }, 400);
      }
      const { data: target, error: targetError } = await admin.from("loan_staff")
        .select("id, business_id, name, role, is_active, auth_user_id")
        .eq("id", staffId).eq("business_id", BUSINESS_ID).maybeSingle();
      if (targetError) throw targetError;
      if (!target) return json({ ok: false, message: "Bripta staff member was not found." }, 404);
      if (target.is_active === false && !target.auth_user_id) {
        return json({ ok: true, staff_id: staffId, message: "Staff access is already inactive." });
      }

      const [clients, loans, applications] = await Promise.all([
        admin.from("loan_clients").select("id", { count: "exact", head: true })
          .eq("business_id", BUSINESS_ID).eq("loan_officer_id", staffId),
        admin.from("loans").select("id", { count: "exact", head: true })
          .eq("business_id", BUSINESS_ID).eq("loan_officer_id", staffId).eq("status", "active"),
        admin.from("loan_applications").select("id", { count: "exact", head: true })
          .eq("business_id", BUSINESS_ID).eq("loan_officer_id", staffId)
          .in("status", ["pending", "approved", "submitted", "under_review"]),
      ]);
      for (const result of [clients, loans, applications]) if (result.error) throw result.error;
      if ((clients.count || 0) + (loans.count || 0) + (applications.count || 0) > 0) {
        return json({ ok: false, message: "Transfer this staff member's clients, active loans and pending applications before removing access." }, 409);
      }
      if (target.auth_user_id) {
        const { data: shared, error: sharedError } = await admin.from("loan_staff")
          .select("id").eq("auth_user_id", target.auth_user_id).neq("id", staffId).limit(1);
        if (sharedError) throw sharedError;
        if (shared?.length) return json({ ok: false, message: "This login is linked to another staff record. Review it before removing access." }, 409);
      }

      // Keep the staff row for historical loans, repayments and audit entries.
      // Unlink before deleting Auth: an unknown foreign-key delete action must
      // never cascade into this historical staff row.
      const { error: disableError } = await admin.from("loan_staff")
        .update({ is_active: false, auth_user_id: null })
        .eq("id", staffId).eq("business_id", BUSINESS_ID);
      if (disableError) throw disableError;
      if (target.auth_user_id) {
        const { error: deleteError } = await admin.auth.admin.deleteUser(target.auth_user_id);
        if (deleteError) {
          const { error: relinkError } = await admin.from("loan_staff")
            .update({ auth_user_id: target.auth_user_id }).eq("id", staffId).eq("business_id", BUSINESS_ID);
          if (relinkError) console.error("Retired staff login relink failed", relinkError);
          return json({ ok: false, message: relinkError
            ? "Staff was deactivated, but login removal needs administrator review."
            : `Staff was deactivated, but login removal failed: ${deleteError.message}. Retry Remove Access.` }, 500);
        }
      }
      const { error: auditError } = await admin.from("loan_audit_log").insert({
        business_id: BUSINESS_ID, user_id: actor.id, action: "staff_retired",
        table_name: "loan_staff", record_id: staffId,
        old_value: { name: target.name, role: target.role, is_active: true },
        new_value: { is_active: false, login_removed: Boolean(target.auth_user_id) },
      });
      if (auditError) console.error("Staff retirement audit failed", auditError);
      return json({ ok: true, staff_id: staffId,
        message: "Staff access removed; historical records retained." });
    }
    if (body.action === "update_staff" || body.action === "promote_admin") {
      const staffId = String(body.staff_id || "");
      if (!/^[0-9a-f]{8}-[0-9a-f-]{27,}$/i.test(staffId) ||
          (body.action === "promote_admin" && staffId === actor.id)) {
        return json({ ok: false, message: "Choose a Bripta branch manager to promote." }, 400);
      }
      const { data: target, error: targetError } = await admin.from("loan_staff")
        .select("id, business_id, name, email, phone, role, is_active, branch_id")
        .eq("id", staffId).eq("business_id", BUSINESS_ID).maybeSingle();
      if (targetError) throw targetError;
      if (!target) return json({ ok: false, message: "Bripta staff member was not found." }, 404);
      const promoting = body.action === "promote_admin";
      if (promoting && !target.is_active) {
        return json({ ok: false, message: "Activate this branch manager before promoting them." }, 409);
      }
      if (promoting && !String(target.role || "").split(",").map((r: string) => r.trim()).includes("branch_manager")) {
        return json({ ok: false, message: "This staff member is no longer a branch manager. Refresh Staff and try again." }, 409);
      }
      const nextRoles = promoting ? ["admin"] : String(body.role || "").split(",").map((r: string) => r.trim()).filter(Boolean);
      const nextName = promoting ? target.name : String(body.name || "").trim();
      const nextBranch = promoting ? target.branch_id : String(body.branch_id || "").trim();
      if (!nextName || !nextRoles.length || nextRoles.some((r: string) => !roles.has(r)) ||
          new Set(nextRoles).size !== nextRoles.length || !nextBranch) {
        return json({ ok: false, message: "Enter a name, branch and valid roles." }, 400);
      }
      if (staffId === actor.id && (!nextRoles.includes("admin") || body.is_active === false)) {
        return json({ ok: false, message: "You cannot remove your own administrator access." }, 400);
      }
      if (!promoting && String(body.email || "").trim().toLowerCase() !== String(target.email || "").trim().toLowerCase()) {
        return json({ ok: false, message: "Staff login email cannot be changed here. Keep the current email and save again." }, 400);
      }
      const { data: assignedBranch, error: assignedBranchError } = await admin.from("bripta_branches")
        .select("id").eq("id", nextBranch).eq("business_id", BUSINESS_ID).eq("is_active", true).maybeSingle();
      if (assignedBranchError) throw assignedBranchError;
      if (!assignedBranch) return json({ ok: false, message: "Choose an active Bripta branch." }, 400);
      const update = promoting
        ? { role: "admin" }
        : { name: nextName, phone: String(body.phone || "").trim() || null,
            role: nextRoles.join(","), is_active: body.is_active !== false, branch_id: nextBranch };
      const { error: updateError } = await admin.from("loan_staff").update(update)
        .eq("id", staffId).eq("business_id", BUSINESS_ID);
      if (updateError) throw updateError;
      if (!promoting) {
        const permissions: Record<string, unknown> = {
          staff_id: staffId, business_id: BUSINESS_ID, branch_id: nextBranch,
          updated_by: actor.id, updated_at: new Date().toISOString(),
        };
        for (const key of permissionNames) permissions[key] = body.permissions?.[key] === true;
        const { error: permissionError } = await admin.from("bripta_staff_permissions")
          .upsert(permissions, { onConflict: "staff_id" });
        if (permissionError) {
          await admin.from("loan_staff").update({ name: target.name, phone: target.phone,
            role: target.role, is_active: target.is_active, branch_id: target.branch_id })
            .eq("id", staffId).eq("business_id", BUSINESS_ID);
          throw permissionError;
        }
      }
      return json({ ok: true, staff_id: staffId, role: nextRoles.join(","),
        message: promoting ? "Branch manager promoted to administrator." : "Staff saved successfully." });
    }
    const name = String(body.name || "").trim();
    const email = String(body.email || "").trim().toLowerCase();
    const password = String(body.password || "");
    const phone = String(body.phone || "").trim() || null;
    const branchId = String(body.branch_id || "").trim();
    const selectedRoles = String(body.role || "").split(",").map((r: string) => r.trim()).filter(Boolean);
    if (!name || name.length > 160 || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) || !branchId ||
        password.length < 8 || password.length > 128 || !selectedRoles.length ||
        selectedRoles.some((r: string) => !roles.has(r)) || new Set(selectedRoles).size !== selectedRoles.length) {
      return json({ ok: false, message: "Enter a name, valid email, assigned branch, valid roles and password of at least 8 characters." }, 400);
    }

    const { data: branch, error: branchError } = await admin.from("bripta_branches")
      .select("id").eq("id", branchId).eq("business_id", BUSINESS_ID).eq("is_active", true).maybeSingle();
    if (branchError) throw branchError;
    if (!branch) return json({ ok: false, message: "Choose an active Bripta branch." }, 400);

    const { data: existingStaff, error: existingError } = await admin.from("loan_staff")
      .select("id").eq("business_id", BUSINESS_ID).ilike("email", email).limit(1);
    if (existingError) throw existingError;
    if (existingStaff?.length) return json({ ok: false, message: "This email already has a Bripta staff record. Edit that record or send a password reset link." }, 409);

    // Create the login as confirmed so the password works immediately. Never
    // use browser signUp here: it may leave an Auth user when staff RLS rejects.
    const { data: authData, error: createError } = await admin.auth.admin.createUser({
      email, password, email_confirm: true, user_metadata: { full_name: name },
    });
    if (createError || !authData?.user?.id) {
      if (/already|registered|exists/i.test(createError?.message || "")) {
        return json({ ok: false, message: "This email already has a login. No staff record or password was changed. Check the existing account before retrying." }, 409);
      }
      throw createError || new Error("Could not create the login.");
    }
    createdAuthId = authData.user.id;

    const { data: staff, error: staffError } = await admin.from("loan_staff").insert({
      business_id: BUSINESS_ID, auth_user_id: createdAuthId, name, email, phone,
      role: selectedRoles.join(","), is_active: body.is_active !== false, branch_id: branch.id,
    }).select("id").single();
    if (staffError) throw staffError;
    createdStaffId = staff.id;

    const permissions: Record<string, unknown> = {
      staff_id: staff.id, business_id: BUSINESS_ID, branch_id: branch.id,
      updated_by: actor.id, updated_at: new Date().toISOString(),
    };
    for (const key of permissionNames) permissions[key] = body.permissions?.[key] === true;
    const { error: permissionError } = await admin.from("bripta_staff_permissions").upsert(permissions, { onConflict: "staff_id" });
    if (permissionError) throw permissionError;

    return json({ ok: true, staff_id: staff.id, message: "Staff login created. The initial password is ready to use." });
  } catch (error) {
    // A failed staff insert must not leave a new login that cannot sign in.
    let cleanupError: string | null = null;
    if (createdStaffId) {
      const removed = await admin.from("loan_staff").delete().eq("id", createdStaffId).eq("business_id", BUSINESS_ID);
      if (removed.error) cleanupError = `Staff rollback failed: ${removed.error.message}`;
    }
    if (createdAuthId && !cleanupError) {
      const removed = await admin.auth.admin.deleteUser(createdAuthId);
      if (removed.error) cleanupError = `Login rollback failed: ${removed.error.message}`;
    }
    console.error("bripta-create-staff failed", error);
    if (cleanupError) console.error("bripta-create-staff cleanup failed", cleanupError);
    return json({ ok: false, message: cleanupError
      ? `Staff creation needs administrator review. ${cleanupError}. Do not retry this email yet.`
      : error instanceof Error ? error.message : "Could not create staff." }, 500);
  }
});
