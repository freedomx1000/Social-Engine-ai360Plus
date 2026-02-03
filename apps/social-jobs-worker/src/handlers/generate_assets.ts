import { supabase } from "../supabase.js";
import type { SocialJobRow, LeadRow, LeadActivityRow } from "../types.js";

export async function handleGenerateAssets(job: SocialJobRow) {
  // 1) Cargar lead
  const { data: lead, error: leadErr } = await supabase
    .from("leads")
    .select("*")
    .eq("id", job.lead_id)
    .eq("org_id", job.org_id)
    .is("deleted_at", null)
    .maybeSingle<LeadRow>();

  if (leadErr || !lead) {
    throw new Error(`Lead not found or access denied. leadErr=${leadErr?.message ?? "null"}`);
  }

  // 2) Cargar actividad origen (crm_lead_activity)
  const { data: act, error: actErr } = await supabase
    .from("crm_lead_activity")
    .select("*")
    .eq("id", job.activity_id)
    .eq("org_id", job.org_id)
    .maybeSingle<LeadActivityRow>();

  if (actErr || !act) {
    throw new Error(`Activity not found. actErr=${actErr?.message ?? "null"}`);
  }

  // 3) Generar resultado "stub" (luego lo conectamos a Social Engine real)
  const assets_stub = {
    kind: "generate_assets_stub",
    lead: { id: lead.id, name: lead.name, email: lead.email },
    activity: { id: act.id, kind: act.kind, type: act.type },
    generated_at: new Date().toISOString()
  };

  // 4) Registrar nueva actividad de sistema (para auditoría)
  const { error: insErr } = await supabase.from("crm_lead_activity").insert({
    org_id: job.org_id,
    lead_id: job.lead_id,
    kind: "system",
    type: "assets_generated",
    message: "Assets generated (stub).",
    meta: { job_id: job.id, source_activity_id: job.activity_id },
    payload: assets_stub
  });

  if (insErr) throw new Error(`Failed to insert activity: ${insErr.message}`);

  // 5) Actualizar lead.last_activity_*
  const { error: updErr } = await supabase
    .from("leads")
    .update({
      last_activity_at: new Date().toISOString(),
      last_activity_kind: "assets_generated"
    })
    .eq("id", lead.id)
    .eq("org_id", job.org_id);

  if (updErr) throw new Error(`Failed to update lead last_activity: ${updErr.message}`);

  return { ok: true, assets_stub };
}
