import { supabase } from "../supabase.js";
import type { CrmLeadActivityRow, SocialJobRow, SocialOutputInsert } from "../types.js";

/**
 * Genera un borrador en social_outputs a partir de crm_lead_activity (+ payload/meta)
 * SIN IA todavía: solo estructura y texto base.
 */
export async function handleGenerateAssets(job: SocialJobRow) {
  if (!job.activity_id) {
    return { ok: false, reason: "missing_activity_id" as const };
  }

  const { data: act, error: actErr } = await supabase
    .from("crm_lead_activity")
    .select("*")
    .eq("id", job.activity_id)
    .single<CrmLeadActivityRow>();

  if (actErr || !act) {
    return { ok: false, reason: "activity_not_found" as const, detail: actErr?.message };
  }

  // Derivamos algunos campos base desde payload/meta
  const p = act.payload ?? {};
  const m = act.meta ?? {};

  const verticalKey: string | null =
    p.vertical_key ?? m.vertical_key ?? p.vertical ?? m.vertical ?? null;

  const channel: string = p.channel ?? m.channel ?? "multi";

  const hook: string | null =
    p.hook ?? m.hook ?? (act.message ? `Idea: ${act.message}` : null);

  const cta: string | null = p.cta ?? m.cta ?? "Escríbeme y te digo cómo hacerlo.";

  // Hashtags: aceptamos array o string “#a #b”
  let hashtags: string[] | null = null;
  const rawHash = p.hashtags ?? m.hashtags;
  if (Array.isArray(rawHash)) hashtags = rawHash.map(String);
  else if (typeof rawHash === "string") hashtags = rawHash.split(/\s+/).filter(Boolean);

  const title: string | null = p.title ?? m.title ?? null;
  const caption: string | null = p.caption ?? m.caption ?? null;

  // prompts de imagen: por ahora dejamos algo mínimo en jsonb
  const image_prompts = p.image_prompts ?? m.image_prompts ?? [
    { prompt: "Foto estilo marca, fondo limpio, enfoque en producto/servicio, alta calidad" }
  ];

  const insert: SocialOutputInsert = {
    org_id: act.org_id,
    lead_id: act.lead_id,
    vertical_key: verticalKey,
    status: "draft",
    channel,
    title,
    caption,
    hashtags,
    hook,
    cta,
    image_prompts,
    assets: [],
    meta: {
      source: "crm_lead_activity",
      activity_id: act.id,
      kind: act.kind,
      job_id: job.id
    }
  };

  const { data: out, error: outErr } = await supabase
    .from("social_outputs")
    .insert(insert)
    .select("id")
    .single();

  if (outErr) {
    return { ok: false, reason: "insert_social_outputs_failed" as const, detail: outErr.message };
  }

  return { ok: true, social_output_id: out?.id as string };
}
