// src/handlers/generate_assets.ts
// Generate Assets worker — Supabase Admin self-contained + OpenAI Responses API (text.format)
// --------------------------------------------------------------------------------------

import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;

const OPENAI_API_KEY = process.env.OPENAI_API_KEY!;
const OPENAI_BASE = process.env.OPENAI_BASE_URL || "https://api.openai.com/v1";
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
}
if (!OPENAI_API_KEY) {
  throw new Error("Missing OPENAI_API_KEY");
}

function supabaseAdmin() {
  return createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
}

function safeJson(v: any) {
  try {
    return JSON.stringify(v);
  } catch {
    return "{}";
  }
}

function pickString(...vals: Array<any>) {
  for (const v of vals) {
    if (typeof v === "string" && v.trim()) return v.trim();
  }
  return "";
}

// ---- OpenAI Responses API (JSON Schema via text.format) -------------------------------

async function openaiResponsesJSON(args: {
  system: string;
  user: string;
  schema: any; // JSON Schema (the "schema" object)
  temperature?: number;
}) {
  const { system, user, schema, temperature = 0.2 } = args;

  // IMPORTANT: Responses API requires text.format.name (your current error)
  const body = {
    model: OPENAI_MODEL,
    temperature,
    text: {
      format: {
        type: "json_schema",
        name: "social_assets_v1",
        schema, // <-- pure JSON schema here
      },
    },
    input: [
      { role: "system", content: system },
      { role: "user", content: user },
    ],
  };

  const resp = await fetch(`${OPENAI_BASE}/responses`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  const data = await resp.json().catch(() => null);
  if (!resp.ok) {
    const err = data?.error ? safeJson(data.error) : safeJson(data);
    throw new Error(`OpenAI error ${resp.status}: ${err}`);
  }

  const textOut =
    data?.output_text ??
    data?.output?.[0]?.content?.find?.((c: any) => c?.type === "output_text")?.text ??
    data?.output?.[0]?.content?.[0]?.text;

  if (!textOut) throw new Error(`OpenAI returned no text. payload=${safeJson(data)}`);

  try {
    return JSON.parse(textOut);
  } catch {
    throw new Error(`OpenAI returned non-JSON output. text=${String(textOut).slice(0, 800)}`);
  }
}

// ---- Main exported handler ------------------------------------------------------------
// The worker loop imports { generate_assets } from "./handlers/generate_assets.js"
// So we MUST export generate_assets.

export async function generate_assets(job: any) {
  const sb = supabaseAdmin();

  // job fields (be tolerant)
  const job_id = job?.id ?? job?.job_id;
  const activity_id = job?.activity_id ?? job?.activityId;
  const payload = job?.payload ?? {};
  const meta = job?.meta ?? payload?.meta ?? {};

  const channel = pickString(meta?.channel, payload?.channel, "multi");
  const vertical_key = pickString(meta?.vertical_key, payload?.vertical_key, "general");

  if (!activity_id) {
    throw new Error("Missing activity_id on job");
  }

  // 1) Load activity context
  const { data: act, error: actErr } = await sb
    .from("crm_lead_activity")
    .select("id, org_id, lead_id, message, meta, payload, created_at")
    .eq("id", activity_id)
    .maybeSingle();

  if (actErr) throw new Error(`crm_lead_activity error: ${actErr.message}`);
  if (!act) throw new Error("crm_lead_activity not found");

  const actPayload = act.payload ?? {};
  const actMeta = act.meta ?? {};

  const lead_name = pickString(actMeta?.lead_name, actPayload?.lead_name, actPayload?.leadName, "unknown");
  const topic = pickString(actMeta?.topic, actPayload?.topic, "none");
  const offer = pickString(actMeta?.offer, actPayload?.offer, "none");
  const brief = pickString(actMeta?.brief, actPayload?.brief, "");

  // 2) Load vertical profile (prompt_preamble)
  const { data: vp, error: vpErr } = await sb
    .from("social_vertical_profiles")
    .select("vertical_key, prompt_preamble, locale, tone, audience")
    .eq("vertical_key", vertical_key)
    .maybeSingle();

  if (vpErr) throw new Error(`social_vertical_profiles error: ${vpErr.message}`);

  const prompt_preamble = pickString(vp?.prompt_preamble, "Eres un copywriter experto en social media.");

  // 3) Build prompt
  const system = `${prompt_preamble}
Devuelve SOLO JSON válido que cumpla el schema. Nada de texto extra.`;

  const userPrompt = `
lead_name: ${lead_name}
topic: ${topic}
offer: ${offer}
brief: ${brief}
channel: ${channel}
vertical_key: ${vertical_key}
activity_message: ${pickString(act.message, "")}
activity_meta_json: ${safeJson(actMeta)}
activity_payload_json: ${safeJson(actPayload)}
`.trim();

  // 4) Schema expected by your DB columns (title, hook, caption, hashtags, cta, image_prompts, assets)
const schema = {
  type: "object",
  additionalProperties: false,
  required: ["title", "hook", "caption", "hashtags", "cta", "image_prompts"],
  properties: {
    title: { type: "string" },
    hook: { type: "string" },
    caption: { type: "string" },
    cta: { type: "string" },
    hashtags: {
      type: "array",
      items: { type: "string" },
      additionalProperties: false,
    },
    image_prompts: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        required: ["prompt", "style"],
        properties: {
          prompt: { type: "string" },
          style: { type: "string" },
        },
      },
      minItems: 1,
    },
    assets: {
      type: "object",
      additionalProperties: false,
      properties: {
        thumbnail_url: { type: "string" },
        video_url: { type: "string" },
      },
    },
  },
};

  // 5) OpenAI call
  const out = await openaiResponsesJSON({
    system,
    user: userPrompt,
    schema,
    temperature: 0.4,
  });

  // Normalize
  const title = pickString(out?.title, "");
  const hook = pickString(out?.hook, "");
  const caption = pickString(out?.caption, "");
  const cta = pickString(out?.cta, "");
  const hashtags = Array.isArray(out?.hashtags) ? out.hashtags : [];
  const image_prompts = out?.image_prompts ?? {};
  const assets = out?.assets ?? {};

  // 6) Insert social_outputs
  const metaOut = {
    ...((payload?.meta ?? {}) as any),
    activity_id,
    job_id,
    vertical_key,
    channel,
    source: "openai",
  };

  const { error: insErr } = await sb.from("social_outputs").insert({
    org_id: act.org_id,
    lead_id: act.lead_id,
    vertical_key,
    channel,
    status: "draft",
    title,
    hook,
    caption,
    cta,
    hashtags,
    image_prompts,
    assets,
    meta: metaOut,
    activity_id: String(activity_id), // your column is text in screenshots
  });

  if (insErr) {
    throw new Error(`social_outputs insert error: ${insErr.message}`);
  }

  return { ok: true };
}
