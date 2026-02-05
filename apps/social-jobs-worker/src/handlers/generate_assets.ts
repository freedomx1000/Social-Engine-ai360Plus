/* eslint-disable no-console */
// Generate Assets worker — FINAL FIXED for OpenAI Responses API
// (based on your Generate_Assets.txt)
// ---------------------------------------------------------------

import {getSupabaseAdmin} from "../supabaseAdmin.js";

const supabaseAdmin = getSupabaseAdmin();

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini"; // usa el que tengas disponible
const OPENAI_BASE = process.env.OPENAI_BASE_URL || "https://api.openai.com/v1";

if (!OPENAI_API_KEY) {
  throw new Error("Missing OPENAI_API_KEY");
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

async function openaiResponsesJSON({
  system,
  user,
  schema,
  temperature = 0.2,
}: {
  system: string;
  user: string;
  schema: any;
  temperature?: number;
}) {
  const resp = await fetch(`${OPENAI_BASE}/responses`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: OPENAI_MODEL,
      temperature,

      // ✅ FIX (Responses API): response_format -> text.format
      // Requiere: text.format.name (además de schema/strict)
      text: {
        format: {
          type: "json_schema",
          name: schema?.name ?? "social_generate_assets",
          schema: schema?.schema ?? schema,
          strict: schema?.strict ?? true,
        },
      },

      input: [
        { role: "system", content: system },
        { role: "user", content: user },
      ],
    }),
  });

  const data = await resp.json().catch(() => null);
  if (!resp.ok) {
    const err = data?.error ? safeJson(data.error) : safeJson(data);
    throw new Error(`OpenAI error ${resp.status}: ${err}`);
  }

  // Responses API: output_text suele venir ya “limpio”
  const textOut =
    data?.output_text ??
    data?.output?.[0]?.content?.find?.((c: any) => c?.type === "output_text")?.text ??
    data?.output?.[0]?.content?.[0]?.text;

  if (!textOut) throw new Error(`OpenAI returned no text. payload=${safeJson(data)}`);

  // Debería ser JSON (porque pedimos json_schema)
  let parsed: any = null;
  try {
    parsed = JSON.parse(textOut);
  } catch {
    throw new Error(`OpenAI returned non-JSON output. text=${String(textOut).slice(0, 500)}`);
  }

  return parsed;
}

async function getVerticalProfile(vertical_key: string) {
  const t0 = Date.now();
  const { data, error } = await supabaseAdmin
    .from("social_vertical_profiles")
    .select("vertical_key, prompt_preamble, style_rules, image_style, hashtag_seed")
    .eq("vertical_key", vertical_key)
    .maybeSingle();
  const ms = Date.now() - t0;
  console.log(`[vertical_profile] vertical=${vertical_key} hit=${error ? 0 : 1} ms=${ms}`);
  if (error) throw error;
  return data;
}

function buildPrompt({
  channel,
  vertical_key,
  context,
  profile,
}: {
  channel: string;
  vertical_key: string;
  context: string;
  profile: any;
}) {
  const style_rules = profile?.style_rules ? safeJson(profile.style_rules) : "{}";
  const image_style = pickString(profile?.image_style, "");
  const hashtag_seed = Array.isArray(profile?.hashtag_seed) ? profile.hashtag_seed.join(", ") : "";

  return `
VERTICAL: ${vertical_key}
CHANNEL: ${channel}

CONTEXT:
${context}

STYLE_RULES(JSON):
${style_rules}

IMAGE_STYLE:
${image_style}

HASHTAG_SEED:
${hashtag_seed}

Return JSON strictly matching schema:
- title: short
- hook: 1 sentence hook
- caption: 2-4 sentences
- hashtags: array of strings without '#'
- cta: short CTA
- image_prompts: array of 1-3 prompts
`.trim();
}

const schema = {
  name: "social_generate_assets",
  strict: true,
  schema: {
    type: "object",
    additionalProperties: false,
    properties: {
      title: { type: "string" },
      hook: { type: "string" },
      caption: { type: "string" },
      hashtags: { type: "array", items: { type: "string" } },
      cta: { type: "string" },
      image_prompts: { type: "array", items: { type: "string" } },
    },
    required: ["title", "hook", "caption", "hashtags", "cta", "image_prompts"],
  },
};

async function writeOutput({
  activity_id,
  org_id,
  lead_id,
  channel,
  vertical_key,
  output,
  trace_id,
}: {
  activity_id: string;
  org_id: string | null;
  lead_id: string | null;
  channel: string;
  vertical_key: string;
  output: any;
  trace_id: string;
}) {
  const hashtags = Array.isArray(output?.hashtags) ? output.hashtags : [];
  const hashtags_norm = hashtags
    .map((s: any) => String(s ?? "").trim().replace(/^#/, ""))
    .filter(Boolean)
    .slice(0, 20);

  const payload = {
    activity_id,
    trace_id,
    channel,
    vertical_key,
  };

  const row = {
    org_id,
    lead_id,
    status: "draft",
    channel,
    vertical_key,
    title: pickString(output?.title, ""),
    hook: pickString(output?.hook, ""),
    caption: pickString(output?.caption, ""),
    hashtags: hashtags_norm,
    cta: pickString(output?.cta, ""),
    image_prompts: Array.isArray(output?.image_prompts) ? output.image_prompts.slice(0, 3) : [],
    meta: payload,
  };

  const { error } = await supabaseAdmin.from("social_outputs").insert(row);
  if (error) throw error;
}

async function fetchActivityContext(activity_id: string) {
  const { data, error } = await supabaseAdmin
    .from("crm_lead_activity")
    .select("id, org_id, lead_id, payload, kind, message, meta, created_at")
    .eq("id", activity_id)
    .maybeSingle();
  if (error) throw error;
  return data;
}

export async function generate_assets(job: any) {
  const activity_id = job?.activity_id as string;
  const trace_id = job?.last_trace_id || job?.id || crypto.randomUUID?.() || String(Date.now());

  const payload = job?.payload ?? {};
  const meta = job?.meta ?? payload?.meta ?? {};

  // channel + vertical (prioridad: payload.meta -> payload -> default)
  const channel = pickString(meta?.channel ?? payload?.channel, "multi");
  const vertical_key = pickString(meta?.vertical_key ?? payload?.vertical_key, "general");

  // context base (si tu job ya trae cosas extra, se vuelcan)
  const lead_name = pickString(payload?.lead_name ?? payload?.leadName ?? meta?.lead_name, "unknown");
  const topic = pickString(payload?.topic ?? meta?.topic, "none");
  const offer = pickString(payload?.offer ?? meta?.offer, "none");
  const brief = pickString(payload?.brief ?? meta?.brief, "");

  const context = `
lead_name: ${lead_name}
topic: ${topic}
offer: ${offer}
brief: ${brief}
payload_json: ${safeJson(payload)}
`.trim();

  console.log(
    `[generate_assets] start trace=${trace_id} activity_id=${activity_id} vertical=${vertical_key} channel=${channel}`
  );

  // vertical profile
  const profile = await getVerticalProfile(vertical_key);
  const systemPreamble = pickString(
    profile?.prompt_preamble,
    "You are an expert social media copywriter. Output only valid JSON."
  );

  // activity context (org_id / lead_id)
  const act = await fetchActivityContext(activity_id);
  const org_id = act?.org_id ?? null;
  const lead_id = act?.lead_id ?? null;

  // prompt
  const prompt = buildPrompt({ channel, vertical_key, context, profile });

  // call OpenAI
  const output = await openaiResponsesJSON({
    system: systemPreamble,
    user: prompt,
    schema,
    temperature: 0.2,
  });

  // write output row
  await writeOutput({
    activity_id,
    org_id,
    lead_id,
    channel,
    vertical_key,
    output,
    trace_id,
  });

  return { ok: true, trace_id };
}
