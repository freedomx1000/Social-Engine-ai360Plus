/* apps/social-jobs-worker/src/handlers/generate_assets.ts */

import OpenAI from "openai";
import { supabase } from "../supabase.js";

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// Cambia si quieres otro modelo
const DEFAULT_MODEL = process.env.OPENAI_MODEL || "gpt-5-mini";

function pickString(...vals: any[]): string {
  for (const v of vals) {
    if (typeof v === "string" && v.trim()) return v.trim();
  }
  return "";
}

function safeJson(v: any): string {
  try {
    return JSON.stringify(v ?? null);
  } catch {
    return "null";
  }
}

function normalizeHashtags(input: any): string[] {
  if (!input) return [];
  const arr = Array.isArray(input) ? input : String(input).split(/[,\n]/g);
  return arr
    .map((s) => String(s).trim())
    .filter(Boolean)
    .map((s) => (s.startsWith("#") ? s : `#${s}`))
    .slice(0, 20);
}

async function getVerticalProfile(vertical_key: string) {
  const { data, error } = await supabase
    .from("social_vertical_profiles")
    .select("vertical_key, prompt_preamble, tone, audience, style_rules, hashtag_seed")
    .eq("vertical_key", vertical_key)
    .maybeSingle();

  if (error) throw new Error(`vertical_profile error: ${error.message}`);
  return data;
}

async function getActivity(activity_id: string) {
  const { data, error } = await supabase
    .from("crm_lead_activity")
    .select("id, org_id, lead_id, kind, message, meta, payload, created_at")
    .eq("id", activity_id)
    .single();

  if (error) throw new Error(`activity lookup error: ${error.message}`);
  return data;
}

async function insertOutput(params: any) {
  const { data, error } = await supabase
    .from("social_outputs")
    .insert(params)
    .select("id")
    .single();

  if (error) throw new Error(`insert social_outputs error: ${error.message}`);
  return data;
}

async function updateJob(id: string, patch: any) {
  const { error } = await supabase.from("social_jobs").update(patch).eq("id", id);
  if (error) throw new Error(`update social_jobs error: ${error.message}`);
}

export async function handleGenerateAssets(job: any) {
  const start = Date.now();

  // job.payload puede traer meta extra; job.meta igual
  const payload = job?.payload ?? {};
  const meta = job?.meta ?? payload?.meta ?? {};

  // channel + vertical_key (prioridad: payload.meta → payload → job → default)
  const channel = pickString(meta?.channel ?? payload?.channel, job?.channel, "multi");
  const vertical_key = pickString(meta?.vertical_key ?? payload?.vertical_key, "general");

  const activity_id = pickString(job?.activity_id, meta?.activity_id, payload?.activity_id);
  const trace_id = pickString(job?.last_trace_id, meta?.trace_id, payload?.trace_id, cryptoRandomShort());

  if (!OPENAI_API_KEY) throw new Error("Missing OPENAI_API_KEY");
  if (!activity_id) throw new Error("Missing activity_id on job");

  console.log(
    `[generate_assets] start trace=${trace_id} activity_id=${activity_id} vertical=${vertical_key} channel=${channel}`
  );

  // Marca job running
  await updateJob(job.id, {
    status: "running",
    started_at: new Date().toISOString(),
    last_error: null,
    last_trace_id: trace_id,
  });

  // Contexto
  const activity = await getActivity(activity_id);

  const lead_name = pickString(payload?.lead_name ?? payload?.leadName ?? meta?.lead_name, "unknown");
  const topic = pickString(payload?.topic ?? meta?.topic, "no");
  const offer = pickString(payload?.offer ?? meta?.offer, "no");
  const brief = pickString(payload?.brief ?? meta?.brief, "");

  const context = `
lead_name: ${lead_name}
topic: ${topic}
offer: ${offer}
brief: ${brief}
activity_kind: ${pickString(activity?.kind, "")}
activity_message: ${pickString(activity?.message, "")}
activity_meta_json: ${safeJson(activity?.meta)}
activity_payload_json: ${safeJson(activity?.payload)}
job_payload_json: ${safeJson(payload)}
`.trim();

  const vprof = await getVerticalProfile(vertical_key);
  const prompt_preamble = pickString(vprof?.prompt_preamble, "");
  const tone = pickString(vprof?.tone, "");
  const audience = pickString(vprof?.audience, "");
  const style_rules = vprof?.style_rules ?? null;
  const hashtag_seed = vprof?.hashtag_seed ?? null;

  console.log(
    `[vertical_profile] trace=${trace_id} vertical=${vertical_key} hit=1 ms=${Date.now() - start}`
  );

  const system = `
Eres un generador de "social outputs" para AI360Plus.
Debes devolver SOLO JSON válido y ajustarte al esquema.
Idioma: Español (si el vertical/audiencia dice lo contrario, respétalo).
Evita claims médicos/legales. No inventes datos del lead; si faltan, sé genérico.
`.trim();

  const user = `
VERTICAL_KEY: ${vertical_key}
CHANNEL: ${channel}

PROMPT_PREAMBLE:
${prompt_preamble}

TONE: ${tone}
AUDIENCE: ${audience}
STYLE_RULES_JSON: ${safeJson(style_rules)}
HASHTAG_SEED: ${safeJson(hashtag_seed)}

CONTEXT:
${context}

INSTRUCCIONES:
- Genera 1 output listo para publicar.
- title: corto (<= 80 chars)
- hook: 1 frase (<= 120 chars)
- caption: 2-5 líneas con valor + emoción (sin humo)
- hashtags: 6-12 hashtags relevantes (con #)
- cta: 1 llamada a la acción clara
- image_prompts: objeto con 2-4 prompts (keys: "square", "story", "banner"...), descriptivos para generación de imagen.
`.trim();

  const schema = {
    type: "object",
    additionalProperties: false,
    properties: {
      title: { type: "string" },
      hook: { type: "string" },
      caption: { type: "string" },
      hashtags: { type: "array", items: { type: "string" } },
      cta: { type: "string" },
      image_prompts: { type: "object", additionalProperties: true },
    },
    required: ["title", "hook", "caption", "hashtags", "cta", "image_prompts"],
  };

  const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

  const resp = await openai.responses.create({
    model: pickString(payload?.model, DEFAULT_MODEL),
    input: [
      { role: "system", content: system },
      { role: "user", content: user },
    ],
    // Responses API: Structured Outputs va en text.format (NO response_format)
    text: { format: { type: "json_schema", json_schema: schema } },
    temperature: 0.2,
    max_output_tokens: 900,
  });

  const raw = resp.output_text ?? "";
  let json: any;
  try {
    json = JSON.parse(raw);
  } catch {
    throw new Error(`LLM returned non-JSON. Raw: ${raw.slice(0, 500)}`);
  }

  const parsed = json as any;

  // Validación defensiva mínima
  const title = pickString(parsed?.title, "(no title)");
  const hook = pickString(parsed?.hook, "");
  const caption = pickString(parsed?.caption, "");
  const cta = pickString(parsed?.cta, "");
  const hashtags = normalizeHashtags(parsed?.hashtags);
  const image_prompts = parsed?.image_prompts ?? {};

  // Inserta social_outputs según tu esquema real
  const out = await insertOutput({
    org_id: activity.org_id,
    lead_id: activity.lead_id,
    vertical_key,
    channel,
    status: "draft",
    title,
    hook,
    caption,
    hashtags,
    cta,
    image_prompts,
    assets: {}, // assets se llenará luego si procede
    meta: {
      activity_id,
      trace_id,
      model: pickString(payload?.model, DEFAULT_MODEL),
    },
  });

  await updateJob(job.id, {
    status: "done",
    finished_at: new Date().toISOString(),
    last_error: null,
  });

  console.log(
    `[generate_assets] done trace=${trace_id} job_id=${job.id} output_id=${out.id} ms=${Date.now() - start}`
  );

  return { ok: true, output_id: out.id };
}

// Mini helper para trace
function cryptoRandomShort() {
  try {
    // @ts-ignore
    return crypto.randomUUID().slice(0, 8);
  } catch {
    return Math.random().toString(16).slice(2, 10);
  }
}
