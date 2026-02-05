// apps/social-jobs-worker/src/handlers/generate_assets.ts
// Generate Assets worker — Responses API (text.format json_schema) + DB write social_outputs
// ------------------------------------------------------------------

import type { SupabaseClient } from "@supabase/supabase-js";

type Json = Record<string, any>;

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini";
const OPENAI_BASE = process.env.OPENAI_BASE_URL || "https://api.openai.com/v1";

if (!OPENAI_API_KEY) {
  throw new Error("Missing OPENAI_API_KEY");
}

function nowMs() {
  return Date.now();
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

function pickArray(v: any): string[] {
  if (Array.isArray(v)) return v.filter((x) => typeof x === "string");
  return [];
}

/**
 * OpenAI Responses API -> expects JSON via text.format.json_schema
 */
async function openaiResponsesJSON(args: {
  system: string;
  user: string;
  schema: any;
  model: string;
  temperature?: number;
}): Promise<any> {
  const resp = await fetch(`${OPENAI_BASE}/responses`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: args.model,
      temperature: args.temperature ?? 0.2,

      // ✅ FIX: response_format moved to text.format in Responses API
      text: { format: { type: "json_schema", json_schema: args.schema } },

      input: [
        { role: "system", content: args.system },
        { role: "user", content: args.user },
      ],
    }),
  });

  const data = await resp.json().catch(() => null);

  if (!resp.ok) {
    const err = data?.error ? safeJson(data.error) : safeJson(data);
    throw new Error(`OpenAI error ${resp.status}: ${err}`);
  }

  // Prefer output_text
  const textOut =
    data?.output_text ??
    data?.output?.[0]?.content?.find?.((c: any) => c?.type === "output_text")?.text ??
    data?.output?.[0]?.content?.[0]?.text;

  if (!textOut) {
    throw new Error(`OpenAI returned no text. payload=${safeJson(data)}`);
  }

  try {
    return JSON.parse(textOut);
  } catch {
    throw new Error(`OpenAI returned non-JSON output. text=${String(textOut).slice(0, 500)}`);
  }
}

/**
 * Schema: lo que insertaremos en social_outputs
 */
function buildOutputSchema() {
  return {
    name: "social_output",
    schema: {
      type: "object",
      additionalProperties: false,
      properties: {
        title: { type: "string" },
        hook: { type: "string" },
        caption: { type: "string" },
        cta: { type: "string" },
        hashtags: { type: "array", items: { type: "string" } },
        image_prompts: {
          type: "object",
          additionalProperties: true,
        },
      },
      required: ["title", "hook", "caption", "cta", "hashtags", "image_prompts"],
    },
  };
}

async function fetchVerticalProfile(supabase: SupabaseClient, vertical_key: string) {
  const t0 = nowMs();
  const { data, error } = await supabase
    .from("social_vertical_profiles")
    .select("vertical_key, prompt_preamble, locale, tone, audience, positioning, style_rules, hashtag_seed")
    .eq("vertical_key", vertical_key)
    .maybeSingle();

  const ms = nowMs() - t0;
  return { data, error, ms };
}

async function fetchActivity(supabase: SupabaseClient, activity_id: string) {
  const t0 = nowMs();
  const { data, error } = await supabase
    .from("crm_lead_activity")
    .select("id, org_id, lead_id, kind, message, meta, payload, created_at")
    .eq("id", activity_id)
    .maybeSingle();
  const ms = nowMs() - t0;
  return { data, error, ms };
}

function buildPrompt(args: {
  systemPreamble: string;
  channel: string;
  vertical_key: string;
  lead_name: string;
  topic: string;
  offer: string;
  brief: string;
  payload: any;
}) {
  const context = `
lead_name: ${args.lead_name}
topic: ${args.topic}
offer: ${args.offer}
brief: ${args.brief}
channel: ${args.channel}
vertical_key: ${args.vertical_key}
payload_json: ${safeJson(args.payload)}
`.trim();

  const user = `
Genera un output listo para publicar.
- Idioma y tono coherente con el preámbulo del vertical.
- Título: corto y humano.
- Hook: 1 frase fuerte.
- Caption: 2-5 frases, claridad + emoción + utilidad.
- CTA: 1 frase.
- Hashtags: 6-10 (sin acentos raros, sin spam).
- image_prompts: json con 2-4 prompts de imagen (pueden ser claves: hero, alt1, alt2, etc).

Contexto:
${context}
`.trim();

  return {
    system: args.systemPreamble,
    user,
  };
}

async function insertSocialOutput(
  supabase: SupabaseClient,
  args: {
    org_id: string | null;
    lead_id: string | null;
    activity_id: string;
    channel: string;
    vertical_key: string;
    status: string;
    out: any;
    trace_id: string;
    job_id: string;
  }
) {
  const t0 = nowMs();

  const title = pickString(args.out?.title, "");
  const hook = pickString(args.out?.hook, "");
  const caption = pickString(args.out?.caption, "");
  const cta = pickString(args.out?.cta, "");
  const hashtags = pickArray(args.out?.hashtags);
  const image_prompts = args.out?.image_prompts ?? {};

  const meta = {
    activity_id: args.activity_id,
    trace_id: args.trace_id,
    job_id: args.job_id,
    model: OPENAI_MODEL,
  };

  const { data, error } = await supabase
    .from("social_outputs")
    .insert({
      org_id: args.org_id,
      lead_id: args.lead_id,
      activity_id: args.activity_id,
      channel: args.channel,
      vertical_key: args.vertical_key,
      status: args.status,
      title,
      hook,
      caption,
      cta,
      hashtags,
      image_prompts,
      assets: {},
      meta,
    })
    .select("id")
    .single();

  const ms = nowMs() - t0;
  return { data, error, ms };
}

/**
 * MAIN: procesa 1 job (tu runner externo lo llama con job/activity ya resuelto)
 */
export async function handleGenerateAssetsJob(args: {
  supabase: SupabaseClient;
  job: {
    id: string;
    activity_id: string;
    payload?: Json | null;
    meta?: Json | null;
  };
}) {
  const started = nowMs();

  const supabase = args.supabase;
  const job_id = args.job.id;
  const activity_id = args.job.activity_id;

  const payload = args.job.payload ?? {};
  const meta = (args.job.meta ?? payload?.meta ?? {}) as Json;

  // channel & vertical_key (prioridad: meta -> payload -> default)
  const channel = pickString(meta?.channel, payload?.channel, "multi");
  const vertical_key = pickString(meta?.vertical_key, payload?.vertical_key, "general");

  // modelo (si lo pasas por payload/meta)
  const model = pickString(meta?.model, payload?.model, OPENAI_MODEL);

  // trace
  const trace_id =
    (globalThis.crypto?.randomUUID?.() as string | undefined) ||
    `trace_${Math.random().toString(16).slice(2)}_${Date.now()}`;

  console.log(
    `[generate_assets] start trace=${trace_id} activity_id=${activity_id} vertical=${vertical_key} channel=${channel}`
  );

  // 1) Vertical profile
  const vpT0 = nowMs();
  const vp = await fetchVerticalProfile(supabase, vertical_key);
  const vpLatency = nowMs() - vpT0;

  if (vp.error) {
    throw new Error(`Vertical profile error: ${vp.error.message}`);
  }

  const promptPreamble = pickString(
    vp.data?.prompt_preamble,
    `Eres un copywriter experto en social media. Sé claro, humano y útil.`
  );

  console.log(
    `[vertical_profile] trace=${trace_id} vertical=${vertical_key} hit=${vp.data ? 1 : 0} ms=${vp.ms}`
  );

  // 2) Activity
  const act = await fetchActivity(supabase, activity_id);
  if (act.error) {
    throw new Error(`Activity read error: ${act.error.message}`);
  }
  if (!act.data) {
    throw new Error(`Activity not found: ${activity_id}`);
  }

  // contexto base (si tu job ya trae cosas extra, se vuelcan)
  const lead_name = pickString(payload?.lead_name, payload?.leadName, meta?.lead_name, "unknown");
  const topic = pickString(payload?.topic, meta?.topic, "no");
  const offer = pickString(payload?.offer, meta?.offer, "no");
  const brief = pickString(payload?.brief, meta?.brief, "");

  const { system, user } = buildPrompt({
    systemPreamble: promptPreamble,
    channel,
    vertical_key,
    lead_name,
    topic,
    offer,
    brief,
    payload,
  });

  // 3) LLM
  const llmT0 = nowMs();
  const schema = buildOutputSchema();
  const out = await openaiResponsesJSON({
    system,
    user,
    schema,
    model,
    temperature: 0.2,
  });
  const llmLatency = nowMs() - llmT0;

  // 4) Insert social_outputs
  const dbT0 = nowMs();
  const inserted = await insertSocialOutput(supabase, {
    org_id: act.data.org_id ?? null,
    lead_id: act.data.lead_id ?? null,
    activity_id,
    channel,
    vertical_key,
    status: "draft",
    out,
    trace_id,
    job_id,
  });
  const dbLatency = nowMs() - dbT0;

  if (inserted.error) {
    throw new Error(`Insert social_outputs error: ${inserted.error.message}`);
  }

  const total = nowMs() - started;

  console.log(
    `[generate_assets] done trace=${trace_id} activity_id=${activity_id} output_id=${inserted.data?.id} model=${model} ` +
      `vp_ms=${vpLatency} llm_ms=${llmLatency} db_ms=${dbLatency} total_ms=${total}`
  );

  return {
    ok: true,
    output_id: inserted.data?.id,
    trace_id,
  };
}
