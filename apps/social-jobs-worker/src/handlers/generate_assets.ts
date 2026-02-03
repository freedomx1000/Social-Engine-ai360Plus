// apps/social-jobs-worker/src/handlers/generate_assets.ts
import { randomUUID } from "crypto";
import { supabase } from "../supabase.js";

type GenerateAssetsResult = {
  title: string;
  hook: string;
  caption: string;
  hashtags: string[];
  cta: string;
  image_prompts: string[];
};

function asString(v: unknown, fallback = ""): string {
  return typeof v === "string" ? v : fallback;
}

function asStringArray(v: unknown): string[] {
  if (Array.isArray(v)) return v.filter((x) => typeof x === "string") as string[];
  if (typeof v === "string" && v.trim()) {
    // por si viene "a, b, c"
    return v
      .split(/[,|\n]/g)
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return [];
}

function safeJsonParse<T>(s: string): T | null {
  try {
    return JSON.parse(s) as T;
  } catch {
    return null;
  }
}

async function updateJobStart(jobId: string, trace_id: string) {
  // attempts++ y running
  // (si no existe la columna attempts/last_trace_id en tu DB, aplica el SQL de Observabilidad PRO)
  const { data: existing } = await supabase
    .from("social_jobs")
    .select("attempts")
    .eq("id", jobId)
    .maybeSingle();

  const attempts = (existing?.attempts ?? 0) + 1;

  const { error } = await supabase
    .from("social_jobs")
    .update({
      status: "running",
      attempts,
      last_trace_id: trace_id
    })
    .eq("id", jobId);

  if (error) throw error;
}

async function updateJobDone(jobId: string, trace_id: string) {
  const { error } = await supabase
    .from("social_jobs")
    .update({
      status: "done",
      last_error: null,
      last_error_at: null,
      last_trace_id: trace_id
    })
    .eq("id", jobId);

  if (error) throw error;
}

async function updateJobFailed(jobId: string, trace_id: string, err: unknown) {
  const msg = (err instanceof Error ? err.message : String(err)).slice(0, 900);

  const { error } = await supabase
    .from("social_jobs")
    .update({
      status: "failed",
      last_error: msg,
      last_error_at: new Date().toISOString(),
      last_trace_id: trace_id
    })
    .eq("id", jobId);

  if (error) throw error;
}

async function getVerticalProfile(vertical_key: string) {
  const key = vertical_key || "general";

  const { data, error } = await supabase
    .from("social_vertical_profiles")
    .select("*")
    .eq("vertical_key", key)
    .eq("is_active", true)
    .maybeSingle();

  if (error) throw error;

  if (!data && key !== "general") {
    const fallback = await supabase
      .from("social_vertical_profiles")
      .select("*")
      .eq("vertical_key", "general")
      .eq("is_active", true)
      .maybeSingle();

    if (fallback.error) throw fallback.error;
    return fallback.data;
  }

  return data;
}

function buildPrompt(params: {
  vertical_key: string;
  profile: any;
  activity: any;
  trace_id: string;
}): { system: string; user: string } {
  const { vertical_key, profile, activity, trace_id } = params;

  const system =
    asString(profile?.prompt_system) ||
    "Eres un copywriter senior de performance. Respondes SOLO en JSON válido con el schema solicitado. Sin markdown.";

  const userPrefix = asString(profile?.prompt_user_prefix);

  const styleRules = Array.isArray(profile?.image_style_rules)
    ? profile.image_style_rules
    : [];
  const styleText = styleRules
    .map((x: any) => asString(x?.rule))
    .filter(Boolean)
    .join("; ");

  const hashtagsSeed = Array.isArray(profile?.hashtag_seed)
    ? profile.hashtag_seed
    : [];
  const ctas = Array.isArray(profile?.cta_library) ? profile.cta_library : [];

  const tone = asString(profile?.tone, "claro, directo, humano");
  const audience = asString(profile?.audience, "general");
  const brandRules = profile?.brand_rules ?? {};

  const kind = asString(activity?.kind);
  const message = asString(activity?.message);
  const meta = activity?.meta ?? {};
  const payload = activity?.payload ?? {};

  // Contexto extra opcional si lo estás mandando en meta/payload
  const topic = asString(meta?.topic || payload?.topic);
  const offer = asString(meta?.offer || payload?.offer);
  const brief = asString(meta?.brief || payload?.brief);
  const lead_name = asString(meta?.lead_name || payload?.lead_name);

  const schemaHint = `
Devuelve EXACTAMENTE este JSON (sin texto extra):
{
  "title": "string",
  "hook": "string",
  "caption": "string",
  "hashtags": ["#tag1", "#tag2"],
  "cta": "string",
  "image_prompts": ["prompt1", "prompt2", "prompt3"]
}
`.trim();

  const user = `
${userPrefix}

TRACE_ID: ${trace_id}

VERTICAL: ${vertical_key}
TONO: ${tone}
AUDIENCIA: ${audience}
REGLAS_DE_MARCA_JSON: ${JSON.stringify(brandRules)}

ESTILO_IMAGEN: ${styleText || "minimal, luz estudio, sin texto"}
SEED_HASHTAGS: ${(hashtagsSeed || []).join(" ") || "(none)"}
CTA_SUGERIDAS: ${(ctas || []).join(" | ") || "(none)"}

ACTIVITY:
- kind: ${kind || "(none)"}
- message: ${message || "(none)"}
- lead_name: ${lead_name || "(unknown)"}
- topic: ${topic || "(none)"}
- offer: ${offer || "(none)"}
- brief: ${brief || "(none)"}

${schemaHint}
`.trim();

  return { system, user };
}

async function callOpenAI(params: {
  model: string;
  apiKey: string;
  system: string;
  user: string;
  traceId: string;
}): Promise<GenerateAssetsResult> {
  const { model, apiKey, system, user, traceId } = params;

  // Chat Completions con JSON Schema (si tu modelo lo soporta)
  // Si tu cuenta/modelo no soporta json_schema, igual funcionará muchas veces con "json_object".
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
        hashtags: {
          type: "array",
          items: { type: "string" }
        },
        cta: { type: "string" },
        image_prompts: {
          type: "array",
          items: { type: "string" }
        }
      },
      required: ["title", "hook", "caption", "hashtags", "cta", "image_prompts"]
    }
  };

  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      model,
      temperature: 0.7,
      messages: [
        { role: "system", content: system },
        { role: "user", content: user }
      ],
      response_format: {
        type: "json_schema",
        json_schema: schema
      },
      // trazabilidad
      metadata: { trace_id: traceId }
    })
  });

  const txt = await res.text();
  if (!res.ok) {
    throw new Error(`OpenAI ${res.status}: ${txt.slice(0, 300)}`);
  }

  const json = safeJsonParse<any>(txt);
  const content: string | undefined =
    json?.choices?.[0]?.message?.content ?? undefined;

  if (!content || typeof content !== "string") {
    throw new Error(`OpenAI: missing message.content. Raw: ${txt.slice(0, 300)}`);
  }

  const parsed = safeJsonParse<GenerateAssetsResult>(content);
  if (!parsed) {
    throw new Error(`OpenAI: invalid JSON content: ${content.slice(0, 300)}`);
  }

  // normaliza
  return {
    title: asString(parsed.title),
    hook: asString(parsed.hook),
    caption: asString(parsed.caption),
    hashtags: asStringArray(parsed.hashtags),
    cta: asString(parsed.cta),
    image_prompts: asStringArray(parsed.image_prompts)
  };
}

async function insertSocialOutput(params: {
  org_id: string;
  lead_id: string;
  vertical_key: string;
  channel: string;
  result: GenerateAssetsResult;
  model: string;
  trace_id: string;
  profile: any;
  activity_id: string;
}) {
  const { org_id, lead_id, vertical_key, channel, result, model, trace_id, profile, activity_id } = params;

  const image_prompts = Array.isArray(result.image_prompts)
    ? result.image_prompts
    : [];

  // assets vacío por contrato
  const assets: any[] = [];

  const meta = {
    model,
    trace_id,
    activity_id,
    vertical_key_used: vertical_key,
    profile_updated_at: profile?.updated_at ?? null
  };

  const row = {
    org_id,
    lead_id,
    status: "draft",
    channel,
    vertical_key,
    title: result.title,
    hook: result.hook,
    caption: result.caption,
    hashtags: result.hashtags,
    cta: result.cta,
    image_prompts, // jsonb array
    assets, // jsonb
    meta // jsonb
  };

  const { error } = await supabase.from("social_outputs").insert(row);
  if (error) throw error;
}

export async function generate_assets(job: any) {
  // job: { id, activity_id, payload?, ... }
  const trace_id = randomUUID();

  const model = process.env.OPENAI_MODEL || "gpt-4o-mini";
  const apiKey = process.env.OPENAI_API_KEY || "";

  const dryRun = String(process.env.SOCIAL_DRY_RUN || "").toLowerCase() === "true";

  try {
    if (!job?.id) throw new Error("Missing job.id");
    if (!job?.activity_id) throw new Error("Missing job.activity_id");

    // Observabilidad PRO (attempts++ + running)
    await updateJobStart(job.id, trace_id);

    // Carga activity (fuente de verdad)
    const { data: activity, error: actErr } = await supabase
      .from("crm_lead_activity")
      .select("id, org_id, lead_id, kind, message, meta, payload, created_at")
      .eq("id", job.activity_id)
      .maybeSingle();

    if (actErr) throw actErr;
    if (!activity) throw new Error(`crm_lead_activity not found: ${job.activity_id}`);

    const org_id = activity.org_id;
    const lead_id = activity.lead_id;

    // channel / vertical_key salen de meta/payload (con fallback)
    const meta = activity.meta ?? {};
    const payload = activity.payload ?? {};

    const channel =
      asString(meta?.channel) ||
      asString(payload?.channel) ||
      "multi";

    const vertical_key =
      asString(meta?.vertical_key) ||
      asString(payload?.vertical_key) ||
      "general";

    // Vertical Profiles
    const profile = await getVerticalProfile(vertical_key);

    const { system, user } = buildPrompt({
      vertical_key,
      profile,
      activity,
      trace_id
    });

    let result: GenerateAssetsResult;

    if (dryRun) {
      // placeholder determinista
      result = {
        title: `Draft ${vertical_key} (dry-run)`,
        hook: `Hook de prueba para ${vertical_key}`,
        caption: `Caption de prueba para ${vertical_key}.`,
        hashtags: ["#ai360plus", "#socialengine", "#draft", "#automation", "#crm"],
        cta: "¿Quieres que te lo deje listo para publicar?",
        image_prompts: [
          `High-quality realistic business scene related to ${vertical_key}, modern minimal style, neutral background, no text`,
          `Close-up of a laptop dashboard UI representing CRM and automation, realistic lighting, no text`,
          `Professional team meeting, modern office, optimistic mood, realistic photo, no text`
        ]
      };
    } else {
      if (!apiKey) throw new Error("Missing env OPENAI_API_KEY");

      result = await callOpenAI({
        model,
        apiKey,
        system,
        user,
        traceId: trace_id
      });
    }

    // Insert en public.social_outputs (status draft, channel multi, vertical_key desde meta/payload)
    await insertSocialOutput({
      org_id,
      lead_id,
      vertical_key,
      channel,
      result,
      model,
      trace_id,
      profile,
      activity_id: job.activity_id
    });

    // Job done
    await updateJobDone(job.id, trace_id);

    return { ok: true, trace_id };
  } catch (err) {
    // Job failed + last_error
    if (job?.id) {
      try {
        await updateJobFailed(job.id, trace_id, err);
      } catch {
        // si falla el update, no reventamos más
      }
    }
    throw err;
  }
}
