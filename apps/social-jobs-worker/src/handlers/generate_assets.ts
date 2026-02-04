/* apps/social-jobs-worker/src/handlers/generate_assets.ts */

import { supabase } from "../supabase.js";

/**
 * Expected LLM JSON shape
 */
type GenerateAssetsResult = {
  title: string;
  hook: string;
  caption: string;
  hashtags: string[];
  cta: string;
  image_prompts: string[];
};

type SocialVerticalProfile = {
  vertical_key: string;
  locale?: string | null;
  name?: string | null;
  tone?: string | null;
  audience?: string | null;
  positioning?: string | null;
  style_rules?: any; // jsonb
  prompt_preamble?: string | null;
  image_style?: string | null;
  hashtag_seed?: string[] | null;
  cta_seed?: string[] | null;
};

function nowIso() {
  return new Date().toISOString();
}

function newTraceId() {
  // simple trace id (no deps)
  return `tr_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
}

function safeJsonParse(text: string): any {
  try {
    return JSON.parse(text);
  } catch {
    // try to extract JSON object if model wraps it with text
    const start = text.indexOf("{");
    const end = text.lastIndexOf("}");
    if (start >= 0 && end > start) {
      const sliced = text.slice(start, end + 1);
      return JSON.parse(sliced);
    }
    throw new Error("LLM returned non-JSON output");
  }
}

function validateResult(obj: any): GenerateAssetsResult {
  const missing: string[] = [];
  const isStr = (v: any) => typeof v === "string" && v.trim().length > 0;
  const isStrArr = (v: any) => Array.isArray(v) && v.every((x) => typeof x === "string");

  if (!obj || typeof obj !== "object") throw new Error("LLM JSON is not an object");

  if (!isStr(obj.title)) missing.push("title");
  if (!isStr(obj.hook)) missing.push("hook");
  if (!isStr(obj.caption)) missing.push("caption");
  if (!isStrArr(obj.hashtags)) missing.push("hashtags");
  if (!isStr(obj.cta)) missing.push("cta");
  if (!isStrArr(obj.image_prompts)) missing.push("image_prompts");

  if (missing.length) {
    throw new Error(`LLM JSON missing/invalid fields: ${missing.join(", ")}`);
  }

  // normalize
  return {
    title: obj.title.trim(),
    hook: obj.hook.trim(),
    caption: obj.caption.trim(),
    hashtags: obj.hashtags.map((s: string) => s.trim()).filter(Boolean),
    cta: obj.cta.trim(),
    image_prompts: obj.image_prompts.map((s: string) => s.trim()).filter(Boolean),
  };
}

/**
 * Vertical Profiles loader
 * - table suggestion: public.social_vertical_profiles
 * - columns: vertical_key (pk/unique), locale, tone, audience, positioning, style_rules(jsonb), prompt_preamble, image_style, hashtag_seed(text[]), cta_seed(text[])
 */
async function loadVerticalProfile(vertical_key: string, org_id?: string | null): Promise<SocialVerticalProfile | null> {
  // If your table is org-scoped, add org_id filter here.
  // This version tries (org_id match) then fallback (global).
  const base = supabase
    .from("social_vertical_profiles")
    .select(
      "vertical_key, locale, name, tone, audience, positioning, style_rules, prompt_preamble, image_style, hashtag_seed, cta_seed"
    )
    .eq("vertical_key", vertical_key)
    .limit(1);

  // Try org-specific if column exists in your schema; if not, this will just ignore
  // (Supabase will error if column doesn't exist; if that's your case, remove org_id parts.)
  if (org_id) {
    const { data, error } = await base.eq("org_id", org_id as any);
    if (!error && data && data.length) return data[0] as any;
  }

  const { data, error } = await supabase
    .from("social_vertical_profiles")
    .select(
      "vertical_key, locale, name, tone, audience, positioning, style_rules, prompt_preamble, image_style, hashtag_seed, cta_seed"
    )
    .eq("vertical_key", vertical_key)
    .limit(1);

  if (error) return null;
  if (!data || !data.length) return null;
  return data[0] as any;
}

function buildPrompt(args: {
  vertical_key: string;
  channel: string;
  lead_name?: string;
  topic?: string;
  offer?: string;
  brief?: string;
  locale?: string;
  profile?: SocialVerticalProfile | null;
}) {
  const {
    vertical_key,
    channel,
    lead_name = "unknown",
    topic = "none",
    offer = "none",
    brief = "none",
    locale = "es",
    profile,
  } = args;

  const tone = profile?.tone || "claro, directo, humano, con energía";
  const audience = profile?.audience || "público general";
  const positioning = profile?.positioning || "valor real, honestidad, claridad";
  const preamble = profile?.prompt_preamble || "";
  const imageStyle = profile?.image_style || "fotografía realista, moderna, minimal, sin texto";
  const hashtagSeed = (profile?.hashtag_seed || []).slice(0, 8);
  const ctaSeed = (profile?.cta_seed || []).slice(0, 6);

  // Observación: damos instrucciones explícitas para evitar relleno y asegurar JSON estricto.
  return `
${preamble}

You are an expert social content generator.
Return ONLY valid JSON matching this exact schema:

{
  "title": "string",
  "hook": "string",
  "caption": "string",
  "hashtags": ["string", ...],
  "cta": "string",
  "image_prompts": ["string", ...]
}

Hard rules:
- Output must be JSON only (no markdown, no extra keys).
- Language/locale: ${locale}.
- Tone: ${tone}.
- Audience: ${audience}.
- Positioning: ${positioning}.
- Keep it suitable for ${channel} and for the vertical "${vertical_key}".
- caption: 2–6 short lines, readable, no walls of text.
- hashtags: 6–14 items, no spaces, lower/normal case accepted, include relevant branded + topical tags.
- cta: 1 short line.
- image_prompts: 3–6 prompts, each describing a single image, with style "${imageStyle}", avoid text in the image.

Context:
- lead_name: ${lead_name}
- topic: ${topic}
- offer: ${offer}
- brief: ${brief}

Optional seeds (use if relevant, don't force):
- hashtag_seed: ${JSON.stringify(hashtagSeed)}
- cta_seed: ${JSON.stringify(ctaSeed)}
`.trim();
}

/**
 * OpenAI call (no SDK required).
 * Uses process.env.OPENAI_API_KEY
 */
async function callOpenAIJson(args: { prompt: string; model: string; trace_id: string }) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) throw new Error("Missing OPENAI_API_KEY");

  const t0 = Date.now();

  // Using Chat Completions with JSON-only instruction.
  // If you later want strict json_schema, we can move to Responses API,
  // but this is robust and simple for Render workers.
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      "X-Trace-Id": args.trace_id,
    },
    body: JSON.stringify({
      model: args.model,
      temperature: 0.7,
      messages: [
        {
          role: "system",
          content: "You output only valid JSON. No markdown. No additional text.",
        },
        { role: "user", content: args.prompt },
      ],
      response_format: { type: "json_object" },
    }),
  });

  const latency_ms = Date.now() - t0;

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`OpenAI HTTP ${res.status}: ${text.slice(0, 400)}`);
  }

  const json = await res.json();
  const content: string | undefined = json?.choices?.[0]?.message?.content;

  if (!content) throw new Error("OpenAI returned empty content");

  return {
    raw: content,
    usage: json?.usage || null,
    latency_ms,
  };
}

/**
 * Main handler
 */
export async function generate_assets(job: any) {
  const trace_id = newTraceId();
  const started_at = Date.now();

  // ---- Extract payload/meta
  const payload = job?.payload || {};
  const meta = job?.meta || {};

  const org_id: string | null = payload.org_id ?? job?.org_id ?? null;
  const lead_id: string | null = payload.lead_id ?? job?.lead_id ?? null;

  const channel: string = payload.channel || meta.channel || "multi";
  const vertical_key: string = payload.vertical_key || meta.vertical_key || "general";

  // These fields feed the prompt
  const lead_name: string | undefined = payload.lead_name || meta.lead_name || undefined;
  const topic: string | undefined = payload.topic || meta.topic || undefined;
  const offer: string | undefined = payload.offer || meta.offer || undefined;
  const brief: string | undefined = payload.brief || meta.brief || undefined;

  const locale: string = payload.locale || meta.locale || "es";

  // Critical: activity_id for idempotency & trace
  const activity_id: string | null = payload.activity_id ?? job?.activity_id ?? meta.activity_id ?? null;

  // Observability base
  const obs: any = {
    trace_id,
    started_at: nowIso(),
    activity_id,
    job_id: job?.id ?? null,
    job_type: job?.job_type ?? "generate_assets",
    org_id,
    lead_id,
    channel,
    vertical_key,
    model: process.env.OPENAI_MODEL || "gpt-4.1-mini",
  };

  // ---- Load vertical profile (GO Vertical Profiles)
  let profile: SocialVerticalProfile | null = null;
  try {
    profile = await loadVerticalProfile(vertical_key, org_id);
    obs.vertical_profile_found = !!profile;
  } catch (e: any) {
    obs.vertical_profile_found = false;
    obs.vertical_profile_error = String(e?.message || e);
  }

  // ---- Build prompt
  const prompt = buildPrompt({
    vertical_key,
    channel,
    lead_name,
    topic,
    offer,
    brief,
    locale: profile?.locale || locale,
    profile,
  });

  // ---- Call OpenAI / generate JSON
  let result: GenerateAssetsResult;
  let llm_usage: any = null;
  let llm_latency_ms: number | null = null;

  try {
    const { raw, usage, latency_ms } = await callOpenAIJson({
      prompt,
      model: obs.model,
      trace_id,
    });
    llm_usage = usage;
    llm_latency_ms = latency_ms;

    const parsed = safeJsonParse(raw);
    result = validateResult(parsed);
  } catch (e: any) {
    obs.status = "failed";
    obs.error = String(e?.message || e);
    obs.finished_at = nowIso();
    obs.total_ms = Date.now() - started_at;

    // Best effort: record failure in social_outputs (optional). If you prefer, remove this insert.
    try {
      await supabase.from("social_outputs").insert({
        org_id,
        lead_id,
        status: "failed",
        channel,
        vertical_key,
        title: "Generation failed",
        hook: "",
        caption: "",
        hashtags: [],
        cta: "",
        image_prompts: [],
        assets: [],
        meta: {
          ...obs,
          llm_usage,
          llm_latency_ms,
        },
      });
    } catch {
      // swallow
    }

    throw e;
  }

  // ---- Insert/Upsert into social_outputs (GO Observabilidad PRO + idempotency-friendly)
  const insertRow: any = {
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
    image_prompts: result.image_prompts, // jsonb array column expected
    assets: [], // jsonb array
    meta: {
      ...obs,
      status: "ok",
      finished_at: nowIso(),
      total_ms: Date.now() - started_at,
      llm_usage,
      llm_latency_ms,
      activity_id, // keep it duplicated inside meta for expression index / constraints
    },
  };

  // If your table supports a dedicated activity_id column, add it here:
  // insertRow.activity_id = activity_id;

  // If you already created a UNIQUE INDEX on (meta->>'activity_id', channel, vertical_key),
  // you cannot "onConflict" it by name in Supabase easily.
  // Best approach is to add a stored generated column activity_id and constrain (activity_id, channel, vertical_key).
  // Meanwhile: do a read-before-write fallback to prevent duplicates.
  try {
    if (activity_id) {
      // Read-before-write guard (works even before DB constraint is perfect)
      const { data: existing, error: exErr } = await supabase
        .from("social_outputs")
        .select("id")
        .eq("channel", channel)
        .eq("vertical_key", vertical_key)
        .eq("meta->>activity_id", activity_id)
        .limit(1);

      if (!exErr && existing && existing.length) {
        const existingId = existing[0].id;
        await supabase.from("social_outputs").update(insertRow).eq("id", existingId);
        return { ok: true, updated: true, id: existingId, trace_id };
      }
    }

    const { data, error } = await supabase.from("social_outputs").insert(insertRow).select("id").single();
    if (error) throw error;

    return { ok: true, inserted: true, id: data?.id, trace_id };
  } catch (e: any) {
    // If a DB unique constraint exists, concurrent retries may throw duplicate-key: then just re-select and return
    if (activity_id) {
      const { data: again } = await supabase
        .from("social_outputs")
        .select("id")
        .eq("channel", channel)
        .eq("vertical_key", vertical_key)
        .eq("meta->>activity_id", activity_id)
        .limit(1);

      if (again && again.length) {
        return { ok: true, deduped: true, id: again[0].id, trace_id };
      }
    }
    throw e;
  }
}

