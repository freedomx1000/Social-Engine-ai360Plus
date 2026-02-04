/* apps/social-jobs-worker/src/handlers/generate_assets.ts */

import crypto from "node:crypto";
import { supabase } from "../supabase.js";

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// Cambia si quieres otro modelo
const DEFAULT_MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini";

/**
 * Expected LLM JSON shape (strict)
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
  name: string | null;
  locale: string | null;
  tone: string | null;
  audience: string | null;
  positioning: string | null;
  prompt_preamble: string | null;
  image_style: string | null;
  hashtag_seed: string[] | null;
  style_rules: any | null; // jsonb
  is_active: boolean;
};

function nowMs() {
  return Date.now();
}

function safeJson(v: any) {
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}

function pickString(v: any, fallback: string) {
  return typeof v === "string" && v.trim() ? v.trim() : fallback;
}

function pickArrayOfStrings(v: any): string[] {
  if (!Array.isArray(v)) return [];
  return v
    .filter((x) => typeof x === "string")
    .map((x) => x.trim())
    .filter(Boolean);
}

/**
 * OpenAI Responses API call forcing strict JSON schema output.
 */
async function callOpenAIJsonStrict(args: {
  model: string;
  traceId: string;
  prompt: string;
}): Promise<GenerateAssetsResult> {
  if (!OPENAI_API_KEY) throw new Error("Missing env OPENAI_API_KEY");

  const t0 = nowMs();

  const schema = {
    name: "social_assets_schema",
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
        image_prompts: { type: "array", items: { type: "string" } }
      },
      required: ["title", "hook", "caption", "hashtags", "cta", "image_prompts"]
    }
  };

  const res = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      model: args.model,
      temperature: 0.8,
      text: { format: { type: "json_schema", json_schema: schema } },
      input: [
        {
          role: "system",
          content:
            "You are an expert social media copywriter. Output MUST strictly match the JSON schema. No extra keys, no markdown."
        },
        {
          role: "user",
          content: args.prompt
        }
      ]
    })
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`OpenAI error ${res.status}: ${txt.slice(0, 500)}`);
  }

  const json = (await res.json()) as any;

  let rawText = "";

  if (typeof json.output_text === "string" && json.output_text.trim()) {
    rawText = json.output_text.trim();
  } else if (Array.isArray(json.output)) {
    for (const o of json.output) {
      const content = o?.content;
      if (Array.isArray(content)) {
        for (const c of content) {
          if (c?.type === "output_text" && typeof c?.text === "string") {
            rawText = c.text.trim();
            break;
          }
        }
      }
      if (rawText) break;
    }
  }

  if (!rawText) {
    if (json?.output?.[0]?.content?.[0]?.json) {
      return json.output[0].content[0].json as GenerateAssetsResult;
    }
    throw new Error(`OpenAI: could not locate output_text. trace=${args.traceId}`);
  }

  const parsed = JSON.parse(rawText) as GenerateAssetsResult;

  if (
    !parsed ||
    typeof parsed.title !== "string" ||
    typeof parsed.hook !== "string" ||
    typeof parsed.caption !== "string" ||
    !Array.isArray(parsed.hashtags) ||
    typeof parsed.cta !== "string" ||
    !Array.isArray(parsed.image_prompts)
  ) {
    throw new Error(
      `OpenAI returned invalid shape. trace=${args.traceId} raw=${rawText.slice(0, 300)}`
    );
  }

  const dt = nowMs() - t0;
  console.log(`[openai] trace=${args.traceId} model=${args.model} ms=${dt}`);

  return {
    title: pickString(parsed.title, "Untitled"),
    hook: pickString(parsed.hook, ""),
    caption: pickString(parsed.caption, ""),
    hashtags: pickArrayOfStrings(parsed.hashtags).slice(0, 18),
    cta: pickString(parsed.cta, ""),
    image_prompts: pickArrayOfStrings(parsed.image_prompts).slice(0, 6)
  };
}

/**
 * Lee Vertical Profile (si existe). Si no existe, usa "general" si está.
 */
async function fetchVerticalProfile(
  vertical_key: string,
  traceId: string
): Promise<SocialVerticalProfile | null> {
  const t0 = nowMs();

  const { data: p1, error: e1 } = await supabase
    .from("social_vertical_profiles")
    .select(
      "vertical_key,name,locale,tone,audience,positioning,prompt_preamble,image_style,hashtag_seed,style_rules,is_active"
    )
    .eq("vertical_key", vertical_key)
    .eq("is_active", true)
    .maybeSingle();

  if (e1) {
    console.warn(
      `[vertical_profile] trace=${traceId} fetch error vertical=${vertical_key} err=${e1.message}`
    );
  }

  if (p1) {
    console.log(
      `[vertical_profile] trace=${traceId} vertical=${vertical_key} hit=1 ms=${nowMs() - t0}`
    );
    return p1 as SocialVerticalProfile;
  }

  const { data: p2, error: e2 } = await supabase
    .from("social_vertical_profiles")
    .select(
      "vertical_key,name,locale,tone,audience,positioning,prompt_preamble,image_style,hashtag_seed,style_rules,is_active"
    )
    .eq("vertical_key", "general")
    .eq("is_active", true)
    .maybeSingle();

  if (e2) {
    console.warn(`[vertical_profile] trace=${traceId} fallback general err=${e2.message}`);
  }

  console.log(
    `[vertical_profile] trace=${traceId} vertical=${vertical_key} hit=${p2 ? 1 : 0} ms=${nowMs() - t0}`
  );
  return (p2 as SocialVerticalProfile) || null;
}

/**
 * Construye prompt final usando context + vertical profile.
 */
function buildPrompt(args: {
  traceId: string;
  vertical_key: string;
  context: string;
  profile: SocialVerticalProfile | null;
}): string {
  const p = args.profile;

  const profileBlock = p
    ? `
VERTICAL PROFILE:
- vertical_key: ${p.vertical_key}
- name: ${p.name ?? ""}
- locale: ${p.locale ?? ""}
- tone: ${p.tone ?? ""}
- audience: ${p.audience ?? ""}
- positioning: ${p.positioning ?? ""}
- prompt_preamble: ${p.prompt_preamble ?? ""}
- image_style: ${p.image_style ?? ""}
- hashtag_seed: ${(p.hashtag_seed ?? []).join(", ")}
- style_rules_json: ${p.style_rules ? safeJson(p.style_rules) : ""}
`
    : `
VERTICAL PROFILE:
- none (use default best-practice social style)
`;

  return `
TASK:
Create social media assets in Spanish, optimized for conversion.

OUTPUT RULES:
- Must output STRICT JSON matching the schema.
- No markdown, no extra keys.
- Keep it punchy, modern, human, no hype-buzzword salad.
- Hashtags: 8-16 max.
- image_prompts: 3-6 prompts. Each MUST be photorealistic, studio lighting, modern, "no text", and consistent with the brand.

CONTEXT:
trace_id: ${args.traceId}
vertical_key: ${args.vertical_key}

${profileBlock}

USER INPUT / JOB CONTEXT:
${args.context}

GUIDANCE:
- title: short, scroll-stopping (<= 60 chars)
- hook: 1 sentence, curiosity + benefit
- caption: 3-6 lines, clarity > poetry, include 1-2 emojis max
- cta: clear single action (demo, audit, checklist, reply, etc.)
- hashtags: relevant, not spammy
- image_prompts: match vertical + brand, neutral background, studio light, no text, no logos
`.trim();
}

/**
 * Handler principal
 */
export async function generate_assets(job: any) {
  const started = nowMs();

  const activity_id =
    job?.activity_id || job?.activityId || job?.payload?.activity_id || job?.payload?.activityId;
  const trace_id =
    job?.trace_id ||
    job?.traceId ||
    job?.payload?.trace_id ||
    job?.payload?.traceId ||
    crypto.randomUUID();

  const payload = job?.payload ?? {};
  const meta = job?.meta ?? payload?.meta ?? {};

  const channel = pickString(meta?.channel ?? payload?.channel, "multi");
  const vertical_key = pickString(meta?.vertical_key ?? payload?.vertical_key, "general");

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

  const model = pickString(payload?.model ?? meta?.model, DEFAULT_MODEL);

  const vp0 = nowMs();
  const profile = await fetchVerticalProfile(vertical_key, trace_id);
  const vpLatency = nowMs() - vp0;

  const llm0 = nowMs();
  const prompt = buildPrompt({ traceId: trace_id, vertical_key, context, profile });
  const result = await callOpenAIJsonStrict({ model, traceId: trace_id, prompt });
  const llmLatency = nowMs() - llm0;

  const db0 = nowMs();

  const insertRow: any = {
    org_id: payload?.org_id ?? payload?.orgId ?? meta?.org_id ?? meta?.orgId ?? null,
    lead_id: payload?.lead_id ?? payload?.leadId ?? meta?.lead_id ?? meta?.leadId ?? null,

    status: "draft",
    channel,
    vertical_key,

    // ✅ FIX: columna real para idempotencia
    activity_id: activity_id ?? null,

    title: result.title,
    hook: result.hook,
    caption: result.caption,
    hashtags: result.hashtags,
    cta: result.cta,
    image_prompts: result.image_prompts,

    assets: [],

    meta: {
      ...(typeof meta === "object" && meta ? meta : {}),
      activity_id: activity_id ?? null,
      trace_id,
      model
    }
  };

  const { data: out, error: outErr } = await supabase
    .from("social_outputs")
    .upsert(insertRow, {
      onConflict: "activity_id,channel,vertical_key"
    })
    .select("id")
    .maybeSingle();

  const dbLatency = nowMs() - db0;

  if (outErr) {
    console.error(
      `[generate_assets] upsert social_outputs failed trace=${trace_id} activity_id=${activity_id} db_ms=${dbLatency} err=${outErr.message}`
    );
    throw outErr;
  }

  const total = nowMs() - started;

  console.log(
    `[generate_assets] done trace=${trace_id} activity_id=${activity_id} output_id=${out?.id} model=${model} ` +
      `vp_ms=${vpLatency} llm_ms=${llmLatency} db_ms=${dbLatency} total_ms=${total}`
  );

  return {
    ok: true,
    output_id: out?.id,
    trace_id
  };
}
