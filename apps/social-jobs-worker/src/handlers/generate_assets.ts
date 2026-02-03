// apps/social-jobs-worker/src/handlers/generate_assets.ts

import { randomUUID } from "crypto";

// AJUSTA ESTE IMPORT SI TU EXPORT SE LLAMA DISTINTO
// - si en tu src/supabase.ts exportas `supabase`, déjalo tal cual
// - si exportas `supabase`, cambia el nombre aquí.
import { supabase } from "../supabase.js";

type GenerateAssetsResult = {
  title: string;
  hook: string;
  caption: string;
  hashtags: string[];
  cta: string;
  image_prompts: string[];
};

function safeString(v: any, fallback = ""): string {
  if (typeof v === "string") return v;
  if (v == null) return fallback;
  return String(v);
}

function safeArrayOfStrings(v: any): string[] {
  if (!Array.isArray(v)) return [];
  return v
    .map((x) => (typeof x === "string" ? x : x == null ? "" : String(x)))
    .map((s) => s.trim())
    .filter(Boolean);
}

function normalizeHashtags(tags: any): string[] {
  const arr = safeArrayOfStrings(tags);
  // Normaliza: "ai360" => "#ai360"
  return arr.map((t) => (t.startsWith("#") ? t : `#${t}`));
}

function parseJsonStrict(text: string): any {
  // Limpia posibles fences ```json ... ```
  const cleaned = text
    .trim()
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/```$/i, "")
    .trim();

  return JSON.parse(cleaned);
}

function validateResult(obj: any): GenerateAssetsResult {
  if (!obj || typeof obj !== "object") throw new Error("OpenAI returned non-object JSON");

  const title = safeString(obj.title).trim();
  const hook = safeString(obj.hook).trim();
  const caption = safeString(obj.caption).trim();
  const cta = safeString(obj.cta).trim();
  const hashtags = normalizeHashtags(obj.hashtags);
  const image_prompts = safeArrayOfStrings(obj.image_prompts);

  if (!title) throw new Error("Missing 'title' in OpenAI JSON");
  if (!hook) throw new Error("Missing 'hook' in OpenAI JSON");
  if (!caption) throw new Error("Missing 'caption' in OpenAI JSON");
  if (!cta) throw new Error("Missing 'cta' in OpenAI JSON");
  if (hashtags.length === 0) throw new Error("Missing/empty 'hashtags' in OpenAI JSON");
  if (image_prompts.length === 0) throw new Error("Missing/empty 'image_prompts' in OpenAI JSON");

  return { title, hook, caption, hashtags, cta, image_prompts };
}

/**
 * Nueva función: Lee el perfil del vertical desde social_vertical_profiles
 * Si no existe el vertical_key solicitado, usa el fallback "general"
 */
async function getVerticalProfile(vertical_key: string) {
  const key = vertical_key || "general";

  const { data, error } = await supabase
    .from("social_vertical_profiles")
    .select("*")
    .eq("vertical_key", key)
    .eq("is_active", true)
    .maybeSingle();

  if (error) throw error;

  // Si no existe el profile y no es "general", busca el fallback
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

async function callOpenAI(params: {
  model: string;
  apiKey: string;
  systemPrompt: string;
  userPrompt: string;
  traceId: string;
}): Promise<GenerateAssetsResult> {
  const { model, apiKey, systemPrompt, userPrompt, traceId } = params;

  // API Chat Completions (compatible y simple)
  const resp = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      "X-Request-Id": traceId,
    },
    body: JSON.stringify({
      model,
      temperature: 0.7,
      messages: [
        {
          role: "system",
          content: systemPrompt,
        },
        { role: "user", content: userPrompt },
      ],
      response_format: { type: "json_object" }, // fuerza JSON en modelos compatibles
    }),
  });

  const raw = await resp.text();
  if (!resp.ok) {
    throw new Error(`OpenAI error ${resp.status}: ${raw}`);
  }

  let data: any;
  try {
    data = JSON.parse(raw);
  } catch {
    throw new Error(`OpenAI returned non-JSON response: ${raw.slice(0, 500)}`);
  }

  const content = data?.choices?.[0]?.message?.content;
  if (!content || typeof content !== "string") {
    throw new Error(`OpenAI response missing message.content: ${raw.slice(0, 500)}`);
  }

  const parsed = parseJsonStrict(content);
  return validateResult(parsed);
}

/**
 * Handler principal
 * Espera job con shape típico:
 * job.id, job.org_id, job.lead_id, job.activity_id, job.payload(jsonb)
 */
export async function generate_assets(job: any) {
  const trace_id = randomUUID();
  const model = process.env.OPENAI_MODEL || "gpt-4.1-mini";
  const apiKey = process.env.OPENAI_API_KEY || "";

  const payload = job?.payload ?? {};
  const meta = payload?.meta ?? payload?.metadata ?? {};

  const org_id = job?.org_id ?? payload?.org_id ?? meta?.org_id;
  const lead_id = job?.lead_id ?? payload?.lead_id ?? meta?.lead_id;

  // vertical_key: lo pediste desde payload/meta
  const vertical_key =
    safeString(payload?.vertical_key, "") ||
    safeString(meta?.vertical_key, "") ||
    "general";

  // channel: guardamos draft siempre multi (por tu spec)
  const channel = "multi";

  // datos opcionales para mejorar el copy (si existen)
  const lead_name = safeString(payload?.lead_name ?? meta?.lead_name, "");
  const locale = safeString(payload?.locale ?? meta?.locale, "es");
  const brief = safeString(payload?.brief ?? meta?.brief, "");
  const topic = safeString(payload?.topic ?? meta?.topic, "");
  const offer = safeString(payload?.offer ?? meta?.offer, "");

  // "IA controlada" (dry run)
  const dryRun = (process.env.AI_DRY_RUN || "").toLowerCase() === "1";

  // ✅ NUEVO: Leer el profile del vertical
  const profile = await getVerticalProfile(vertical_key);

  // Construir system prompt desde el profile
  const systemPrompt = profile?.prompt_system || "Responde SOLO JSON válido con el schema solicitado.";

  // Prefijo del prompt de usuario
  const userPrefix = profile?.prompt_user_prefix || "";

  // Reglas de estilo de imagen
  const styleRules = Array.isArray(profile?.image_style_rules) ? profile!.image_style_rules : [];
  const styleText = styleRules.map((x: any) => x?.rule).filter(Boolean).join("; ");

  // Hashtags seed
  const hashtagsSeed = Array.isArray(profile?.hashtag_seed) ? profile!.hashtag_seed : [];

  // CTA library
  const ctas = Array.isArray(profile?.cta_library) ? profile!.cta_library : [];

  // ✅ NUEVO: Construir prompt con identidad del vertical
  const userPrompt = `
${userPrefix}

VERTICAL: ${vertical_key}
TONO: ${profile?.tone || "claro y accionable"}
AUDIENCIA: ${profile?.audience || "general"}
REGLAS DE MARCA (json): ${JSON.stringify(profile?.brand_rules || {})}

ESTILO IMAGEN: ${styleText || "minimal, luz estudio, sin texto"}

SEED HASHTAGS: ${hashtagsSeed.join(" ")}
CTA SUGERIDAS: ${ctas.join(" | ")}

---
Ahora genera contenido para esta actividad:

Devuelve SOLO JSON con EXACTAMENTE estas claves:
{
  "title": string,
  "hook": string,
  "caption": string,
  "hashtags": string[],
  "cta": string,
  "image_prompts": string[]
}

Reglas:
- Idioma: ${locale === "en" ? "English" : "Spanish"}.
- Hashtags: 6 a 12 elementos, sin espacios raros. Usa los SEED HASHTAGS si son relevantes.
- image_prompts: 3 a 6 prompts, cada prompt debe describir una imagen clara. Aplica el ESTILO IMAGEN.
- No incluyas markdown. No incluyas comentarios. Solo JSON.

Contexto:
- lead_name: ${lead_name || "(unknown)"}
- topic: ${topic || "(none)"}
- offer: ${offer || "(none)"}
- brief: ${brief || "(none)"}
`.trim();

  let result: GenerateAssetsResult;

  if (dryRun) {
    // Placeholder determinista
    result = {
      title: `Draft ${vertical_key} (dry-run)`,
      hook: `Hook de prueba para ${vertical_key}`,
      caption: `Caption de prueba para ${vertical_key}.`,
      hashtags: ["#ai360plus", "#socialengine", "#draft", "#automation", "#crm", "#growth"],
      cta: "¿Quieres que lo dejemos listo para publicar?",
      image_prompts: [
        `High-quality realistic business scene related to ${vertical_key}, modern minimal style, neutral background`,
        `Close-up of a laptop dashboard UI representing CRM and automation, realistic lighting, no text`,
        `Professional team meeting, modern office, optimistic mood, realistic photo, no text`,
      ],
    };
  } else {
    try {
      if (!apiKey) throw new Error("Missing env OPENAI_API_KEY");

      result = await callOpenAI({
        model,
        apiKey,
        systemPrompt,
        userPrompt,
        traceId: trace_id,
      });
    } catch (err: any) {
      console.error("[AI ERROR]", err?.message ?? err);

      // 🔒 FALLBACK SEGURO (nunca rompe el worker)
      result = {
        title: `Draft ${vertical_key}`,
        hook: "Contenido pendiente de generación",
        caption: "No se pudo generar el contenido automáticamente. Reintentar.",
        hashtags: [],
        cta: "Contactar",
        image_prompts: [],
      };
    }
  }

  // ✅ NUEVO: Guardar metadata extendida
  // Incluimos: model, trace_id, vertical_key_used, profile_version (updated_at del profile)
  const insertRow = {
    org_id,
    lead_id,
    vertical_key,
    status: "draft",
    channel,
    title: result.title,
    hook: result.hook,
    caption: result.caption,
    hashtags: result.hashtags, // postgres array
    cta: result.cta,
    image_prompts: result.image_prompts, // jsonb array
    assets: [], // jsonb
    meta: {
      model,
      trace_id,
      vertical_key_used: vertical_key,
      profile_version: profile?.updated_at || profile?.created_at || null,
      source: "openai",
      dry_run: dryRun,
      job_id: job?.id ?? null,
      activity_id: job?.activity_id ?? null,
    },
  };

  const { data, error } = await supabase
    .from("social_outputs")
    .insert(insertRow)
    .select("id, org_id, lead_id, status, channel, vertical_key, created_at")
    .single();

  if (error) {
    throw new Error(`Insert social_outputs failed: ${error.message}`);
  }

  return {
    ok: true,
    trace_id,
    output_id: data?.id,
  };
}
