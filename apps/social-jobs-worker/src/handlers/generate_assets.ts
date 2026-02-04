// Generate Assets worker — patched: response_format -> text.format
// (based on your Generate_Assets.txt)
// ---------------------------------------------------------------

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini"; // pon aquí el que tengas disponible
const OPENAI_BASE = process.env.OPENAI_BASE_URL || "https://api.openai.com/v1";

if (!OPENAI_API_KEY) {
  throw new Error("Missing OPENAI_API_KEY");
}

function safeJson(v: any) {
  try { return JSON.stringify(v); } catch { return "{}"; }
}

function pickString(...vals: Array<any>) {
  for (const v of vals) {
    if (typeof v === "string" && v.trim()) return v.trim();
  }
  return "";
}

async function openaiResponsesJSON({ system, user, schema, temperature = 0.2 }: {
  system: string;
  user: string;
  schema: any;
  temperature?: number;
}) {
  const resp = await fetch(`${OPENAI_BASE}/responses`, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: OPENAI_MODEL,
      temperature,

      // ✅ FIX: Responses API now expects this under text.format
      text: { format: { type: "json_schema", json_schema: schema } },

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

  // Responses API: easiest is output_text if available
  const textOut =
    data?.output_text ??
    data?.output?.[0]?.content?.find?.((c: any) => c?.type === "output_text")?.text ??
    data?.output?.[0]?.content?.[0]?.text;

  if (!textOut) throw new Error(`OpenAI returned no text. payload=${safeJson(data)}`);

  // Should be JSON because we requested json_schema
  let parsed: any = null;
  try {
    parsed = JSON.parse(textOut);
  } catch {
    throw new Error(`OpenAI returned non-JSON output. text=${textOut.slice(0, 500)}`);
  }

  return parsed;
}

// ---------------------------------------------------------------
// Tu lógica existente: schema + build prompt + write to social_outputs
// Solo asegúrate de que "schema" tiene forma compatible.
// ---------------------------------------------------------------

export async function generateAssetsWithOpenAI({
  systemPreamble,
  prompt,
  schema,
}: {
  systemPreamble: string;
  prompt: string;
  schema: any;
}) {
  return openaiResponsesJSON({
    system: systemPreamble,
    user: prompt,
    schema,
    temperature: 0.2,
  });
}

// ⚠️ El resto de tu worker (DB reads/writes, jobs loop, etc.) se queda igual.
// ---------------------------------------------------------------
