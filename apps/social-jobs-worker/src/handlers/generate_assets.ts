// Generate Assets worker — FINAL FIXED for OpenAI Responses API
// -------------------------------------------------------------

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini";
const OPENAI_BASE = process.env.OPENAI_BASE_URL || "https://api.openai.com/v1";

if (!OPENAI_API_KEY) {
  throw new Error("Missing OPENAI_API_KEY");
}

function safeJson(v: any) {
  try { return JSON.stringify(v); } catch { return "{}"; }
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

      // ✅ FORMATO CORRECTO RESPONSES API 2025
      text: {
        format: {
          type: "json_schema",
          name: "social_output",   // ← CLAVE QUE FALTABA
          json_schema: schema,
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
    throw new Error(`OpenAI returned non-JSON output: ${textOut.slice(0, 500)}`);
  }
}

// -------------------------------------------------------------

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
