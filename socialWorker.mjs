import { createClient } from "@supabase/supabase-js";
import OpenAI from "openai";

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
if (!OPENAI_API_KEY) throw new Error("Missing OPENAI_API_KEY");

const openai = new OpenAI({ apiKey: OPENAI_API_KEY });


const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const SLEEP_IDLE_MS = Number(process.env.SOCIAL_SLEEP_IDLE_MS ?? 1500);
const SLEEP_ERROR_MS = Number(process.env.SOCIAL_SLEEP_ERROR_MS ?? 900);
const MAX_ATTEMPTS = Number(process.env.SOCIAL_MAX_ATTEMPTS ?? 3);

// Si un job lleva "processing" más de X, lo devolvemos a pending
const STUCK_AFTER_MS = Number(process.env.SOCIAL_STUCK_AFTER_MS ?? 10 * 60 * 1000);

// Backoff base para reintentos (se multiplica por attempts)
const BACKOFF_BASE_MS = Number(process.env.SOCIAL_BACKOFF_BASE_MS ?? 2500);

// Worker identity (para audit/debug)
const WORKER_ID =
  process.env.SOCIAL_WORKER_ID ?? `social-${Math.random().toString(16).slice(2, 8)}`;

const log = (...a) => console.log(new Date().toISOString(), `[${WORKER_ID}]`, ...a);

async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function backoffMs(attempts) {
  // 0 -> 0.., 1 -> ~2.5s, 2 -> ~5s, 3 -> ~7.5s (top 30s)
  return Math.min(30000, attempts * BACKOFF_BASE_MS);
}

async function fetchLeadContext(orgId, leadId) {
  const { data, error } = await supabase
    .from("leads")
    .select(
      "id, title, name, company, city, country, phone, email, notes, meta, source"
    )
    .eq("org_id", orgId)
    .eq("id", leadId)
    .single();

  if (error) throw error;
  return data;
}

async function generateSocialKit({ verticalKey, goal, channels, lead }) {
  const prompt = {
    role: "user",
    content: [
      "Eres un copywriter de respuesta directa para RRSS.",
      "Genera un pack listo para publicar que atraiga al gran público pero sin humo.",
      "",
      `Vertical: ${verticalKey}`,
      `Objetivo: ${goal}`,
      `Canales: ${channels.join(", ")}`,
      "",
      "Contexto del lead/cliente (puede venir incompleto):",
      JSON.stringify(lead ?? {}, null, 2),
      "",
      "Devuelve SOLO JSON válido con esta forma:",
      `{
        "title": "...",
        "hook": "...",
        "caption": "...",
        "cta": "...",
        "hashtags": ["#..."],
        "image_prompts": [
          {"type":"image","prompt":"..."},
          {"type":"image","prompt":"..."},
          {"type":"image","prompt":"..."}
        ],
        "style": {"tone":"...","voice":"...","format":"..."}
      }`,
      "",
      "Reglas:",
      "- Español neutro (si no se especifica país).",
      "- Evita claims ilegales (salud/finanzas), y evita promesas absolutas.",
      "- Copy corto, claro, con una idea potente.",
      "- Que parezca humano.",
    ].join("\n"),
  };

  const resp = await openai.chat.completions.create({
    model: "gpt-4.1-mini",
    temperature: 0.7,
    messages: [prompt],
    response_format: { type: "json_object" },
  });

  const text = resp.choices?.[0]?.message?.content ?? "{}";
  return JSON.parse(text);
}

// Stub: aquí luego conectas Meta/LinkedIn/etc.
async function performAction(job) {
  const { action, payload, org_id, lead_id, vertical_key } = job;

  if (action !== "generate_post") {
    log("[SOCIAL] skip action", action);
    return { ok: true, skipped: true };
  }

  const lead = lead_id ? await fetchLeadContext(org_id, lead_id) : null;

  const kit = await generateSocialKit({
    verticalKey: payload?.verticalKey ?? vertical_key ?? "general",
    goal: payload?.goal ?? "sell",
    channels: payload?.channels ?? ["instagram", "facebook", "linkedin"],
    lead,
  });

  const { error: ierr } = await supabase.from("social_outputs").insert({
    org_id,
    lead_id,
    vertical_key: payload?.verticalKey ?? vertical_key ?? "general",
    status: "draft",
    channel: "multi",
    title: kit.title ?? null,
    hook: kit.hook ?? null,
    caption: kit.caption ?? null,
    cta: kit.cta ?? null,
    hashtags: kit.hashtags ?? [],
    image_prompts: kit.image_prompts ?? [],
    assets: [],
    meta: kit.style ?? {},
  });

  if (ierr) throw ierr;

  log("[SOCIAL] draft created in social_outputs");
  return { ok: true };
}
// Audit trail en audit_log
async function audit(orgId, action, payload) {
  const SYSTEM_USER_ID = process.env.SYSTEM_USER_ID; // uuid sistema
  if (!SYSTEM_USER_ID) return;

  const { error } = await supabase.from("audit_log").insert({
    org_id: orgId,
    actor_user_id: SYSTEM_USER_ID,
    actor_mode: "direct",
    actor_agency_org_id: null,
    action,
    payload,
  });

  if (error) {
    log("[AUDIT] error", error);
  }
}

// Requeue de jobs atascados en processing
async function requeueStuck() {
  const thresholdIso = new Date(Date.now() - STUCK_AFTER_MS).toISOString();

  const { error } = await supabase
    .from("social_queue")
    .update({
      status: "pending",
      last_error: "requeued: stuck processing timeout",
      updated_at: new Date().toISOString(),
    })
    .eq("status", "processing")
    .lt("claimed_at", thresholdIso);

  if (error) log("[WORKER] requeueStuck error", error);
}

// Claim atómico
async function claimOne() {
  const { data, error } = await supabase
    .from("social_queue")
    .select("*")
    .eq("status", "pending")
    .order("created_at", { ascending: true })
    .limit(1);

  if (error) throw error;
  const job = data?.[0];
  if (!job) return null;

  const nowIso = new Date().toISOString();
  const { data: upd, error: uerr } = await supabase
    .from("social_queue")
    .update({
      status: "processing",
      claimed_at: nowIso,
      updated_at: nowIso,
    })
    .eq("id", job.id)
    .eq("status", "pending")
    .select(
      "id,status,claimed_at,attempts,action,payload,org_id,lead_id,move_id,created_at,updated_at,last_error"
    );

  if (uerr) throw uerr;
  if (!upd || upd.length === 0) return null;
  return upd[0];
}

async function markDone(job) {
  const nowIso = new Date().toISOString();
  await supabase
    .from("social_queue")
    .update({ status: "done", updated_at: nowIso })
    .eq("id", job.id);

  await audit(job.org_id, "social.done", {
    id: job.id,
    action: job.action,
    lead_id: job.lead_id,
    move_id: job.move_id,
  });
}

async function markFail(job, err) {
  const attempts = (job.attempts ?? 0) + 1;
  const failed = attempts >= MAX_ATTEMPTS;
  const nowIso = new Date().toISOString();
  const errorText = String(err?.message ?? err);

  await supabase
    .from("social_queue")
    .update({
      status: failed ? "failed" : "pending",
      attempts,
      last_error: errorText,
      updated_at: nowIso,
    })
    .eq("id", job.id);

  await audit(job.org_id, failed ? "social.failed" : "social.retry", {
    id: job.id,
    action: job.action,
    attempts,
    error: errorText,
  });

  const wait = backoffMs(attempts);
  if (!failed && wait > 0) {
    log("[WORKER] backoff", { id: job.id, attempts, wait });
    await sleep(wait);
  }
}

async function main() {
  log("[WORKER] Social Engine started");

  let lastStuckSweep = 0;

  while (true) {
    try {
      const now = Date.now();
      if (now - lastStuckSweep > 30000) {
        lastStuckSweep = now;
        await requeueStuck();
      }

      const job = await claimOne();
      if (!job) {
        await sleep(SLEEP_IDLE_MS);
        continue;
      }

      try {
        await performAction(job);
        await markDone(job);
      } catch (err) {
        log("[WORKER] job error", err);
        await markFail(job, err);
      }
    } catch (e) {
      log("[WORKER] loop error", e);
      await sleep(SLEEP_ERROR_MS);
    }
  }
}

main();
