import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const SLEEP_IDLE_MS = 1200;
const SLEEP_ERROR_MS = 800;
const MAX_ATTEMPTS = 3;

const log = (...a) => console.log(new Date().toISOString(), ...a);

async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// Aquí, de momento, NO publicamos nada: solo “touch” stub (logs).
// Luego sustituyes por conectores (Meta/LinkedIn/etc).
async function performAction(job) {
  const { action, payload, org_id, lead_id, move_id } = job;
  log("[SOCIAL] perform", { action, org_id, lead_id, move_id, title: payload?.title });

  // Simula “éxito”
  return { ok: true };
}

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

  // paso a processing sólo si sigue pending
  const { error: uerr } = await supabase
    .from("social_queue")
    .update({ status: "processing", updated_at: new Date().toISOString() })
    .eq("id", job.id)
    .eq("status", "pending");

  if (uerr) return null; // otro worker lo pilló
  return job;
}

async function markDone(id) {
  await supabase
    .from("social_queue")
    .update({ status: "done", updated_at: new Date().toISOString() })
    .eq("id", id);
}

async function markFail(job, err) {
  const attempts = (job.attempts ?? 0) + 1;
  const failed = attempts >= MAX_ATTEMPTS;

  await supabase
    .from("social_queue")
    .update({
      status: failed ? "failed" : "pending",
      attempts,
      last_error: String(err?.message ?? err),
      updated_at: new Date().toISOString(),
    })
    .eq("id", job.id);
}

async function main() {
  log("[WORKER] Social Engine started");
  while (true) {
    try {
      const job = await claimOne();
      if (!job) {
        await sleep(SLEEP_IDLE_MS);
        continue;
      }

      try {
        await performAction(job);
        await markDone(job.id);
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
