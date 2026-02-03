import { supabase } from "./supabase.js";
import type { SocialJobRow } from "./types.js";
import { handleGenerateAssets } from "./handlers/generate_assets.js";

const WORKER_ID =
  process.env.WORKER_ID || `social-jobs-worker-${Math.random().toString(16).slice(2, 8)}`;

const SLEEP_IDLE_MS = Number(process.env.SOCIAL_SLEEP_IDLE_MS ?? 1500);
const SLEEP_ERROR_MS = Number(process.env.SOCIAL_SLEEP_ERROR_MS ?? 900);
const STUCK_AFTER_MS = Number(process.env.SOCIAL_STUCK_AFTER_MS ?? 10 * 60 * 1000);

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function nowIso() {
  return new Date().toISOString();
}

async function unlockStuckJobs() {
  // Si un job quedó "running" demasiado tiempo, lo devolvemos a queued
  const cutoff = new Date(Date.now() - STUCK_AFTER_MS).toISOString();

  await supabase
    .from("social_jobs")
    .update({ status: "queued", locked_at: null, locked_by: null })
    .eq("status", "running")
    .lt("locked_at", cutoff);
}

async function lockNextJob(): Promise<SocialJobRow | null> {
  // 1) Candidato
  const { data: candidate } = await supabase
    .from("social_jobs")
    .select("*")
    .eq("status", "queued")
    .order("created_at", { ascending: true })
    .limit(1)
    .maybeSingle<SocialJobRow>();

  if (!candidate) return null;

  // 2) Lock optimista (solo si sigue queued y unlocked)
  const { data: locked } = await supabase
    .from("social_jobs")
    .update({ status: "running", locked_at: nowIso(), locked_by: WORKER_ID })
    .eq("id", candidate.id)
    .eq("status", "queued")
    .is("locked_at", null)
    .select("*")
    .maybeSingle<SocialJobRow>();

  return locked ?? null;
}

async function markDone(jobId: string, result: any) {
  await supabase
    .from("social_jobs")
    .update({
      status: "done",
      payload: result,
      locked_at: null,
      locked_by: null,
      updated_at: nowIso()
    })
    .eq("id", jobId);
}

async function markFailed(job: SocialJobRow, errMsg: string) {
  const attempts = (job.attempts ?? 0) + 1;
  const max = job.max_attempts ?? 3;
  const terminal = attempts >= max;

  await supabase
    .from("social_jobs")
    .update({
      attempts,
      status: terminal ? "failed" : "queued",
      locked_at: null,
      locked_by: null,
      payload: {
        ...(job.payload ?? {}),
        last_error: errMsg,
        last_error_at: nowIso(),
        attempts,
        terminal
      },
      updated_at: nowIso()
    })
    .eq("id", job.id);
}

async function processJob(job: SocialJobRow) {
  if (job.job_type === "generate_assets") {
    const res = await handleGenerateAssets(job);
    await markDone(job.id, res);
    return;
  }
  throw new Error(`Unknown job_type: ${job.job_type}`);
}

export async function runLoop() {
  // Pequeño self-heal
  await unlockStuckJobs();

  while (true) {
    try {
      const job = await lockNextJob();
      if (!job) {
        await sleep(SLEEP_IDLE_MS);
        continue;
      }

      try {
        await processJob(job);
      } catch (err: any) {
        await markFailed(job, String(err?.message ?? err));
      }
    } catch (err) {
      // Si falla supabase o la red, dormimos y seguimos
      await sleep(SLEEP_ERROR_MS);
    }
  }
}
