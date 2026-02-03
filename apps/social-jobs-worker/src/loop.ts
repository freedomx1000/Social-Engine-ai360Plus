import { supabase } from "./supabase.js";
import type { SocialJobRow } from "./types.js";
import { handleGenerateAssets } from "./handlers/generate_assets.js";

const WORKER_ID =
  process.env.WORKER_ID || `social-jobs-worker-${Math.random().toString(16).slice(2, 8)}`;

const SLEEP_IDLE_MS = Number(process.env.SOCIAL_SLEEP_IDLE_MS ?? 1500);
const SLEEP_ERROR_MS = Number(process.env.SOCIAL_SLEEP_ERROR_MS ?? 1200);

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function log(...args: any[]) {
  console.log(new Date().toISOString(), `[${WORKER_ID}]`, ...args);
}

/**
 * Claim (lock) atomically-ish:
 * - we select one queued job
 * - then update it with locked_by/locked_at + status=running
 * - and re-select to verify we got it
 *
 * NOTE: For true atomic claim, we'd use a SQL function (recommended later),
 * but this is stable enough to start with given low concurrency.
 */
async function claimOneJob(): Promise<SocialJobRow | null> {
  const { data: rows, error } = await supabase
    .from("social_jobs")
    .select("*")
    .eq("status", "queued")
    .order("created_at", { ascending: true })
    .limit(1);

  if (error) throw error;
  const job = rows?.[0] as SocialJobRow | undefined;
  if (!job) return null;

  const now = new Date().toISOString();

  // Try claim
  const { error: updErr } = await supabase
    .from("social_jobs")
    .update({
      status: "running",
      locked_at: now,
      locked_by: WORKER_ID,
      updated_at: now,
    })
    .eq("activity_id", job.activity_id)
    .eq("job_type", job.job_type)
    .eq("status", "queued"); // only claim if still queued

  if (updErr) throw updErr;

  // Verify we got it
  const { data: verify, error: vErr } = await supabase
    .from("social_jobs")
    .select("*")
    .eq("activity_id", job.activity_id)
    .eq("job_type", job.job_type)
    .limit(1);

  if (vErr) throw vErr;

  const claimed = verify?.[0] as SocialJobRow | undefined;
  if (!claimed) return null;
  if (claimed.locked_by !== WORKER_ID) return null;
  if (claimed.status !== "running") return null;

  return claimed;
}

async function markFailed(job: SocialJobRow, err: any) {
  const now = new Date().toISOString();
  const payload = {
    ...(job.payload ?? {}),
    error: {
      message: String(err?.message ?? err),
      stack: String(err?.stack ?? ""),
      ts: now,
    },
  };

  const { error } = await supabase
    .from("social_jobs")
    .update({
      status: "failed",
      attempts: (job.attempts ?? 0) + 1,
      payload,
      updated_at: now,
    })
    .eq("activity_id", job.activity_id)
    .eq("job_type", job.job_type);

  if (error) throw error;
}

async function requeue(job: SocialJobRow, err: any) {
  const now = new Date().toISOString();
  const nextAttempts = (job.attempts ?? 0) + 1;

  const payload = {
    ...(job.payload ?? {}),
    last_error: {
      message: String(err?.message ?? err),
      ts: now,
    },
  };

  const { error } = await supabase
    .from("social_jobs")
    .update({
      status: "queued",
      attempts: nextAttempts,
      locked_at: null,
      locked_by: null,
      payload,
      updated_at: now,
    })
    .eq("activity_id", job.activity_id)
    .eq("job_type", job.job_type);

  if (error) throw error;
}

async function processJob(job: SocialJobRow) {
  log("processing job", job.job_type, "activity_id=", job.activity_id);

  if (job.job_type === "generate_assets") {
    await handleGenerateAssets(job);
    log("done job", job.job_type, "activity_id=", job.activity_id);
    return;
  }

  throw new Error(`Unknown job_type: ${job.job_type}`);
}

export async function runLoop() {
  log("booting...");
  while (true) {
    try {
      const job = await claimOneJob();
      if (!job) {
        await sleep(SLEEP_IDLE_MS);
        continue;
      }

      try {
        await processJob(job);
      } catch (err: any) {
        const attempts = job.attempts ?? 0;
        const maxAttempts = job.max_attempts ?? 5;

        if (attempts + 1 >= maxAttempts) {
          log("job failed (max attempts)", job.activity_id, err?.message ?? err);
          await markFailed(job, err);
        } else {
          log("job error, requeue", job.activity_id, err?.message ?? err);
          await requeue(job, err);
        }
      }
    } catch (err: any) {
      log("loop error", err?.message ?? err);
      await sleep(SLEEP_ERROR_MS);
    }
  }
}
