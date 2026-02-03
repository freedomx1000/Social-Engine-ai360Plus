import { supabase } from "./supabase.js";
import type { SocialJobRow } from "./types.js";
import { generate_assets } from "./handlers/generate_assets.js";
const WORKER_ID =
  process.env.SOCIAL_WORKER_ID || `social-jobs-worker-${Math.random().toString(16).slice(2, 8)}`;

const SLEEP_IDLE_MS = Number(process.env.SOCIAL_SLEEP_IDLE_MS ?? 1500);
const SLEEP_ERROR_MS = Number(process.env.SOCIAL_SLEEP_ERROR_MS ?? 900);
const BACKOFF_BASE_MS = Number(process.env.SOCIAL_BACKOFF_BASE_MS ?? 2500);
const STUCK_AFTER_MS = Number(process.env.SOCIAL_STUCK_AFTER_MS ?? 600000);

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function backoffMs(attempts: number) {
  // 0->2.5s, 1->5s, 2->7.5s ... cap 30s
  return Math.min(30000, Math.max(1, attempts + 1) * BACKOFF_BASE_MS);
}

async function claimNext(): Promise<SocialJobRow | null> {
  const { data, error } = await supabase.rpc("social_jobs_claim_next", {
    p_worker: WORKER_ID,
    p_stuck_after_ms: STUCK_AFTER_MS
  });

  if (error) throw error;

  const rows = (data ?? []) as SocialJobRow[];
  return rows.length ? rows[0] : null;
}

async function markDone(jobId: string) {
  const { error } = await supabase
    .from("social_jobs")
    .update({ status: "done", locked_at: null, locked_by: null })
    .eq("id", jobId)
    .eq("locked_by", WORKER_ID);

  if (error) throw error;
}

async function markFailed(job: SocialJobRow, reason: string) {
  const attempts = Number(job.attempts ?? 0) + 1;
  const maxAttempts = Number(job.max_attempts ?? 5);

  const nextStatus = attempts >= maxAttempts ? "failed" : "queued";

  const payload = {
    ...(job.payload ?? {}),
    last_error: reason,
    last_error_at: new Date().toISOString()
  };

  const { error } = await supabase
    .from("social_jobs")
    .update({
      status: nextStatus,
      attempts,
      payload,
      locked_at: null,
      locked_by: null
    })
    .eq("id", job.id)
    .eq("locked_by", WORKER_ID);

  if (error) throw error;

  if (nextStatus === "queued") {
    await sleep(backoffMs(attempts));
  }
}

export async function runLoop() {
  console.log(`[${WORKER_ID}] booting...`);

  while (true) {
    try {
      const job = await claimNext();

      if (!job) {
        await sleep(SLEEP_IDLE_MS);
        continue;
      }

      console.log(
        `[${WORKER_ID}] processing job ${job.job_type} activity_id=${job.activity_id ?? "null"}`
      );

      if (job.job_type === "generate_assets") {
        const res = await generate_assets(job);
        if (!res.ok) {
          console.log(`[${WORKER_ID}] failed generate_assets: ${res.reason}`);
          await markFailed(job, String(res.reason));
          continue;
        }
      } else {
        // job_type desconocido: lo fallamos (o lo dejamos queued)
        await markFailed(job, `unknown_job_type:${job.job_type}`);
        continue;
      }

      await markDone(job.id);
      console.log(`[${WORKER_ID}] done job ${job.job_type} id=${job.id}`);
    } catch (err: any) {
      console.log(`[${WORKER_ID}] loop error:`, err?.message ?? err);
      await sleep(SLEEP_ERROR_MS);
    }
  }
}
