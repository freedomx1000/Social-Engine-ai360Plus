import { randomUUID } from "crypto";
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

/**
 * ✅ OBSERVABILIDAD PRO: Marca job como "running" al empezar a procesarlo
 * Incrementa attempts y guarda trace_id
 */
async function markRunning(job: SocialJobRow, trace_id: string) {
  const { error } = await supabase
    .from("social_jobs")
    .update({
      status: "running",
      attempts: (job.attempts ?? 0) + 1,
      last_trace_id: trace_id
    })
    .eq("id", job.id)
    .eq("locked_by", WORKER_ID);

  if (error) throw error;
}

/**
 * ✅ OBSERVABILIDAD PRO: Marca job como "done" con trace_id
 */
async function markDone(jobId: string, trace_id: string) {
  const { error } = await supabase
    .from("social_jobs")
    .update({
      status: "done",
      last_trace_id: trace_id,
      locked_at: null,
      locked_by: null
    })
    .eq("id", jobId)
    .eq("locked_by", WORKER_ID);

  if (error) throw error;
}

/**
 * ✅ OBSERVABILIDAD PRO: Marca job como "failed" con error details
 * Guarda: last_error, last_error_at, last_trace_id
 */
async function markFailed(job: SocialJobRow, error: Error | string, trace_id: string) {
  const attempts = Number(job.attempts ?? 0) + 1;
  const maxAttempts = Number(job.max_attempts ?? 5);
  const nextStatus = attempts >= maxAttempts ? "failed" : "queued";

  // Recortar mensaje de error a 900 caracteres
  const errorMsg = (error instanceof Error ? error.message : String(error)).slice(0, 900);

  const { error: updateError } = await supabase
    .from("social_jobs")
    .update({
      status: nextStatus,
      attempts,
      last_error: errorMsg,
      last_error_at: new Date().toISOString(),
      last_trace_id: trace_id,
      locked_at: null,
      locked_by: null
    })
    .eq("id", job.id)
    .eq("locked_by", WORKER_ID);

  if (updateError) throw updateError;

  // Si va a reintento, aplicar backoff
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

      // ✅ Generar trace_id para toda la ejecución
      const trace_id = randomUUID();

      console.log(
        `[${WORKER_ID}] processing job ${job.job_type} activity_id=${job.activity_id ?? "null"} trace_id=${trace_id}`
      );

      try {
        // ✅ Marcar como "running" antes de procesar
        await markRunning(job, trace_id);

        // Procesar según el job_type
        if (job.job_type === "generate_assets") {
          const res = await generate_assets(job);

          if (!res.ok) {
            console.error(`[${WORKER_ID}] generate_assets returned not ok for job ${job.id}`);
            await markFailed(job, "generate_assets_failed", trace_id);
            continue;
          }

          // ✅ Éxito: marcar como done
          await markDone(job.id, trace_id);
          console.log(`[${WORKER_ID}] ✅ done job ${job.job_type} id=${job.id} trace_id=${trace_id}`);
        } else {
          // job_type desconocido: marcarlo como failed
          console.error(`[${WORKER_ID}] unknown job_type: ${job.job_type}`);
          await markFailed(job, `unknown_job_type:${job.job_type}`, trace_id);
        }
      } catch (err: any) {
        // ✅ Error durante procesamiento: marcar como failed
        console.error(`[${WORKER_ID}] job processing error:`, err?.message ?? err);
        await markFailed(job, err, trace_id);
      }
    } catch (err: any) {
      // Error en el loop (claim, etc)
      console.error(`[${WORKER_ID}] loop error:`, err?.message ?? err);
      await sleep(SLEEP_ERROR_MS);
    }
  }
}
