import { supabase } from "../supabase.js";
import type { SocialJobRow } from "../types.js";

export async function handleGenerateAssets(job: SocialJobRow) {
  // Aquí va tu pipeline real (copy, imagen, vídeo, etc.)
  // Por ahora: dejamos huella determinista y cerramos el job correctamente.

  const resultPayload = {
    ok: true,
    generated: {
      kind: "assets_stub",
      ts: new Date().toISOString(),
    },
    input: job.payload ?? {},
  };

  // Opcional: escribir actividad "worker_done" en crm_lead_activity (si existe)
  // (si no quieres esto, lo quitamos)
  try {
    await supabase.from("crm_lead_activity").insert({
      org_id: job.org_id,
      lead_id: job.lead_id,
      kind: "worker_done",
      message: "Social job processed: generate_assets",
      meta: { job_type: job.job_type },
      payload: resultPayload,
    });
  } catch {
    // No bloquea el flujo si la tabla o columnas cambian
  }

  // Marcar el job DONE
  const { error } = await supabase
    .from("social_jobs")
    .update({
      status: "done",
      payload: { ...(job.payload ?? {}), result: resultPayload },
      updated_at: new Date().toISOString(),
    })
    .eq("activity_id", job.activity_id)
    .eq("job_type", job.job_type);

  if (error) throw error;
}
