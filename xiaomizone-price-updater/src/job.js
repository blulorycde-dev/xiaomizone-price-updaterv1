// src/job.js

const JOB_KEY = "price_job";
const LOG_KEY = "price_log";

export async function getJob(env) {
  const raw = await env.JOBS_KV.get(JOB_KEY);
  if (!raw) {
    return {
      status: "idle", // idle | queued | running | completed | cancelled | error
      startedAt: null,
      updatedAt: null,
      finishedAt: null,
      processed: 0,
      totalVariants: 0,
      cursor: null,
      rate: null,
      margin: null,
      roundStep: null,
      mode: "update",
      lastError: null,
    };
  }
  try {
    return JSON.parse(raw);
  } catch {
    return {
      status: "error",
      lastError: "Error parsing JOBS_KV JSON",
    };
  }
}

export async function saveJob(env, job) {
  job.updatedAt = new Date().toISOString();
  await env.JOBS_KV.put(JOB_KEY, JSON.stringify(job));
  return job;
}

export async function appendLog(env, entry) {
  const timestamp = new Date().toISOString();
  const line = `[${timestamp}] ${entry}`;
  await env.LOG_KV.put(`${LOG_KEY}:${timestamp}`, line);
}

export async function startJob(env, params) {
  const now = new Date().toISOString();
  const job = {
    status: "queued",
    startedAt: now,
    updatedAt: now,
    finishedAt: null,
    processed: 0,
    totalVariants: params.totalVariants,
    cursor: null,
    rate: params.rate,
    margin: params.margin,
    roundStep: params.roundStep,
    mode: params.mode || "update",
    lastError: null,
  };
  await saveJob(env, job);
  await appendLog(
    env,
    `Nuevo job iniciado: rate=${job.rate}, margin=${job.margin}, round=${job.roundStep}, total=${job.totalVariants}, mode=${job.mode}`
  );
  return job;
}

export async function cancelJob(env) {
  const job = await getJob(env);
  job.status = "cancelled";
  job.finishedAt = new Date().toISOString();
  await saveJob(env, job);
  await appendLog(env, "Job cancelado manualmente");
  return job;
}

export async function finishJob(env, job, status = "completed") {
  job.status = status;
  job.finishedAt = new Date().toISOString();
  await saveJob(env, job);
  await appendLog(
    env,
    `Job finalizado con estado=${status}, processed=${job.processed}/${job.totalVariants}`
  );
  return job;
}

