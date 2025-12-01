// src/index.js

import { getConfig } from "./env.js";
import { jsonResponse, textResponse } from "./utils.js";
import { getJob, startJob, cancelJob, saveJob, finishJob } from "./job.js";
import { processPriceUpdateBatch } from "./shopify.js";
import { renderPanelHtml } from "./panel.js";

function isOptions(req) {
  return req.method === "OPTIONS";
}

// Autenticación simple para rutas /admin/*
async function requireAdmin(req, config) {
  const url = new URL(req.url);
  const pinHeader = req.headers.get("X-Admin-Pin");
  const pinQuery = url.searchParams.get("pin");
  const pin = pinHeader || pinQuery;

  if (!config.adminPin) {
    return {
      ok: false,
      res: jsonResponse({ error: "ADMIN_PIN no configurado" }, 500),
    };
  }

  if (!pin || pin !== config.adminPin) {
    return {
      ok: false,
      res: jsonResponse({ error: "PIN inválido o ausente" }, 401),
    };
  }

  return { ok: true };
}

// Maneja un tick del cron: procesa un batch del job si está activo
async function handleScheduledTick(env, config) {
  let job = await getJob(env);

  if (job.status !== "queued" && job.status !== "running") {
    return;
  }

  job.status = "running";
  job = await saveJob(env, job);

  try {
    const { processedDelta, hasMore, newCursor } =
      await processPriceUpdateBatch(env, config, job);

    job.processed += processedDelta;
    job.cursor = newCursor;

    if (!hasMore || (job.totalVariants && job.processed >= job.totalVariants)) {
      await finishJob(env, job, "completed");
    } else {
      job.status = "queued";
      await saveJob(env, job);
    }
  } catch (err) {
    job.lastError = String(err?.message || err);
    await finishJob(env, job, "error");
  }
}

export default {
  async fetch(req, env, ctx) {
    const config = getConfig(env);

    if (isOptions(req)) {
      return textResponse("", 204);
    }

    const url = new URL(req.url);
    const { pathname } = url;

    // Panel visual
    if (pathname === "/" || pathname === "/panel") {
      const html = renderPanelHtml();
      return textResponse(html, 200, "text/html; charset=utf-8");
    }

    // Estado del job (sin PIN, solo lectura)
    if (pathname === "/status") {
      const job = await getJob(env);
      return jsonResponse({
        status: job.status,
        startedAt: job.startedAt,
        updatedAt: job.updatedAt,
        finishedAt: job.finishedAt,
        processed: job.processed,
        totalVariants: job.totalVariants,
        cursor: job.cursor,
        rate: job.rate,
        margin: job.margin,
        roundStep: job.roundStep,
        mode: job.mode,
        lastError: job.lastError,
      });
    }

    // Iniciar job
    if (pathname === "/admin/start") {
      const auth = await requireAdmin(req, config);
      if (!auth.ok) return auth.res;

      const rate = Number(url.searchParams.get("rate") || config.manualRate);
      const margin = Number(
        url.searchParams.get("margin") || config.manualMargin
      );
      const roundStep = Number(
        url.searchParams.get("round") || config.roundStep
      );
      const totalVariants = Number(
        url.searchParams.get("total_variants") || config.totalVariantsHint
      );
      const mode = url.searchParams.get("mode") || "update";

      const job = await startJob(env, {
        rate,
        margin,
        roundStep,
        totalVariants,
        mode,
      });

      return jsonResponse({
        message: "Job iniciado",
        job,
      });
    }

    // Cancelar job
    if (pathname === "/admin/cancel") {
      const auth = await requireAdmin(req, config);
      if (!auth.ok) return auth.res;

      const job = await cancelJob(env);
      return jsonResponse({
        message: "Job cancelado",
        job,
      });
    }

    // Endpoint reservado para futuro reset de base_usd
    if (pathname === "/admin/reset-base") {
      const auth = await requireAdmin(req, config);
      if (!auth.ok) return auth.res;

      return jsonResponse({
        message:
          "Endpoint /admin/reset-base listo. Falta implementar la lógica específica.",
      });
    }

    // Endpoint reservado para futura lista tipo Excel
    if (pathname === "/admin/base-list") {
      const auth = await requireAdmin(req, config);
      if (!auth.ok) return auth.res;

      return jsonResponse({
        message: "Endpoint /admin/base-list listo para implementar.",
        hint: "Aquí devolverás la lista de variantes con base_usd para tu panel.",
      });
    }

    return jsonResponse({ error: "Ruta no encontrada", path: pathname }, 404);
  },

  async scheduled(event, env, ctx) {
    const config = getConfig(env);
    ctx.waitUntil(handleScheduledTick(env, config));
  },
};
