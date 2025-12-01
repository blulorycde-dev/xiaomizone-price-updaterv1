// src/env.js

export function getConfig(env) {
  const apiVersion = env.API_VERSION || "2024-10";

  return {
    apiVersion,

    // PIN panel admin
    adminPin: env.ADMIN_PIN, // 0527-6

    // Shopify
    storeDomain: env.SHOPIFY_STORE_DOMAIN, // 0bjaat-hq.myshopify.com
    adminToken: env.SHOPIFY_ADMIN_TOKEN,   // secreto

    // Parámetros por defecto para precios
    manualRate: Number(env.MANUAL_RATE || "7200"),        // MANUAL_RATE
    manualMargin: Number(env.MARGIN_FACTOR || "1.00"),    // MARGIN_FACTOR
    roundStep: Number(env.ROUND_TO || "100"),             // ROUND_TO

    // Pista aproximada de cuántas variantes procesar
    totalVariantsHint: Number(env.TOTAL_VARIANTS_HINT || "500"),

    // Opcionales (si no existen, usan estos valores)
    batchLimit: Number(env.BATCH_LIMIT || "20"),
    throttleMs: Number(env.REQ_THROTTLE_MS || "600"),

    cronMinutes: Number(env.CRON_MINUTES || "2"),

    statusTimezone: env.STATUS_TIMEZONE || "America/Asuncion",
  };
}
