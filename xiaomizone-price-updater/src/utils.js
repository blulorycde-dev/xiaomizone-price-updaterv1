// src/utils.js

export function cors(response) {
  response.headers.set("Access-Control-Allow-Origin", "*");
  response.headers.set("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  response.headers.set(
    "Access-Control-Allow-Headers",
    "Content-Type, Authorization, X-Admin-Pin"
  );
  return response;
}

export function jsonResponse(obj, status = 200) {
  const res = new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
  return cors(res);
}

export function textResponse(
  text,
  status = 200,
  contentType = "text/plain; charset=utf-8"
) {
  const res = new Response(text, {
    status,
    headers: { "Content-Type": contentType },
  });
  return cors(res);
}

export function roundToStep(value, step) {
  if (!step || step <= 0) return Math.round(value);
  return Math.round(value / step) * step;
}

// Pequeño retraso para evitar throttling de Shopify
export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
