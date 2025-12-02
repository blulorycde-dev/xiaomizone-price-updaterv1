// Worker: price-updater con panel /admin (tema claro),
// modos update/reset, log y lista de productos

const API_VERSION = "2024-10";
const JOB_KEY = "price_job";
const LOG_KEY = "price_log";

const BATCH_LIMIT = 10;        // variantes por corrida del cron
const REQ_THROTTLE_MS = 600;   // espera entre peticiones
const STATUS_TIMEZONE = "America/Asuncion";

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    const path = url.pathname.replace(/\/+$/, "") || "/";

    // valida que haya dominio/token (lanza error si falta algo)
    getShop(env);

    if (req.method === "OPTIONS") {
      return cors(new Response(null, { status: 204 }));
    }

    // ---------- STATUS ----------
    if (path === "/status") {
      const job = await getJob(env);
      const body = job || { running: false, msg: "no job" };

      if (job?.running || typeof job?.processedVariants === "number") {
        const eta = computeETA(job);
        body.eta = eta;
        if (eta?.etaMinutes && Number.isFinite(eta.etaMinutes)) {
          const end = new Date(Date.now() + eta.etaMinutes * 60000);
          body.projectedEndLocal = end.toLocaleString("es-PY", {
            timeZone: STATUS_TIMEZONE,
          });
        }
      }

      return cors(json(body));
    }

    // ---------- PANEL ADMIN (HTML) ----------
    if (path === "/admin") {
      const html = `
<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <title>Price Updater</title>
  <style>
    :root { color-scheme: light; }
    * { box-sizing: border-box; }
    body {
      font-family: Arial, sans-serif;
      background: #f4f5f7;
      color: #222;
      margin: 0;
      padding: 24px;
    }
    .box {
      background: #ffffff;
      padding: 20px 24px;
      border-radius: 12px;
      max-width: 980px;
      margin: 0 auto;
      box-shadow: 0 8px 20px rgba(0,0,0,0.06);
      border: 1px solid #e2e4ea;
    }
    h1 { margin: 0 0 4px; font-size: 22px; }
    h3 { margin: 0 0 6px; font-size: 16px; }
    small {
      display:block;
      color:#777;
      margin-bottom:16px;
    }
    label {
      display: block;
      margin-top: 10px;
      font-size: 13px;
      font-weight: 600;
      color:#444;
    }
    input, select {
      width: 100%;
      padding: 8px 10px;
      font-size: 14px;
      border-radius: 6px;
      margin-top: 4px;
      border: 1px solid #cfd3dd;
      background: #fff;
      color: #222;
    }
    input:focus {
      outline: none;
      border-color: #ff6600;
      box-shadow: 0 0 0 1px rgba(255,102,0,0.25);
    }
    button {
      margin-top: 12px;
      padding: 8px 14px;
      font-size: 14px;
      background: #ff6600;
      border: none;
      color: #fff;
      cursor: pointer;
      border-radius: 6px;
      font-weight: 600;
    }
    button.secondary {
      background:#e4e7ef;
      color:#333;
    }
    button:hover { background: #ff7f24; }
    button.secondary:hover { background:#d5d9e6; }
    .row {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 8px;
    }
    .row button { flex: 1; }
    .section {
      margin-top: 18px;
      padding-top: 14px;
      border-top: 1px solid #e0e3ec;
    }
    .section-header {
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:8px;
    }
    .hint {
      font-size: 12px;
      color:#777;
      margin: 4px 0 8px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 6px;
    }
    th, td {
      padding: 6px 8px;
      font-size: 13px;
      border-bottom: 1px solid #e2e4ea;
      vertical-align: middle;
    }
    th {
      text-align: left;
      background:#f5f6fa;
      font-weight:600;
    }
    td.numeric {
      text-align:right;
      white-space:nowrap;
    }
    .tag {
      display:inline-block;
      padding:2px 6px;
      border-radius:999px;
      background:#eef2ff;
      color:#333;
      font-size:11px;
    }
    .badge {
      display:inline-block;
      padding:2px 6px;
      border-radius:999px;
      font-size:11px;
      background:#eaf7ff;
      color:#1362a3;
    }
    .badge-danger {
      background:#ffe9e6;
      color:#b0302b;
    }
    .pill {
      display:inline-flex;
      align-items:center;
      gap:4px;
      padding:3px 8px;
      border-radius:999px;
      font-size:11px;
      background:#f8fafc;
      border:1px solid #e2e8f0;
    }
    .pill-dot {
      width:6px;
      height:6px;
      border-radius:50%;
      background:#27ae60;
    }
    .pill-dot.red { background:#e74c3c; }
    .toolbar {
      display:flex;
      align-items:center;
      gap:8px;
      margin-top:8px;
    }
    .toolbar input { max-width:260px; }
    .toolbar button {
      margin-top:0;
      flex:0 0 auto;
    }
    .table-wrap {
      max-height:420px;
      overflow:auto;
      margin-top:6px;
      border-radius:8px;
      border:1px solid #e2e4ea;
      background:#fff;
    }
    .footer-note {
      margin-top:8px;
      font-size:11px;
      color:#777;
    }
  </style>
</head>
<body>
  <div class="box">
    <h1>Price Updater</h1>
    <small>Worker: price-updater · Admin protegido por PIN</small>

    <label>PIN de administrador</label>
    <input type="password" id="pin" placeholder="Tu PIN secreto">

    <div class="section">
      <div class="section-header">
        <h3>Actualizar precios masivos</h3>
        <span class="pill">
          <span class="pill-dot"></span>
          Modo: update
        </span>
      </div>
      <p class="hint">
        Usa la tasa, margen y redondeo para recalcular precios en guaraní a partir de <b>pricing.base_usd</b>.
      </p>
      <form id="form-update">
        <div class="row">
          <div style="flex:1 1 120px;">
            <label>Tasa USD → PYG</label>
            <input type="number" name="rate" step="0.01" required placeholder="Ej: 7200">
          </div>
          <div style="flex:1 1 120px;">
            <label>Margen</label>
            <input type="number" name="margin" step="0.01" required placeholder="Ej: 1.20">
          </div>
          <div style="flex:1 1 120px;">
            <label>Redondeo (Gs)</label>
            <input type="number" name="round" required placeholder="Ej: 100">
          </div>
        </div>
        <div class="row">
          <div style="flex:1 1 120px;">
            <label>Variantes a procesar (hint)</label>
            <input type="number" name="total_variants" required placeholder="Ej: 500">
          </div>
          <div style="flex:1 1 120px;">
            <label>Cron (minutos)</label>
            <input type="number" name="cron_min" required placeholder="Ej: 2">
          </div>
        </div>

        <button type="submit">Iniciar actualización de precios</button>
      </form>
    </div>

    <div class="section">
      <div class="section-header">
        <h3>Reset Base USD (sin cambiar precios)</h3>
        <span class="pill">
          <span class="pill-dot red"></span>
          Modo: reset_base
        </span>
      </div>
      <p class="hint">
        Recalcula <b>pricing.base_usd</b> a partir del <b>precio PYG actual</b> y la tasa elegida. No toca el precio de venta.
      </p>
      <form id="form-reset">
        <div class="row">
          <div style="flex:1 1 120px;">
            <label>Tasa USD → PYG</label>
            <input type="number" name="rate" step="0.01" required placeholder="Ej: 7200">
          </div>
          <div style="flex:1 1 120px;">
            <label>Variantes a procesar (hint)</label>
            <input type="number" name="total_variants" required placeholder="Ej: 500">
          </div>
          <div style="flex:1 1 120px;">
            <label>Cron (minutos)</label>
            <input type="number" name="cron_min" required placeholder="Ej: 2">
          </div>
        </div>
        <button type="submit">Resetear base_usd</button>
      </form>
    </div>

    <div class="section">
      <h3>Gestión manual Base USD (por variante)</h3>
      <p class="hint">
        Útil para corregir un error puntual conociendo el <b>ID de variante</b>.
      </p>
      <form id="form-base">
        <div class="row">
          <div style="flex:1 1 140px;">
            <label>ID de variante</label>
            <input type="text" name="variantId" placeholder="Ej: 5153…">
          </div>
          <div style="flex:1 1 140px;">
            <label>Base USD</label>
            <input type="number" name="baseUsd" step="0.01" placeholder="Ej: 10">
          </div>
        </div>
        <button type="submit" class="secondary">Guardar Base USD (1 variante)</button>
      </form>
    </div>

    <div class="section">
      <div class="section-header">
        <h3>Lista de productos</h3>
        <span class="tag">Vista tipo editor masivo</span>
      </div>
      <p class="hint">
        Biblioteca de productos con orden alfabético. Podés buscar por nombre o SKU y editar <b>Base USD</b> directo en la tabla.
        La columna <b>Tasa estimada</b> es PYG / Base USD, útil para detectar valores raros.
      </p>

      <div class="toolbar">
        <input type="text" id="base-q" placeholder="Buscar por nombre, SKU, etc.">
        <button type="button" id="btn-load-base" class="secondary">Buscar / refrescar</button>
        <button type="button" id="btn-clear-table" class="secondary">Limpiar tabla</button>
      </div>

      <div class="table-wrap" id="base-table"></div>
      <div class="footer-note">
        Tip: hacé varias búsquedas específicas (por marca, categoría, etc.) para revisar rápido sin CSV.
      </div>
    </div>

    <div class="section">
      <h3>Control rápido del job</h3>
      <div class="row">
        <button id="btn-status" type="button" class="secondary">Ver status</button>
        <button id="btn-cancel" type="button" class="secondary">Cancelar job</button>
      </div>
    </div>
  </div>

  <script>
    const pinInput = document.getElementById("pin");

    const formUpdate = document.getElementById("form-update");
    const formReset  = document.getElementById("form-reset");
    const formBase   = document.getElementById("form-base");

    const btnStatus = document.getElementById("btn-status");
    const btnCancel = document.getElementById("btn-cancel");

    const baseQ         = document.getElementById("base-q");
    const btnLoadBase   = document.getElementById("btn-load-base");
    const btnClearTable = document.getElementById("btn-clear-table");
    const baseTableDiv  = document.getElementById("base-table");

    function getPinOrAlert() {
      const pin = pinInput.value.trim();
      if (!pin) {
        alert("Ingresá el PIN de administrador");
        return null;
      }
      return pin;
    }

    // ---------- update precios ----------
    formUpdate.addEventListener("submit", async (e) => {
      e.preventDefault();
      const pin = getPinOrAlert();
      if (!pin) return;

      const form = new FormData(formUpdate);
      form.append("pin", pin);
      const params = new URLSearchParams(form);
      const url = "/start?" + params.toString();

      try {
        const r = await fetch(url);
        const txt = await r.text();
        let j;
        try { j = JSON.parse(txt); } catch (_) { j = { message: txt }; }
        alert(j.message || JSON.stringify(j));
      } catch (err) {
        alert("Error llamando al worker: " + err);
      }
    });

    // ---------- reset base_usd ----------
    formReset.addEventListener("submit", async (e) => {
      e.preventDefault();
      const pin = getPinOrAlert();
      if (!pin) return;

      const form = new FormData(formReset);
      form.append("pin", pin);
      // margin fijo 1.00, round 0 (no se usan para reset)
      form.append("margin", "1.00");
      form.append("round", "0");
      const params = new URLSearchParams(form);
      const url = "/reset-base?" + params.toString();
      try {
        const r = await fetch(url);
        const txt = await r.text();
        let j;
        try { j = JSON.parse(txt); } catch (_) { j = { message: txt }; }
        alert(j.message || JSON.stringify(j));
      } catch (err) {
        alert("Error llamando al worker: " + err);
      }
    });

    // ---------- set base_usd manual (1 variante) ----------
    formBase.addEventListener("submit", async (e) => {
      e.preventDefault();
      const pin = getPinOrAlert();
      if (!pin) return;

      const form = new FormData(formBase);
      form.append("pin", pin);
      const params = new URLSearchParams(form);
      const url = "/set-base-usd?" + params.toString();
      try {
        const r = await fetch(url);
        const txt = await r.text();
        let j;
        try { j = JSON.parse(txt); } catch (_) { j = { message: txt }; }
        alert(j.message || JSON.stringify(j));
      } catch (err) {
        alert("Error llamando al worker: " + err);
      }
    });

    // ---------- status y cancel ----------
    btnStatus.addEventListener("click", async () => {
      try {
        const r = await fetch("/status");
        const txt = await r.text();
        alert(txt);
      } catch (err) {
        alert("Error consultando status: " + err);
      }
    });

    btnCancel.addEventListener("click", async () => {
      const pin = getPinOrAlert();
      if (!pin) return;
      try {
        const r = await fetch("/cancel?pin=" + encodeURIComponent(pin));
        const txt = await r.text();
        let j;
        try { j = JSON.parse(txt); } catch (_) { j = { message: txt }; }
        alert(j.message || JSON.stringify(j));
      } catch (err) {
        alert("Error cancelando job: " + err);
      }
    });

    // ---------- Lista de productos ----------
    btnClearTable.addEventListener("click", () => {
      baseTableDiv.innerHTML = "";
    });

    btnLoadBase.addEventListener("click", async () => {
      const pin = getPinOrAlert();
      if (!pin) return;
      const q = (baseQ.value || "").trim();

      const params = new URLSearchParams();
      params.set("pin", pin);
      if (q) params.set("q", q);
      params.set("limit", "50"); // 50 productos por búsqueda

      try {
        const r = await fetch("/base-list?" + params.toString());
        const txt = await r.text();
        let j;
        try { j = JSON.parse(txt); } catch (_) {
          alert("Respuesta no válida de /base-list");
          console.log(txt);
          return;
        }
        if (!j.ok) {
          alert(j.message || "Error en /base-list");
          return;
        }
        renderBaseTable(j.rows || []);
      } catch (err) {
        alert("Error llamando a /base-list: " + err);
      }
    });

    function renderBaseTable(rows) {
      if (!rows.length) {
        baseTableDiv.innerHTML =
          "<div style='padding:10px;font-size:13px;color:#777;'>No se encontraron variantes para esta búsqueda.</div>";
        return;
      }

      let html = "<table>";
      html += "<thead><tr>";
      html += "<th>Producto</th>";
      html += "<th>SKU</th>";
      html += "<th class='numeric'>Precio PYG</th>";
      html += "<th class='numeric'>Base USD</th>";
      html += "<th class='numeric'>Tasa estimada</th>";
      html += "<th style='text-align:center;'>Acción</th>";
      html += "</tr></thead><tbody>";

      for (const row of rows) {
        const vid = row.variantId;
        const sku = row.sku || "";
        const title = row.productTitle || "";
        const price = (row.pricePyg != null && !isNaN(row.pricePyg))
          ? Math.round(row.pricePyg)
          : null;
        const base = (row.baseUsd != null && !isNaN(row.baseUsd))
          ? Number(row.baseUsd)
          : null;
        const tasa = (price != null && base != null && base > 0)
          ? (price / base)
          : null;

        const baseStr = base != null ? base.toFixed(2) : "";
        const tasaStr = tasa != null ? tasa.toFixed(2) : "";

        const tasaClass =
          tasa != null && (tasa < 5000 || tasa > 10000)
            ? "badge badge-danger"
            : "badge";

        html += "<tr>";
        html += "<td>" + escapeHtml(title) +
                "<br><small style='color:#999;'>Var ID: " + vid + "</small></td>";
        html += "<td>" + escapeHtml(sku) + "</td>";
        html += "<td class='numeric'>" +
                (price != null ? price.toLocaleString('es-PY') : "") +
                "</td>";
        html += "<td class='numeric'>";
        html += "<input type='number' step='0.01' " +
                "style='width:100%;padding:4px 6px;font-size:12px;border-radius:4px;border:1px solid #cfd3dd;' " +
                "value='" + baseStr + "' data-variant-id='" + vid + "' class='base-input-row'>";
        html += "</td>";
        html += "<td class='numeric'>" +
                (tasaStr ? "<span class='" + tasaClass + "'>" + tasaStr + "</span>" : "") +
                "</td>";
        html += "<td style='text-align:center;'>";
        html += "<button type='button' class='secondary btn-save-row' " +
                "data-variant-id='" + vid + "' " +
                "style='font-size:12px;padding:4px 10px;margin-top:0;'>Guardar</button>";
        html += "</td>";
        html += "</tr>";
      }

      html += "</tbody></table>";
      baseTableDiv.innerHTML = html;

      const buttons = baseTableDiv.querySelectorAll(".btn-save-row");
      buttons.forEach((btn) => {
        btn.addEventListener("click", async () => {
          const pin = getPinOrAlert();
          if (!pin) return;
          const vid = btn.getAttribute("data-variant-id");
          const input = baseTableDiv.querySelector(
            "input.base-input-row[data-variant-id='" + vid + "']"
          );
          if (!input) {
            alert("No se encontró el input para esa fila");
            return;
          }
          const val = input.value.trim();
          if (!val) {
            alert("Ingresá un Base USD válido");
            return;
          }

          const params = new URLSearchParams();
          params.set("pin", pin);
          params.set("variantId", vid);
          params.set("baseUsd", val);

          try {
            const r = await fetch("/set-base-usd?" + params.toString());
            const txt = await r.text();
            let j;
            try { j = JSON.parse(txt); } catch (_) { j = { message: txt }; }
            alert(j.message || JSON.stringify(j));
          } catch (err) {
            alert("Error guardando Base USD: " + err);
          }
        });
      });
    }

    function escapeHtml(str) {
      if (!str) return "";
      return String(str)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }
  </script>
</body>
</html>`;
      return new Response(html, {
        headers: { "Content-Type": "text/html; charset=utf-8" },
      });
    }

    // ---------- START protegido con PIN (modo update precios) ----------
    if (path === "/start" && (req.method === "POST" || req.method === "GET")) {
      const pinProvided =
        url.searchParams.get("pin") || req.headers.get("X-Admin-Pin") || "";
      const validPin = env.ADMIN_PIN;

      if (!validPin)
        return cors(text("Falta ADMIN_PIN en variables de entorno", 500));
      if (!pinProvided || pinProvided !== validPin)
        return cors(text("PIN inválido", 403));

      const existingJob = await getJob(env);
      if (existingJob?.running) {
        return cors(
          json(
            {
              ok: false,
              message:
                "Ya hay un job en curso. Usá /status o /cancel antes de iniciar otro.",
              job: existingJob,
            },
            400
          )
        );
      }

      const rate =
        norm(url.searchParams.get("rate")) || norm(env.MANUAL_RATE);
      const margin =
        norm(url.searchParams.get("margin")) ||
        norm(env.MARGIN_FACTOR) ||
        1.0;
      const roundTo =
        parseInt(
          url.searchParams.get("round") || env.ROUND_TO || "0",
          10
        ) || 0;
      const totalVariantsHint =
        url.searchParams.get("total_variants") ||
        env.TOTAL_VARIANTS_HINT ||
        null;
      const cronMin =
        url.searchParams.get("cron_min") || env.CRON_MINUTES || null;

      if (!rate || rate <= 0)
        return cors(
          text("Debe indicar ?rate= o configurar MANUAL_RATE", 400)
        );

      const job = {
        mode: "update",
        running: true,
        startedAt: new Date().toISOString(),
        pageInfo: null,
        processedProducts: 0,
        processedVariants: 0,
        updatedVariants: 0,
        seededVariants: 0,
        rate,
        margin,
        roundTo,
        totalVariantsHint,
        cronMin,
        lastRunAt: null,
        lastMsg: "started (update)",
      };
      await saveJob(env, job);
      return cors(
        json({ ok: true, message: "Job de actualización iniciado", job })
      );
    }

    // ---------- RESET BASE_USD protegido con PIN (no cambia precios) ----------
    if (
      path === "/reset-base" &&
      (req.method === "POST" || req.method === "GET")
    ) {
      const pinProvided =
        url.searchParams.get("pin") || req.headers.get("X-Admin-Pin") || "";
      const validPin = env.ADMIN_PIN;
      if (!validPin)
        return cors(text("Falta ADMIN_PIN en variables de entorno", 500));
      if (!pinProvided || pinProvided !== validPin)
        return cors(text("PIN inválido", 403));

      const existingJob = await getJob(env);
      if (existingJob?.running) {
        return cors(
          json(
            {
              ok: false,
              message:
                "Ya hay un job en curso. Usá /status o /cancel antes de iniciar otro.",
              job: existingJob,
            },
            400
          )
        );
      }

      const rate =
        norm(url.searchParams.get("rate")) || norm(env.MANUAL_RATE);
      const totalVariantsHint =
        url.searchParams.get("total_variants") ||
        env.TOTAL_VARIANTS_HINT ||
        null;
      const cronMin =
        url.searchParams.get("cron_min") || env.CRON_MINUTES || null;

      if (!rate || rate <= 0)
        return cors(
          text("Debe indicar ?rate= o configurar MANUAL_RATE", 400)
        );

      const job = {
        mode: "reset_base",
        running: true,
        startedAt: new Date().toISOString(),
        pageInfo: null,
        processedProducts: 0,
        processedVariants: 0,
        updatedVariants: 0,
        seededVariants: 0,
        rate,
        margin: 1.0,
        roundTo: 0,
        totalVariantsHint,
        cronMin,
        lastRunAt: null,
        lastMsg: "started (reset_base)",
      };
      await saveJob(env, job);
      return cors(
        json({ ok: true, message: "Job de reset base_usd iniciado", job })
      );
    }

    // ---------- SET BASE_USD manual (1 variante) ----------
    if (
      path === "/set-base-usd" &&
      (req.method === "POST" || req.method === "GET")
    ) {
      const pinProvided =
        url.searchParams.get("pin") || req.headers.get("X-Admin-Pin") || "";
      const validPin = env.ADMIN_PIN;
      if (!validPin)
        return cors(text("Falta ADMIN_PIN en variables de entorno", 500));
      if (!pinProvided || pinProvided !== validPin)
        return cors(text("PIN inválido", 403));

      const variantId = url.searchParams.get("variantId");
      const baseUsdRaw = url.searchParams.get("baseUsd");
      const baseUsd = norm(baseUsdRaw);

      if (!variantId) return cors(text("Falta variantId", 400));
      if (!baseUsd || baseUsd <= 0)
        return cors(text("Base USD inválido", 400));

      const shop = getShop(env);
      const ok = await upsertBaseUSD_GQL(shop, variantId, baseUsd);
      if (ok) {
        await addLog(env, {
          product: "(manual)",
          variantId,
          price_before: null,
          price_after: null,
          status: "base_usd_manual_set",
          baseUsd,
          time: new Date().toLocaleString("es-PY", {
            timeZone: STATUS_TIMEZONE,
          }),
        });
        return cors(
          json({
            ok: true,
            message: "Base USD guardado para la variante " + variantId,
          })
        );
      } else {
        return cors(
          json({ ok: false, message: "Error al guardar Base USD" }, 500)
        );
      }
    }

    // ---------- CANCEL protegido con PIN ----------
    if (
      path === "/cancel" &&
      (req.method === "POST" || req.method === "GET")
    ) {
      const pinProvided =
        url.searchParams.get("pin") || req.headers.get("X-Admin-Pin") || "";
      const validPin = env.ADMIN_PIN;
      if (!validPin)
        return cors(text("Falta ADMIN_PIN en variables de entorno", 500));
      if (!pinProvided || pinProvided !== validPin)
        return cors(text("PIN inválido", 403));

      await env.JOBS_KV.delete(JOB_KEY);
      return cors(json({ ok: true, message: "Job cancelado" }));
    }

    // ---------- LOG: ver historial completo ----------
    if (path === "/log") {
      const pinProvided = url.searchParams.get("pin") || "";
      const validPin = env.ADMIN_PIN;
      if (!validPin)
        return cors(text("Falta ADMIN_PIN en variables de entorno", 500));
      if (!pinProvided || pinProvided !== validPin)
        return cors(text("PIN inválido", 403));

      const raw = await env.LOG_KV.get(LOG_KEY);
      return cors(json(raw ? JSON.parse(raw) : []));
    }

    // ---------- LOG: limpiar ----------
    if (path === "/log/clear") {
      const pinProvided = url.searchParams.get("pin") || "";
      const validPin = env.ADMIN_PIN;
      if (!validPin)
        return cors(text("Falta ADMIN_PIN en variables de entorno", 500));
      if (!pinProvided || pinProvided !== validPin)
        return cors(text("PIN inválido", 403));

      await env.LOG_KV.delete(LOG_KEY);
      return cors(json({ ok: true, message: "Log limpiado" }));
    }

    // ---------- BASE-LIST: lista tipo “Lista de productos” ----------
    if (path === "/base-list" && req.method === "GET") {
      const pinProvided = url.searchParams.get("pin") || "";
      const validPin = env.ADMIN_PIN;
      if (!validPin)
        return cors(text("Falta ADMIN_PIN en variables de entorno", 500));
      if (!pinProvided || pinProvided !== validPin)
        return cors(text("PIN inválido", 403));

      const searchQ = (url.searchParams.get("q") || "").trim() || null;
      const pageSize =
        parseInt(url.searchParams.get("limit") || "50", 10) || 50;

      const shop = getShop(env);
      const page = await fetchBaseListPage(shop, {
        query: searchQ,
        pageSize,
      });

      return cors(
        json({
          ok: true,
          rows: page.rows,
        })
      );
    }

    // Mensaje por defecto
    return cors(
      text(
        "Usos: /admin, /start, /reset-base, /set-base-usd, /status, /cancel, /log, /log/clear, /base-list"
      )
    );
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(runBatch(env));
  },
};

// ============ PROCESAMIENTO POR LOTES ============

async function runBatch(env) {
  const shop = getShop(env);
  let job = await getJob(env);
  if (!job?.running) return;

  const mode = job.mode || "update";

  try {
    const pageSize = 25;
    const { products, nextPageInfo } = await fetchProducts(shop, {
      pageSize,
      pageInfo: job.pageInfo,
      onlyActive: true,
    });

    if (!products.length) {
      job.running = false;
      job.lastMsg = "Completado (fin de paginación)";
      job.lastRunAt = new Date().toISOString();
      await saveJob(env, job);
      return;
    }

    let variantsDone = 0;

    for (const p of products) {
      job.processedProducts++;
      for (const v of p.variants || []) {
        if (variantsDone >= BATCH_LIMIT) break;

        const variantId = v.id;
        const pricePYG = norm(v.price);
        if (!pricePYG || pricePYG <= 0) {
          await sleep(REQ_THROTTLE_MS);
          continue;
        }

        job.processedVariants++;

        // MODO RESET_BASE: recalcular y sobreescribir base_usd sin tocar precio
        if (mode === "reset_base") {
          const baseCalculated =
            Math.round((pricePYG / job.rate) * 100) / 100;
          let status = "skipped";
          if (Number.isFinite(baseCalculated) && baseCalculated > 0) {
            const ok = await upsertBaseUSD_GQL(
              shop,
              variantId,
              baseCalculated
            );
            if (ok) {
              job.seededVariants++;
              status = "base_usd_reset";
            }
          }

          await addLog(env, {
            product: p.title || "No title",
            variantId,
            price_before: Math.round(pricePYG),
            price_after: Math.round(pricePYG),
            status,
            time: new Date().toLocaleString("es-PY", {
              timeZone: STATUS_TIMEZONE,
            }),
          });

          variantsDone++;
          await sleep(REQ_THROTTLE_MS);
          if (variantsDone >= BATCH_LIMIT) break;
          continue;
        }

        // MODO UPDATE PRECIOS (normal)
        const { baseUsd, seeded } = await getOrSeedBaseUSD(
          shop,
          variantId,
          pricePYG,
          job.rate
        );
        if (!baseUsd || baseUsd <= 0) {
          await sleep(REQ_THROTTLE_MS);
          continue;
        }
        if (seeded) job.seededVariants++;

        let newPyg = baseUsd * job.rate * job.margin;
        newPyg = roundTo(newPyg, job.roundTo);

        let status = "skipped";
        if (Math.abs(newPyg - pricePYG) >= 1) {
          const ok = await updateVariantPrice(shop, variantId, newPyg);
          if (ok) {
            job.updatedVariants++;
            status = "updated";
          }
        } else if (seeded) {
          status = "seeded";
        }

        await addLog(env, {
          product: p.title || "No title",
          variantId,
          price_before: Math.round(pricePYG),
          price_after: Math.round(newPyg),
          status,
          time: new Date().toLocaleString("es-PY", {
            timeZone: STATUS_TIMEZONE,
          }),
        });

        variantsDone++;
        await sleep(REQ_THROTTLE_MS);
        if (variantsDone >= BATCH_LIMIT) break;
      }
      if (variantsDone >= BATCH_LIMIT) break;
    }

    job.pageInfo = nextPageInfo || null;
    job.lastRunAt = new Date().toISOString();
    job.lastMsg = !nextPageInfo
      ? "Completado (fin de paginación)"
      : `Procesados: ${variantsDone}`;
    if (!nextPageInfo && variantsDone < BATCH_LIMIT) job.running = false;

    await saveJob(env, job);
  } catch (e) {
    if (String(e.message || "").includes("GET products → 400")) {
      job.pageInfo = null;
      job.running = false;
      job.lastMsg = "Completado (fin de paginación)";
    } else {
      job.lastMsg = "ERROR: " + (e?.message || String(e));
    }
    job.lastRunAt = new Date().toISOString();
    await saveJob(env, job);
  }
}

// ============ base_usd: lectura y siembra segura (modo update) ============

async function getOrSeedBaseUSD(shop, variantId, currentPricePyg, rate) {
  const baseFromMeta = await readBaseUSD_GQL(shop, variantId);

  if (baseFromMeta && baseFromMeta > 0) {
    return { baseUsd: baseFromMeta, seeded: false };
  }

  if (!rate || rate <= 0) {
    return { baseUsd: null, seeded: false };
  }

  const baseCalculated =
    Math.round((currentPricePyg / rate) * 100) / 100;
  if (!Number.isFinite(baseCalculated) || baseCalculated <= 0) {
    return { baseUsd: null, seeded: false };
  }

  const seededOk = await upsertBaseUSD_GQL(
    shop,
    variantId,
    baseCalculated
  );
  return {
    baseUsd: seededOk ? baseCalculated : null,
    seeded: !!seededOk,
  };
}

async function readBaseUSD_GQL(shop, variantId) {
  const { domain, token } = shop;
  const endpoint = `https://${domain}/admin/api/${API_VERSION}/graphql.json`;

  const query = `
    query variantBaseUsd($id: ID!) {
      productVariant(id: $id) {
        id
        metafield(namespace: "pricing", key: "base_usd") {
          value
        }
      }
    }
  `;

  const variables = {
    id: `gid://shopify/ProductVariant/${variantId}`,
  };

  const r = await fetch(endpoint, {
    method: "POST",
    headers: {
      "X-Shopify-Access-Token": token,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query, variables }),
  });

  if (!r.ok) return null;

  const data = await r.json();
  const val = data?.data?.productVariant?.metafield?.value;
  if (!val) return null;

  const num = parseFloat(val);
  return Number.isFinite(num) ? num : null;
}

// ============ GraphQL Upsert (1 request por variante) ============

async function upsertBaseUSD_GQL(shop, variantId, baseUSD) {
  const { domain, token } = shop;
  const endpoint = `https://${domain}/admin/api/${API_VERSION}/graphql.json`;

  const query = `
    mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields { id key namespace type value ownerType }
        userErrors { field message }
      }
    }
  `;

  const variables = {
    metafields: [
      {
        ownerId: `gid://shopify/ProductVariant/${variantId}`,
        namespace: "pricing",
        key: "base_usd",
        type: "number_decimal",
        value: String(Number(baseUSD).toFixed(2)),
      },
    ],
  };

  const r = await fetch(endpoint, {
    method: "POST",
    headers: {
      "X-Shopify-Access-Token": token,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query, variables }),
  });

  if (!r.ok) return false;
  const data = await r.json();
  const errs = data?.data?.metafieldsSet?.userErrors;
  return !errs || errs.length === 0;
}

// ============ Actualización de precio (REST) ============

async function updateVariantPrice(shop, variantId, pyg) {
  const { domain, token } = shop;
  const url = `https://${domain}/admin/api/${API_VERSION}/variants/${variantId}.json`;
  const r = await fetch(url, {
    method: "PUT",
    headers: {
      "X-Shopify-Access-Token": token,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      variant: { id: variantId, price: Math.round(pyg) },
    }),
  });
  return r.ok;
}

// ============ Helpers Shopify ============

function getShop(env) {
  const domain = env.SHOPIFY_STORE_DOMAIN;
  const token = env.SHOPIFY_ADMIN_TOKEN;
  if (!domain || !token)
    throw new Error("Falta SHOPIFY_STORE_DOMAIN o SHOPIFY_ADMIN_TOKEN");
  return { domain, token };
}

// PATCH 400-safe paginación REST
async function fetchProducts(shop, { pageSize, pageInfo, onlyActive }) {
  const { domain, token } = shop;

  const qs = new URLSearchParams();
  qs.set("limit", String(pageSize));

  if (pageInfo) {
    qs.set("page_info", pageInfo);
  } else {
    if (onlyActive) qs.set("status", "active");
    qs.set("fields", "id,title,variants");
  }

  const url = `https://${domain}/admin/api/${API_VERSION}/products.json?${qs.toString()}`;

  const r = await fetch(url, {
    headers: {
      "X-Shopify-Access-Token": token,
      "Content-Type": "application/json",
    },
  });

  if (!r.ok) {
    if (r.status === 400 && pageInfo) {
      return { products: [], nextPageInfo: null };
    }
    throw new Error(`GET products → ${r.status}`);
  }

  const data = await r.json();
  const link = r.headers.get("Link") || "";
  const nextPageInfo = extractPageInfo(link);

  return { products: data?.products || [], nextPageInfo };
}

// ============ Listado de productos (para Lista de productos) ============

async function fetchBaseListPage(shop, { query, pageSize }) {
  const { domain, token } = shop;
  const endpoint = `https://${domain}/admin/api/${API_VERSION}/graphql.json`;

  const gql = `
    query productVariantBaseList($first: Int!, $query: String) {
      products(first: $first, query: $query, sortKey: TITLE) {
        edges {
          node {
            id
            title
            variants(first: 20) {
              edges {
                node {
                  id
                  sku
                  price
                  metafield(namespace: "pricing", key: "base_usd") {
                    value
                  }
                }
              }
            }
          }
        }
      }
    }
  `;

  const variables = {
    first: pageSize,
    query: query || null,
  };

  const r = await fetch(endpoint, {
    method: "POST",
    headers: {
      "X-Shopify-Access-Token": token,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query: gql, variables }),
  });

  if (!r.ok) throw new Error(`GraphQL base-list → ${r.status}`);

  const data = await r.json();
  const edges = data?.data?.products?.edges || [];

  const rows = [];

  for (const e of edges) {
    const pNode = e.node;
    const title = pNode?.title || "Sin título";
    const variantsEdges = pNode?.variants?.edges || [];
    for (const ve of variantsEdges) {
      const vNode = ve.node;
      if (!vNode) continue;
      const gid = vNode.id || "";
      const variantId = gid.split("/").pop(); // ID numérico para REST
      const sku = vNode.sku || "";
      const price = vNode.price != null ? Number(vNode.price) : null;
      const mf = vNode.metafield;
      const baseUsdVal =
        mf?.value != null ? Number(mf.value) : null;

      rows.push({
        productTitle: title,
        variantId,
        sku,
        pricePyg: price,
        baseUsd: baseUsdVal,
      });
    }
  }

  return { rows };
}

// ============ KV, ETA y LOG ============

async function getJob(env) {
  const raw = await env.JOBS_KV.get(JOB_KEY);
  return raw ? JSON.parse(raw) : null;
}

async function saveJob(env, job) {
  await env.JOBS_KV.put(JOB_KEY, JSON.stringify(job));
}

function computeETA(job) {
  const total = parseInt(job.totalVariantsHint || 0, 10);
  const cron = parseInt(job.cronMin || 2, 10);
  const batch = BATCH_LIMIT;
  if (!total) return { etaMinutes: null };
  const processed = parseInt(job.processedVariants || 0, 10);
  const remaining = Math.max(0, total - processed);
  const batches = Math.ceil(remaining / batch);
  return {
    etaMinutes: batches * cron,
    batchesRemaining: batches,
    variantsRemaining: remaining,
    hintTotalVariants: total,
    cronMinutes: cron,
    batchLimit: batch,
  };
}

async function addLog(env, entry) {
  const raw = await env.LOG_KV.get(LOG_KEY);
  let arr = [];
  if (raw) {
    try {
      arr = JSON.parse(raw);
    } catch (e) {
      arr = [];
    }
  }
  arr.push(entry);

  if (arr.length > 2000) arr.shift();

  await env.LOG_KV.put(LOG_KEY, JSON.stringify(arr));
}

// ============ Utilidades ============

function extractPageInfo(linkHeader) {
  const m = /<[^>]*page_info=([^&>]+)[^>]*>;\s*rel="next"/i.exec(
    linkHeader || ""
  );
  return m ? m[1] : null;
}

function cors(res) {
  res.headers.set("Access-Control-Allow-Origin", "*");
  res.headers.set("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.headers.set(
    "Access-Control-Allow-Headers",
    "Content-Type, Authorization"
  );
  return res;
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

function text(s, status = 200) {
  return new Response(String(s), {
    status,
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  });
}

function norm(s) {
  const t = String(s || "").trim();
  if (!t) return NaN;

  // 1.234.567,89
  if (/^\d{1,3}(\.\d{3})+(,\d+)?$/.test(t)) {
    return parseFloat(t.replace(/\./g, "").replace(",", "."));
  }
  // 1,234,567.89
  if (/^\d{1,3}(,\d{3})+(\.\d+)?$/.test(t)) {
    return parseFloat(t.replace(/,/g, ""));
  }
  // 1234,56
  if (t.includes(",") && !t.includes(".")) {
    return parseFloat(t.replace(",", "."));
  }
  return parseFloat(t);
}

function roundTo(n, step) {
  if (!step || step <= 1) return Math.round(n);
  return Math.round(n / step) * step;
}

async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
