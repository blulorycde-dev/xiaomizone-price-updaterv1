// Worker: price-updater con panel /admin (tema claro), modos update/reset,
// log, lista de productos y set-base-usd con actualización de precio

const API_VERSION = "2024-10";
const JOB_KEY = "price_job";
const LOG_KEY = "price_log";
const BATCH_LIMIT = 10;       // variantes por corrida del cron
const REQ_THROTTLE_MS = 600;  // espera entre peticiones
const STATUS_TIMEZONE = "America/Asuncion";

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    const path = url.pathname.replace(/\/+$/, "") || "/";

    if (req.method === "OPTIONS") {
      return cors(new Response(null));
    }

    // ---------- STATUS ----------

    if (path === "/status") {
      const job = await getJob(env);
      const body = job || { running: false, msg: "no job" };

      if (job?.running || typeof job?.processedVariants === "number") {
        const eta = computeETA(job);
        body.eta = eta;

        const total = parseInt(job?.totalVariantsHint || 0, 10) || 0;
        const processed = parseInt(job?.processedVariants || 0, 10) || 0;
        if (total > 0) {
          body.progressPercent = Math.min(
            100,
            Math.round((processed / total) * 100)
          );
        }

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
      // Leer las variables reales del Worker para sincronizar el panel
      const rateEnv = norm(env.MANUAL_RATE);
      const marginEnv = norm(env.MARGIN_FACTOR);
      const roundEnv = parseInt(env.ROUND_TO || "100", 10) || 100;

      const rateForJs = rateEnv && rateEnv > 0 ? rateEnv : 7200;
      const marginForJs = marginEnv && marginEnv > 0 ? marginEnv : 1.25;
      const roundForJs = roundEnv;

      const html = `
<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <title>PANEL GENERAL DE PRECIOS</title>
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

    input:focus, select:focus {
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

    /* Botones secundarios (Ver productos, Limpiar tabla, Guardar, etc.) */
    button.secondary {
      background:#63B8EE;
      color:#fff;
    }
    button.secondary:hover {
      background:#3da4e8;
    }

    button[disabled] {
      opacity: 0.5;
      cursor: default;
    }

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

    /* Encabezados centrados */
    th {
      text-align: center;
      background:#f5f6fa;
      font-weight:600;
    }

    /* Las dos primeras columnas siguen alineadas a la izquierda */
    td:first-child,
    td:nth-child(2) {
      text-align:left;
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
      background:#009929; /* verde principal */
    }

    .pill-dot.red { background:#e74c3c; }

    .toolbar {
      display:flex;
      align-items:center;
      gap:8px;
      margin-top:8px;
      flex-wrap: wrap;
    }

    .toolbar input {
      max-width:260px;
    }

    .toolbar select {
      max-width:200px;
    }

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

    .toggle-modified {
      display:flex;
      align-items:center;
      gap:4px;
      font-size:12px;
      color:#555;
    }

    .toggle-modified input {
      width:auto;
      margin-top:0;
    }

    /* Filas modificadas (verde suave) */
    tr.row-modified {
      background: rgba(92, 203, 95, 0.18);
    }

    /* Input de Base USD modificado (borde verde principal + fondo suave) */
    input.base-input-row.modified {
      border-color:#009929;
      background: rgba(92, 203, 95, 0.28);
    }
  </style>
</head>
<body>
  <div class="box">
    <h1>PANEL GENERAL DE PRECIOS</h1>
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
        <h3>Ver productos</h3>
        <span class="tag">Vista tipo editor masivo</span>
      </div>
      <p class="hint">
        Ver y editar productos en orden alfabético. Usá <b>"Ver productos"</b> para cargar la lista y <b>"Ver más productos"</b> para seguir cargando.
        El buscador y el filtro de estado son opcionales.
      </p>

      <div class="toolbar">
        <input
          type="text"
          id="base-q"
          placeholder="Buscar (opcional) por nombre, SKU, etc."
        >
        <select id="status-filter">
          <option value="">Todos los estados</option>
          <option value="active">Solo activos</option>
          <option value="draft">Solo borradores</option>
          <option value="archived">Solo archivados</option>
        </select>
        <button type="button" id="btn-load-base" class="secondary">
          Ver productos
        </button>
        <button type="button" id="btn-clear-table" class="secondary">
          Limpiar tabla
        </button>
        <label class="toggle-modified">
          <input type="checkbox" id="toggle-modified">
          Mostrar solo modificados
        </label>
      </div>

      <div class="table-wrap" id="base-table"></div>

      <div style="display:flex; justify-content:flex-end; margin-top:8px;">
        <button type="button" id="btn-more-base" class="secondary" disabled>
          Ver más productos
        </button>
      </div>

      <div class="footer-note">
        Tip: con el buscador vacío y filtro en "Todos", <b>Ver productos</b> recorre el catálogo completo página por página.
        Podés combinar búsqueda + estado para revisar solo ciertas líneas o campañas.
      </div>
    </div>

    <div class="section">
      <h3>Control rápido del job</h3>
      <div class="row">
        <button id="btn-status" type="button" class="secondary">Ver Estado</button>
        <button id="btn-cancel" type="button" class="secondary">Cancelar Actualización</button>
      </div>
    </div>

    <div class="section">
      <div class="section-header">
        <h3>Historial reciente de cambios</h3>
        <span class="tag">Vista solo lectura</span>
      </div>
      <p class="hint">
        Consultá las últimas modificaciones de <b>base_usd</b> y precios que registró el worker.
      </p>

      <div class="toolbar">
        <select id="log-range">
          <option value="24">Últimas 24 horas</option>
          <option value="168">Últimos 7 días</option>
          <option value="0">Todo el historial</option>
        </select>
        <button type="button" id="btn-load-log" class="secondary">
          Ver historial
        </button>
      </div>

      <div id="log-list" style="margin-top:8px;font-size:12px;"></div>
    </div>
  </div>

  <script>
    // Configuración sincronizada con las variables del Worker
    const FIXED_RATE   = ${rateForJs};
    const FIXED_MARGIN = ${marginForJs};
    const ROUND_STEP   = ${roundForJs};

    const pinInput   = document.getElementById("pin");
    const formUpdate = document.getElementById("form-update");
    const formReset  = document.getElementById("form-reset");
    const formBase   = document.getElementById("form-base");
    const btnStatus  = document.getElementById("btn-status");
    const btnCancel  = document.getElementById("btn-cancel");

    const baseQ          = document.getElementById("base-q");
    const statusFilter   = document.getElementById("status-filter");
    const btnLoadBase    = document.getElementById("btn-load-base");
    const btnMoreBase    = document.getElementById("btn-more-base");
    const btnClearTable  = document.getElementById("btn-clear-table");
    const baseTableDiv   = document.getElementById("base-table");
    const toggleModified = document.getElementById("toggle-modified");

    const logRange   = document.getElementById("log-range");
    const btnLoadLog = document.getElementById("btn-load-log");
    const logList    = document.getElementById("log-list");

    // Prellenar formulario de actualización con valores de entorno
    if (formUpdate) {
      const rateInput   = formUpdate.querySelector('input[name="rate"]');
      const marginInput = formUpdate.querySelector('input[name="margin"]');
      const roundInput  = formUpdate.querySelector('input[name="round"]');
      if (rateInput)   rateInput.value   = FIXED_RATE;
      if (marginInput) marginInput.value = FIXED_MARGIN.toFixed(2);
      if (roundInput)  roundInput.value  = ROUND_STEP;
    }

    function getPinOrAlert() {
      const pin = pinInput ? pinInput.value.trim() : "";
      if (!pin) {
        alert("Ingresá el PIN de administrador");
        return null;
      }
      return pin;
    }

    function roundToClient(n, step) {
      if (!step || step <= 1) return Math.round(n);
      return Math.round(n / step) * step;
    }

    // --------- UPDATE ----------

    if (formUpdate) {
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
    }

    // --------- RESET ----------

    if (formReset) {
      formReset.addEventListener("submit", async (e) => {
        e.preventDefault();
        const pin = getPinOrAlert();
        if (!pin) return;

        const form = new FormData(formReset);
        form.append("pin", pin);
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
    }

    // --------- SET BASE (1 variante manual) ----------

    if (formBase) {
      formBase.addEventListener("submit", async (e) => {
        e.preventDefault();
        const pin = getPinOrAlert();
        if (!pin) return;

        const form = new FormData(formBase);
        form.append("pin", pin);
        form.append("applyRate", "1");
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
    }

    // --------- STATUS / CANCEL ----------

    if (btnStatus) {
      btnStatus.addEventListener("click", async () => {
        try {
          const r = await fetch("/status");
          const txt = await r.text();
          alert(txt);
        } catch (err) {
          alert("Error consultando status: " + err);
        }
      });
    }

    if (btnCancel) {
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
    }

    // --------- LISTA DE PRODUCTOS (paginada) ----------

    let baseCursor = null;
    let baseRows = [];

    if (btnClearTable) {
      btnClearTable.addEventListener("click", () => {
        baseRows = [];
        baseCursor = null;
        baseTableDiv.innerHTML = "";
        if (btnMoreBase) btnMoreBase.disabled = true;
      });
    }

    if (btnLoadBase) {
      btnLoadBase.addEventListener("click", () => {
        baseRows = [];
        baseCursor = null;
        loadBase(true);
      });
    }

    if (btnMoreBase) {
      btnMoreBase.addEventListener("click", () => {
        if (!baseCursor) {
          alert("No hay más productos para cargar.");
          return;
        }
        loadBase(false);
      });
    }

    if (toggleModified) {
      toggleModified.addEventListener("change", () => {
        applyModifiedFilter();
      });
    }

    async function loadBase(reset) {
      const pin = getPinOrAlert();
      if (!pin) return;

      const q = (baseQ && baseQ.value || "").trim();
      const status = statusFilter ? (statusFilter.value || "") : "";

      const params = new URLSearchParams();
      params.set("pin", pin);
      params.set("limit", "50");
      if (q) params.set("q", q);
      if (status) params.set("status", status);
      if (!reset && baseCursor) params.set("cursor", baseCursor);

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

        const rows = j.rows || [];
        if (reset) {
          baseRows = rows;
        } else {
          baseRows = baseRows.concat(rows);
        }
        baseCursor = j.nextCursor || null;
        renderBaseTable(baseRows);

        if (btnMoreBase) btnMoreBase.disabled = !baseCursor;
      } catch (err) {
        alert("Error llamando a /base-list: " + err);
      }
    }

    function renderBaseTable(rows) {
      if (!baseTableDiv) return;

      if (!rows.length) {
        baseTableDiv.innerHTML = "<div style='padding:10px;font-size:13px;color:#777;'>No se encontraron variantes para esta búsqueda.</div>";
        return;
      }

      let html = "<table>";
      html += "<thead><tr>";
      html += "<th>Producto</th>";
      html += "<th>SKU</th>";
      html += "<th class='numeric'>Precio PYG base</th>";
      html += "<th class='numeric'>Precio PYG con recargo</th>";
      html += "<th class='numeric'>Base USD</th>";
      html += "<th class='numeric'>Tasa estimada</th>";
      html += "<th style='text-align:center;'>Acción</th>";
      html += "</tr></thead><tbody>";

      for (const row of rows) {
        const vid   = row.variantId;
        const sku   = row.sku || "";
        const title = row.productTitle || "";

        const priceShop = (row.pricePyg != null && !isNaN(row.pricePyg))
          ? Math.round(row.pricePyg)
          : null;

        const base = (row.baseUsd != null && !isNaN(row.baseUsd))
          ? Number(row.baseUsd)
          : null;

        const priceBase = (base != null)
          ? roundToClient(base * FIXED_RATE, ROUND_STEP)
          : null;

        const priceRecargo = (base != null)
          ? roundToClient(base * FIXED_RATE * FIXED_MARGIN, ROUND_STEP)
          : null;

        const tasa = (priceShop != null && base != null && base > 0)
          ? (priceShop / base)
          : null;

        const baseStr = base != null ? base.toFixed(2) : "";
        const tasaStr = tasa != null ? tasa.toFixed(2) : "";

        const tasaClass =
          tasa != null && (tasa < 5000 || tasa > 15000)
            ? "badge badge-danger"
            : "badge";

        html += "<tr data-modified='0'>";
        html += "<td>" + escapeHtml(title) +
          "<br><small style='color:#999;'>Var ID: " + vid + "</small></td>";
        html += "<td>" + escapeHtml(sku) + "</td>";

        html += "<td class='numeric'>" +
          (priceBase != null ? priceBase.toLocaleString('es-PY') : "") +
          "</td>";

        html += "<td class='numeric'>" +
          (priceRecargo != null ? priceRecargo.toLocaleString('es-PY') : "") +
          "</td>";

        html += "<td class='numeric'>";
        html += "<input type='number' step='0.01' style='width:100%;padding:4px 6px;font-size:12px;border-radius:4px;border:1px solid #cfd3dd;' ";
        html += "value='" + baseStr + "' data-variant-id='" + vid + "' class='base-input-row'>";
        html += "</td>";

        html += "<td class='numeric'>" + (tasaStr
          ? "<span class='" + tasaClass + "'>" + tasaStr + "</span>"
          : "") + "</td>";

        html += "<td style='text-align:center;'>";
        html += "<button type='button' class='secondary btn-save-row' data-variant-id='" + vid + "' style='font-size:12px;padding:4px 10px;margin-top:0;'>Guardar</button>";
        html += "</td>";
        html += "</tr>";
      }

      html += "</tbody></table>";
      baseTableDiv.innerHTML = html;

      const buttons = baseTableDiv.querySelectorAll(".btn-save-row");
      buttons.forEach(btn => {
        btn.addEventListener("click", async () => {
          const pin = getPinOrAlert();
          if (!pin) return;
          const vid = btn.getAttribute("data-variant-id");
          const input = baseTableDiv.querySelector("input.base-input-row[data-variant-id='" + vid + "']");
          if (!input) { alert("No se encontró el input para esa fila"); return; }
          const val = input.value.trim();
          if (!val) { alert("Ingresá un Base USD válido"); return; }

          const params = new URLSearchParams();
          params.set("pin", pin);
          params.set("variantId", vid);
          params.set("baseUsd", val);
          params.set("applyRate", "1");
          params.set("rate", String(FIXED_RATE));
params.set("margin", String(FIXED_MARGIN));
params.set("round", String(ROUND_STEP));


          try {
            const r = await fetch("/set-base-usd?" + params.toString());
            const txt = await r.text();
            let j;
            try { j = JSON.parse(txt); } catch (_) { j = { message: txt }; }
            alert(j.message || JSON.stringify(j));

            if (j.ok) {
              const tr = btn.closest("tr");
              if (tr) {
                const cells = tr.querySelectorAll("td");

                const baseNum = parseFloat(val.replace(",", "."));
                if (baseNum && isFinite(baseNum) && baseNum > 0) {
                  const priceBase = roundToClient(baseNum * FIXED_RATE, ROUND_STEP);
                  const priceRecargo = roundToClient(baseNum * FIXED_RATE * FIXED_MARGIN, ROUND_STEP);

                  const cellBase    = cells[2];
                  const cellRecargo = cells[3];
                  if (cellBase) {
                    cellBase.textContent = priceBase.toLocaleString("es-PY");
                  }
                  if (cellRecargo) {
                    cellRecargo.textContent = priceRecargo.toLocaleString("es-PY");
                  }

                  if (typeof j.newPricePYG === "number") {
                    const newPrice = j.newPricePYG;
                    const tasa = newPrice / baseNum;
                    const tasaCell = cells[5];
                    if (tasaCell) {
                      const cls = (tasa < 5000 || tasa > 10000)
                        ? "badge badge-danger"
                        : "badge";
                      tasaCell.innerHTML =
                        "<span class='" + cls + "'>" + tasa.toFixed(2) + "</span>";
                    }
                  }
                }

                input.classList.add("modified");
                tr.classList.add("row-modified");
                tr.setAttribute("data-modified", "1");
                applyModifiedFilter();
              }
            }
          } catch (err) {
            alert("Error guardando Base USD: " + err);
          }
        });
      });
    }

    function applyModifiedFilter() {
      if (!toggleModified || !baseTableDiv.querySelector("table")) return;
      const onlyMod = toggleModified.checked;
      const rows = baseTableDiv.querySelectorAll("tbody tr");
      rows.forEach(tr => {
        const isMod = tr.getAttribute("data-modified") === "1";
        tr.style.display = (!onlyMod || isMod) ? "" : "none";
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

    // --------- HISTORIAL DE CAMBIOS (LOG) ----------

    if (btnLoadLog) {
      btnLoadLog.addEventListener("click", async () => {
        const pin = getPinOrAlert();
        if (!pin) return;

        const rangeVal = logRange ? parseInt(logRange.value || "0", 10) : 0;

        const params = new URLSearchParams();
        params.set("pin", pin);

        try {
          const r = await fetch("/log?" + params.toString());
          const txt = await r.text();
          let entries;
          try {
            entries = JSON.parse(txt);
          } catch (_) {
            alert("Respuesta no válida de /log");
            console.log(txt);
            return;
          }

          if (!Array.isArray(entries)) entries = [];

          const now = Date.now();
          let filtered = entries;

          if (rangeVal > 0) {
            const maxAgeMs = rangeVal * 60 * 60 * 1000;
            filtered = entries.filter((e) => {
              if (!e.iso) return false;
              const t = Date.parse(e.iso);
              if (!t || Number.isNaN(t)) return false;
              return now - t <= maxAgeMs;
            });
          }

          if (!filtered.length) {
            logList.innerHTML =
              "<div style='padding:6px;color:#777;'>No hay registros para el rango seleccionado.</div>";
            return;
          }

          const sorted = filtered.slice().reverse();

          let html = "<ul style='list-style:none;padding-left:0;margin:0;'>";
          for (const entry of sorted) {
            const product = entry.product || "(sin nombre)";
            const baseUsd =
              entry.baseUsd != null && !isNaN(entry.baseUsd)
                ? Number(entry.baseUsd).toFixed(2)
                : "";
            const beforeP =
              entry.price_before != null && !isNaN(entry.price_before)
                ? Number(entry.price_before).toLocaleString("es-PY")
                : "";
            const afterP =
              entry.price_after != null && !isNaN(entry.price_after)
                ? Number(entry.price_after).toLocaleString("es-PY")
                : "";
            const status = entry.status || "";
            const timeTxt = entry.time || "";

            let line = "<strong>" + escapeHtml(product) + "</strong>";

            if (baseUsd) {
              line += " — " + baseUsd + " USD";
            }
            if (afterP) {
              line += " — precio: " + afterP + " Gs";
            }
            if (beforeP) {
              line += " (antes: " + beforeP + " Gs)";
            }
            if (status) {
              line +=
                " — <span style='color:#555;'>" +
                escapeHtml(status) +
                "</span>";
            }
            if (timeTxt) {
              line +=
                "<br><span style='color:#999;font-size:11px;'>" +
                escapeHtml(timeTxt) +
                "</span>";
            }

            html +=
              "<li style='padding:4px 0;border-bottom:1px solid #eee;'>" +
              line +
              "</li>";
          }
          html += "</ul>";

          logList.innerHTML = html;
        } catch (err) {
          alert("Error cargando historial: " + err);
        }
      });
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
        url.searchParams.get("pin") ||
        req.headers.get("X-Admin-Pin") ||
        "";
      const validPin = env.ADMIN_PIN;
      if (!validPin) return cors(text("Falta ADMIN_PIN en variables de entorno", 500));
      if (!pinProvided || pinProvided !== validPin) {
        return cors(text("PIN inválido", 403));
      }

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

      const rate = norm(url.searchParams.get("rate")) || norm(env.MANUAL_RATE);
      const margin =
        norm(url.searchParams.get("margin")) ||
        norm(env.MARGIN_FACTOR) ||
        1.25;
      const roundToVal =
        parseInt(url.searchParams.get("round") || env.ROUND_TO || "0", 10) || 0;
      const totalVariantsHint =
        url.searchParams.get("total_variants") ||
        env.TOTAL_VARIANTS_HINT ||
        null;
      const cronMin =
        url.searchParams.get("cron_min") || env.CRON_MINUTES || null;

      if (!rate || rate <= 0) {
        return cors(text("Debe indicar ?rate= o configurar MANUAL_RATE", 400));
      }

      const job = {
        mode: "update",
        running: true,
        startedAt: new Date().toISOString(),
        pageInfo: null,
        cursorResets: 0,
        processedProducts: 0,
        processedVariants: 0,
        updatedVariants: 0,
        seededVariants: 0,
        rate,
        margin,
        roundTo: roundToVal,
        totalVariantsHint,
        cronMin,
        lastRunAt: null,
        lastMsg: "started (update)",
      };

      await saveJob(env, job);
      return cors(
        json({
          ok: true,
          message: "Job de actualización iniciado",
          job,
        })
      );
    }

    // ---------- RESET BASE_USD protegido con PIN (no cambia precios) ----------

    if (path === "/reset-base" && (req.method === "POST" || req.method === "GET")) {
      const pinProvided =
        url.searchParams.get("pin") ||
        req.headers.get("X-Admin-Pin") ||
        "";
      const validPin = env.ADMIN_PIN;
      if (!validPin) return cors(text("Falta ADMIN_PIN en variables de entorno", 500));
      if (!pinProvided || pinProvided !== validPin) {
        return cors(text("PIN inválido", 403));
      }

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

      const rate = norm(url.searchParams.get("rate")) || norm(env.MANUAL_RATE);
      const totalVariantsHint =
        url.searchParams.get("total_variants") ||
        env.TOTAL_VARIANTS_HINT ||
        null;
      const cronMin =
        url.searchParams.get("cron_min") || env.CRON_MINUTES || null;

      if (!rate || rate <= 0) {
        return cors(text("Debe indicar ?rate= o configurar MANUAL_RATE", 400));
      }

      const job = {
        mode: "reset_base",
        running: true,
        startedAt: new Date().toISOString(),
        pageInfo: null,
        cursorResets: 0,
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
        json({
          ok: true,
          message: "Job de reset base_usd iniciado",
          job,
        })
      );
    }

    // ---------- SET BASE_USD manual (1 variante) + actualizar precio ----------

    if (path === "/set-base-usd" && (req.method === "POST" || req.method === "GET")) {
      const pinProvided =
        url.searchParams.get("pin") ||
        req.headers.get("X-Admin-Pin") ||
        "";
      const validPin = env.ADMIN_PIN;
      if (!validPin) return cors(text("Falta ADMIN_PIN en variables de entorno", 500));
      if (!pinProvided || pinProvided !== validPin) {
        return cors(text("PIN inválido", 403));
      }

      const variantId = url.searchParams.get("variantId");
      const baseUsdRaw = url.searchParams.get("baseUsd");
      const baseUsd = norm(baseUsdRaw);
      const applyRate = url.searchParams.get("applyRate") === "1";

      if (!variantId) return cors(text("Falta variantId", 400));
      if (!baseUsd || baseUsd <= 0) {
        return cors(text("Base USD inválido", 400));
      }

      const shop = getShop(env);
      const okBase = await upsertBaseUSD_GQL(shop, variantId, baseUsd);

      let newPricePYG = null;
      if (okBase && applyRate) {
        // Prioridad: params del panel -> env -> fallback seguro
        const rateParam = norm(url.searchParams.get("rate"));
        const marginParam = norm(url.searchParams.get("margin"));
        const roundParamRaw = url.searchParams.get("round");

        const rateEnv = norm(env.MANUAL_RATE);
        const marginEnv = norm(env.MARGIN_FACTOR);
        const roundEnv = parseInt(env.ROUND_TO || "0", 10) || 0;

        const rateUse =
          Number.isFinite(rateParam) && rateParam > 0 ? rateParam : rateEnv;

        const marginUse =
          Number.isFinite(marginParam) && marginParam > 0
            ? marginParam
            : (Number.isFinite(marginEnv) && marginEnv > 0 ? marginEnv : 1.25);

        const roundUse = (() => {
          const rp = parseInt(roundParamRaw || "", 10);
          return Number.isFinite(rp) ? rp : roundEnv;
        })();

        if (rateUse && rateUse > 0) {
          let calc = baseUsd * rateUse * marginUse;
          calc = roundTo(calc, roundUse);

          const okPrice = await updateVariantPrice(shop, variantId, calc);
          if (okPrice) newPricePYG = calc;
        }
      }

      if (okBase) {
        await addLog(env, {
          product: "(manual)",
          variantId,
          price_before: null,
          price_after: newPricePYG,
          status: applyRate && newPricePYG != null
            ? "base_usd_manual_set+price"
            : "base_usd_manual_set",
          baseUsd,
        });

        return cors(
          json({
            ok: true,
            message:
              "Base USD guardado para la variante " +
              variantId +
              (applyRate && newPricePYG != null
                ? " y precio actualizado"
                : ""),
            baseUsd,
            newPricePYG,
          })
        );
      } else {
        return cors(
          json(
            {
              ok: false,
              message: "Error al guardar Base USD",
            },
            500
          )
        );
      }
    }

    // ---------- CANCEL protegido con PIN ----------

    if (path === "/cancel" && (req.method === "POST" || req.method === "GET")) {
      const pinProvided =
        url.searchParams.get("pin") ||
        req.headers.get("X-Admin-Pin") ||
        "";
      const validPin = env.ADMIN_PIN;
      if (!validPin) return cors(text("Falta ADMIN_PIN en variables de entorno", 500));
      if (!pinProvided || pinProvided !== validPin) {
        return cors(text("PIN inválido", 403));
      }

      await env.JOBS_KV.delete(JOB_KEY);
      return cors(json({ ok: true, message: "Job cancelado" }));
    }

    // ---------- LOG: ver historial completo ----------

    if (path === "/log") {
      const pinProvided = url.searchParams.get("pin") || "";
      const validPin = env.ADMIN_PIN;
      if (!validPin) return cors(text("Falta ADMIN_PIN en variables de entorno", 500));
      if (!pinProvided || pinProvided !== validPin) {
        return cors(text("PIN inválido", 403));
      }

      const raw = await env.LOG_KV.get(LOG_KEY);
      return cors(json(raw ? JSON.parse(raw) : []));
    }

    // ---------- LOG: limpiar ----------

    if (path === "/log/clear") {
      const pinProvided = url.searchParams.get("pin") || "";
      const validPin = env.ADMIN_PIN;
      if (!validPin) return cors(text("Falta ADMIN_PIN en variables de entorno", 500));
      if (!pinProvided || pinProvided !== validPin) {
        return cors(text("PIN inválido", 403));
      }

      await env.LOG_KV.delete(LOG_KEY);
      return cors(json({ ok: true, message: "Log limpiado" }));
    }

    // ---------- BASE-LIST: lista tipo "Lista de productos" ----------

    if (path === "/base-list" && req.method === "GET") {
      const pinProvided = url.searchParams.get("pin") || "";
      const validPin = env.ADMIN_PIN;
      if (!validPin) return cors(text("Falta ADMIN_PIN en variables de entorno", 500));
      if (!pinProvided || pinProvided !== validPin) {
        return cors(text("PIN inválido", 403));
      }

      const rawSearch = (url.searchParams.get("q") || "").trim();
      const status = (url.searchParams.get("status") || "").trim();
      const pageSize =
        parseInt(url.searchParams.get("limit") || "50", 10) || 50;
      const cursor = url.searchParams.get("cursor") || null;

      const parts = [];
      if (rawSearch) parts.push(rawSearch);
      if (status === "active") parts.push("status:active");
      else if (status === "draft") parts.push("status:draft");
      else if (status === "archived") parts.push("status:archived");

      const finalQuery = parts.length ? parts.join(" AND ") : null;

      const shop = getShop(env);
      const page = await fetchBaseListPage(shop, {
        query: finalQuery,
        pageSize,
        cursor,
      });

      return cors(
        json({
          ok: true,
          rows: page.rows,
          nextCursor: page.nextCursor,
        })
      );
    }

    // fallback
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
    const pageSize = 10;

    const { products, nextPageInfo, cursorInvalid } = await fetchProducts(shop, {
      pageSize,
      pageInfo: job.pageInfo,
      onlyActive: true,
    });

    // FIX: si Shopify devolvió 400 por cursor, reiniciar paginación sin “rendirse”
    if (cursorInvalid) {
      job.cursorResets = (job.cursorResets || 0) + 1;

      if (job.cursorResets <= 3) {
        job.pageInfo = null; // restart paginación
        job.lastRunAt = new Date().toISOString();
        job.lastMsg =
          "Cursor inválido (400). Reiniciando paginación (" +
          job.cursorResets +
          "/3)";
        await saveJob(env, job);
        return;
      } else {
        job.running = false;
        job.lastRunAt = new Date().toISOString();
        job.lastMsg =
          "ERROR: cursor inválido repetido. Job detenido para evitar loop.";
        await saveJob(env, job);
        return;
      }
    }

    if (!products.length) {
      job.running = false;
      job.lastMsg = "Completado (fin de paginacion)";
      job.lastRunAt = new Date().toISOString();
      await saveJob(env, job);
      return;
    }

    let variantsDone = 0;

    for (const p of products) {
      job.processedProducts++;

      for (const v of (p.variants || [])) {
        if (variantsDone >= BATCH_LIMIT) break;

        const variantId = v.id;
        const pricePYG = norm(v.price);

        if (!pricePYG || pricePYG <= 0) {
          await sleep(REQ_THROTTLE_MS);
          continue;
        }

        job.processedVariants++;

        if (mode === "reset_base") {
          const baseCalculated = Math.round((pricePYG / job.rate) * 100) / 100;
          let status = "skipped";

          if (Number.isFinite(baseCalculated) && baseCalculated > 0) {
            const ok = await upsertBaseUSD_GQL(shop, variantId, baseCalculated);
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
          });

          variantsDone++;
          await sleep(REQ_THROTTLE_MS);
          if (variantsDone >= BATCH_LIMIT) break;
          continue;
        }

        // modo update normal
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
      ? "Completado (fin de paginacion)"
      : "Procesados: " + variantsDone;

    if (!nextPageInfo && variantsDone < BATCH_LIMIT) {
      job.running = false;
    }

    await saveJob(env, job);
  } catch (e) {
    job.lastMsg = "ERROR: " + (e?.message || String(e));
    job.lastRunAt = new Date().toISOString();
    await saveJob(env, job);
  }
  }

// ============ Shopify: listado de productos (REST para el job) ============

function extractPageInfo(linkHeader) {
  if (!linkHeader) return null;
  const match = linkHeader.match(/<[^>]*[?&]page_info=([^&>]*)>; rel="next"/i);
  return match ? match[1] : null;
}
  
async function fetchProducts(shop, { pageSize = 50, pageInfo = null, onlyActive = true }) {
  const { domain, token } = shop;
  const baseUrl = `https://${domain}/admin/api/${API_VERSION}/products.json`;

  const params = new URLSearchParams();
  params.set("limit", String(pageSize));
  if (pageInfo) params.set("page_info", pageInfo);
  if (onlyActive && !pageInfo) params.set("status", "active");
  params.set("order", "title asc");

  const url = `${baseUrl}?${params.toString()}`;

  const r = await fetch(url, {
    method: "GET",
    headers: {
      "X-Shopify-Access-Token": token,
      "Content-Type": "application/json",
    },
  });

  if (!r.ok) {
    // Cursor inválido: NO tirar excepción, avisar al job para resetear
    if (r.status === 400 && pageInfo) {
      return { products: [], nextPageInfo: null, cursorInvalid: true };
    }
    throw new Error("GET products -> " + r.status);
  }

  const data = await r.json();
  const link = r.headers.get("Link") || "";
  const nextPageInfo = extractPageInfo(link);

  return {
    products: (data && data.products) ? data.products : [],
    nextPageInfo,
    cursorInvalid: false,
  };
}


// ============ BASE-LIST: lista tipo "Lista de productos" (GraphQL + cursor) ============

async function fetchBaseListPage(shop, { query, pageSize, cursor }) {
  const { domain, token } = shop;
  const endpoint = `https://${domain}/admin/api/${API_VERSION}/graphql.json`;

  const gql = `
    query productVariantBaseList($first: Int!, $query: String, $after: String) {
      products(first: $first, query: $query, sortKey: TITLE, after: $after) {
        edges {
          cursor
          node {
            id
            title
            variants(first: 50) {
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
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
  `;

  const variables = {
    first: pageSize,
    query: query || null,
    after: cursor || null,
  };

  const r = await fetch(endpoint, {
    method: "POST",
    headers: {
      "X-Shopify-Access-Token": token,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query: gql, variables }),
  });

  if (!r.ok) {
    throw new Error("GraphQL base-list -> " + r.status);
  }

  const data = await r.json();
  const productsNode = data?.data?.products;
  const edges = productsNode?.edges || [];
  const pageInfo = productsNode?.pageInfo || {};
  const rows = [];

  for (const e of edges) {
    const pNode = e.node;
    const title = pNode?.title || "Sin titulo";
    const variantsEdges = pNode?.variants?.edges || [];

    for (const ve of variantsEdges) {
      const vNode = ve.node;
      if (!vNode) continue;

      const gid = vNode.id || "";
      const variantId = gid.split("/").pop();
      const sku = vNode.sku || "";
      const price = vNode.price != null ? Number(vNode.price) : null;
      const mf = vNode.metafield;
      const baseUsdVal = mf?.value != null ? Number(mf.value) : null;

      rows.push({
        productTitle: title,
        variantId,
        sku,
        pricePyg: price,
        baseUsd: baseUsdVal,
      });
    }
  }

  const nextCursor =
    pageInfo && pageInfo.hasNextPage ? pageInfo.endCursor || null : null;

  return { rows, nextCursor };
}

// ============ base_usd: lectura y siembra segura (para modo update) ============

async function getOrSeedBaseUSD(shop, variantId, currentPricePyg, rate) {
  const baseFromMeta = await readBaseUSD_GQL(shop, variantId);

  if (baseFromMeta && baseFromMeta > 0) {
    return { baseUsd: baseFromMeta, seeded: false };
  }

  if (!rate || rate <= 0) {
    return { baseUsd: null, seeded: false };
  }

  const baseCalculated = Math.round((currentPricePyg / rate) * 100) / 100;
  if (!Number.isFinite(baseCalculated) || baseCalculated <= 0) {
    return { baseUsd: null, seeded: false };
  }

  const seededOk = await upsertBaseUSD_GQL(shop, variantId, baseCalculated);
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
      variant: {
        id: variantId,
        price: Math.round(pyg),
      },
    }),
  });

  return r.ok;
}

// ============ Helpers Shopify / KV / utils ============

function getShop(env) {
  const domain = env.SHOPIFY_STORE_DOMAIN;
  const token = env.SHOPIFY_ADMIN_TOKEN;
  if (!domain || !token) {
    throw new Error("Falta SHOPIFY_STORE_DOMAIN o SHOPIFY_ADMIN_TOKEN");
  }
  return { domain, token };
}

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
    try { arr = JSON.parse(raw); } catch (e) { arr = []; }
  }

  if (!entry.time) {
    entry.time = new Date().toLocaleString("es-PY", {
      timeZone: STATUS_TIMEZONE,
    });
  }
  if (!entry.iso) {
    entry.iso = new Date().toISOString();
  }

  arr.push(entry);
  if (arr.length > 2000) arr.shift();
  await env.LOG_KV.put(LOG_KEY, JSON.stringify(arr));
}

function cors(res) {
  res.headers.set("Access-Control-Allow-Origin", "*");
  res.headers.set("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.headers.set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Admin-Pin");
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
  if (/^\d{1,3}(\.\d{3})+(,\d+)?$/.test(t)) {
    return parseFloat(t.replace(/\./g, "").replace(",", "."));
  }
  if (/^\d{1,3}(,\d{3})+(\.\d+)?$/.test(t)) {
    return parseFloat(t.replace(/,/g, ""));
  }
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
  return new Promise((resolve) => setTimeout(resolve, ms));
}










