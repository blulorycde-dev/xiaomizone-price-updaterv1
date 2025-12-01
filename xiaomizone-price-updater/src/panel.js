// src/panel.js

export function renderPanelHtml() {
  return `
<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <title>Panel price-updater</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body {
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #f5f5f5;
      margin: 0;
      padding: 16px;
    }
    .card {
      max-width: 720px;
      margin: 0 auto;
      background: #ffffff;
      border-radius: 10px;
      padding: 16px 20px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.05);
    }
    h1 {
      font-size: 20px;
      margin-bottom: 8px;
    }
    label {
      display: block;
      margin-top: 8px;
      font-size: 14px;
    }
    input {
      width: 100%;
      padding: 6px 8px;
      font-size: 14px;
      border-radius: 6px;
      border: 1px solid #ccc;
      box-sizing: border-box;
    }
    button {
      margin-top: 12px;
      padding: 8px 12px;
      font-size: 14px;
      border-radius: 6px;
      border: none;
      cursor: pointer;
      background: #ff6a00;
      color: #fff;
    }
    button.secondary {
      background: #e0e0e0;
      color: #333;
      margin-left: 8px;
    }
    pre {
      background: #111827;
      color: #e5e7eb;
      padding: 8px 10px;
      border-radius: 6px;
      font-size: 12px;
      max-height: 260px;
      overflow: auto;
      margin-top: 12px;
    }
  </style>
</head>
<body>
  <div class="card">
    <h1>price-updater · Panel</h1>
    <p>Job de actualización de precios basado en <code>base_usd</code>.</p>

    <label>PIN administrador</label>
    <input id="pin" type="password" placeholder="ADMIN_PIN" />

    <label>Tasa (Gs/USD)</label>
    <input id="rate" type="number" step="0.01" placeholder="Ej: 7200" />

    <label>Margen (factor)</label>
    <input id="margin" type="number" step="0.01" placeholder="Ej: 1.00" />

    <label>Redondeo (Paso en Gs)</label>
    <input id="round" type="number" step="10" placeholder="Ej: 100" />

    <label>Total variantes (límite aproximado)</label>
    <input id="total" type="number" step="10" placeholder="Ej: 500" />

    <button id="start">Iniciar job</button>
    <button id="cancel" class="secondary">Cancelar job</button>
    <button id="refresh" class="secondary">Refrescar estado</button>

    <pre id="statusBox">{ cargando estado... }</pre>
  </div>

  <script>
    async function callApi(path, options = {}) {
      const pin = document.getElementById("pin").value.trim();
      const headers = options.headers || {};
      if (pin) headers["X-Admin-Pin"] = pin;
      options.headers = headers;

      const res = await fetch(path, options);
      const text = await res.text();
      try {
        return JSON.parse(text);
      } catch {
        return text;
      }
    }

    async function refreshStatus() {
      const data = await callApi("/status");
      document.getElementById("statusBox").textContent = JSON.stringify(data, null, 2);
    }

    document.getElementById("start").onclick = async () => {
      const rate = document.getElementById("rate").value;
      const margin = document.getElementById("margin").value;
      const round = document.getElementById("round").value;
      const total = document.getElementById("total").value;

      const params = new URLSearchParams();
      if (rate) params.set("rate", rate);
      if (margin) params.set("margin", margin);
      if (round) params.set("round", round);
      if (total) params.set("total_variants", total);
      params.set("mode", "update");

      const data = await callApi("/admin/start?" + params.toString());
      document.getElementById("statusBox").textContent = JSON.stringify(data, null, 2);
    };

    document.getElementById("cancel").onclick = async () => {
      const data = await callApi("/admin/cancel");
      document.getElementById("statusBox").textContent = JSON.stringify(data, null, 2);
    };

    document.getElementById("refresh").onclick = refreshStatus;

    refreshStatus();
    setInterval(refreshStatus, 8000);
  </script>
</body>
</html>
`;
}
