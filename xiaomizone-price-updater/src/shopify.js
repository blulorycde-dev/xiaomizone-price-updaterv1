// src/shopify.js

import { sleep, roundToStep } from "./utils.js";

function adminHeaders(config) {
  return {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": config.adminToken,
  };
}

// Convierte GID GraphQL a ID numérico (para REST)
function gidToNumericId(gid) {
  const parts = gid.split("/");
  return parts[parts.length - 1];
}

// Consulta variantes por lotes con GraphQL
export async function fetchVariantBatch(env, config, cursor, limit) {
  const query = `
    query FetchVariants($cursor: String, $limit: Int!) {
      productVariants(first: $limit, after: $cursor) {
        edges {
          cursor
          node {
            id
            sku
            price
            compareAtPrice
            metafields(first: 10, namespace: "xiaomizone") {
              edges {
                node {
                  key
                  value
                }
              }
            }
          }
        }
        pageInfo {
          hasNextPage
        }
      }
    }
  `;

  const body = JSON.stringify({
    query,
    variables: { cursor, limit },
  });

  const url = `https://${config.storeDomain}/admin/api/${config.apiVersion}/graphql.json`;

  const res = await fetch(url, {
    method: "POST",
    headers: adminHeaders(config),
    body,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`GraphQL error ${res.status}: ${text}`);
  }

  const data = await res.json();
  const pv = data.data?.productVariants;
  if (!pv) {
    throw new Error("Respuesta GraphQL inesperada");
  }

  const edges = pv.edges || [];
  const variants = edges.map((edge) => {
    const node = edge.node;
    const metaEdges = node.metafields?.edges || [];
    const metafields = {};
    for (const m of metaEdges) {
      metafields[m.node.key] = m.node.value;
    }
    return {
      cursor: edge.cursor,
      id: node.id,
      sku: node.sku,
      price: Number(node.price),
      compareAtPrice: node.compareAtPrice
        ? Number(node.compareAtPrice)
        : null,
      metafields,
    };
  });

  return {
    variants,
    hasNextPage: pv.pageInfo?.hasNextPage || false,
    lastCursor: edges.length > 0 ? edges[edges.length - 1].cursor : cursor,
  };
}

// ACTUALIZA EL PRECIO DE UNA VARIANTE EN SHOPIFY (REST)
export async function updateVariantPrice(env, config, variantIdGid, newPriceGs) {
  const numericId = gidToNumericId(variantIdGid);
  const url = `https://${config.storeDomain}/admin/api/${config.apiVersion}/variants/${numericId}.json`;

  const body = JSON.stringify({
    variant: {
      id: Number(numericId),
      price: newPriceGs,
    },
  });

  const res = await fetch(url, {
    method: "PUT",
    headers: adminHeaders(config),
    body,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(
      `Error al actualizar variante ${numericId}: ${res.status} - ${text}`
    );
  }
}

// Procesa un batch: calcula nuevos precios y los aplica
export async function processPriceUpdateBatch(env, config, job) {
  const limit = config.batchLimit;
  const { variants, hasNextPage, lastCursor } = await fetchVariantBatch(
    env,
    config,
    job.cursor,
    limit
  );

  if (!variants.length) {
    return {
      processedDelta: 0,
      hasMore: false,
      newCursor: job.cursor,
    };
  }

  let processedDelta = 0;

  for (const variant of variants) {
    const baseUsdRaw = variant.metafields["base_usd"];
    if (!baseUsdRaw) {
      processedDelta += 1;
      continue;
    }

    const baseUsd = Number(baseUsdRaw);
    if (!baseUsd || isNaN(baseUsd)) {
      processedDelta += 1;
      continue;
    }

    const rate = job.rate ?? config.manualRate;
    const margin = job.margin ?? config.manualMargin;
    const roundStep = job.roundStep ?? config.roundStep;

    const rawPriceGs = baseUsd * rate * margin;
    const finalPriceGs = roundToStep(rawPriceGs, roundStep);

    await updateVariantPrice(env, config, variant.id, finalPriceGs);

    processedDelta += 1;

    if (config.throttleMs > 0) {
      await sleep(config.throttleMs);
    }
  }

  return {
    processedDelta,
    hasMore: hasNextPage,
    newCursor: hasNextPage ? lastCursor : null,
  };
}
