require("dotenv").config();
const crypto = require("crypto");
const { MongoClient } = require("mongodb");



const QUEUE_COLLECTION = "webhookQueue";

const WEBHOOK_PROCESS_URL = process.env.WEBHOOK_PROCESS_URL || "";
const WEBHOOK_CRON_SECRET = process.env.WEBHOOK_CRON_SECRET || "";

function serializeForWebhook(obj) {
  if (obj == null) return obj;
  if (typeof obj !== "object") return obj;
  if (obj.constructor && obj.constructor.name === "ObjectId") return obj.toString();
  if (obj instanceof Date) return obj.toISOString();
  if (Array.isArray(obj)) return obj.map(serializeForWebhook);
  const out = {};
  for (const k of Object.keys(obj)) out[k] = serializeForWebhook(obj[k]);
  return out;
}


const FILE_FIELDS_MAP = {
  Product: ["imageLink", "techPacks", "additionalDocuments"],
  Design: ["profileImage"],
  DesignVersion: ["imageLink", "profileImage"],
  PricingRequest: ["attachments", "techPacks", "additionalDocuments"],
  PO: ["attachments", "techPacks", "additionalDocuments", "pdfUrl"],
  PricingRequests: ["attachments", "techPacks", "imageUrl"],
  Request: ["files"],
  lineItem: ["images", "pdfUrl"],
};

function getFileFields(entityType) {
  if (!entityType || typeof entityType !== "string") return [];
  return FILE_FIELDS_MAP[entityType] || [];
}

function generateFileId(url) {
  if (!url || typeof url !== "string") return null;
  let normalized = String(url).trim();
  try {
    if (normalized.includes("firebasestorage.googleapis.com")) {
      const beforeQuery = normalized.split("?")[0];
      if (beforeQuery) normalized = beforeQuery;
    }
  } catch (_) {}
  return crypto
    .createHash("sha256")
    .update(normalized, "utf8")
    .digest("hex")
    .substring(0, 32);
}

function extractUrlsFromValue(val) {
  const out = [];
  if (!val) return out;
  if (typeof val === "string") {
    const s = val.trim();
    if (s && (s.startsWith("http://") || s.startsWith("https://"))) out.push(val);
    return out;
  }
  if (Array.isArray(val)) {
    for (const item of val) {
      if (typeof item === "string") {
        const s = item.trim();
        if (s && (s.startsWith("http://") || s.startsWith("https://"))) out.push(item);
      } else if (item && typeof item === "object") {
        const u = item.url || item.imageUrl || item.fileUrl || item.downloadUrl;
        if (u && typeof u === "string") {
          const s = String(u).trim();
          if (s && (s.startsWith("http://") || s.startsWith("https://"))) out.push(u);
        }
      }
    }
  }
  return out;
}

function separateFileFields(data, entityType) {
  const regularData = JSON.parse(JSON.stringify(data || {}));
  const fileFields = getFileFields(entityType);
  const files = [];
  const seen = new Set();

  for (const field of fileFields) {
    let val = regularData[field] ?? regularData.customFields?.[field];
    if (val === undefined || val === null) continue;
    const urls = extractUrlsFromValue(val);
    for (const url of urls) {
      if (seen.has(url)) continue;
      seen.add(url);
      const fid = generateFileId(url);
      if (fid) files.push({ File_id: fid, File_url: url, file_type: field });
    }
    if (regularData[field] !== undefined) delete regularData[field];
    if (regularData.customFields && regularData.customFields[field] !== undefined) {
      delete regularData.customFields[field];
      if (Object.keys(regularData.customFields).length === 0) delete regularData.customFields;
    }
  }

  return { regularData, files };
}

// --- Queue helpers (mirrors webhookQueue.js shape) ---

function buildIdempotencyKey(entityType, operation, entityId) {
  const raw = `${entityType}:${operation}:${entityId || ""}`;
  return crypto.createHash("sha256").update(raw, "utf8").digest("hex").substring(0, 32);
}

function buildQueueDocFromOptions(options) {
  const {
    entityType,
    operation,
    data,
    responseData = null,
    entityId = null,
    actionType = null,
    files = [],
    headers = {},
  } = options;

  let finalEntityId = entityId;
  if (!finalEntityId && responseData) {
    finalEntityId = responseData?.id || responseData?._id || data?._id;
  }
  if (finalEntityId && typeof finalEntityId === "object" && finalEntityId.toString) {
    finalEntityId = finalEntityId.toString();
  }

  const idempotencyKey = buildIdempotencyKey(entityType, operation, finalEntityId);
  return {
    entityType,
    operation,
    data: data || {},
    responseData: responseData || null,
    entityId: finalEntityId,
    actionType: actionType || null,
    files: Array.isArray(files) ? files : [],
    headers: headers || {},
    status: "pending",
    retryCount: 0,
    nextRetryAt: null,
    claimedAt: null,
    idempotencyKey,
    createdAt: new Date(),
  };
}

async function enqueueWebhookEvent(options) {
  const queueDoc = buildQueueDocFromOptions(options);
  const client = new MongoClient(process.env.MONGODB_CONNECTION_STRING);
  await client.connect();
  const db = client.db(process.env.DB_NAME);
  const coll = db.collection(QUEUE_COLLECTION);

  const windowStart = new Date(Date.now() - 60 * 1000); // 1 minute idempotency window
  const existing = await coll.findOne({
    idempotencyKey: queueDoc.idempotencyKey,
    status: { $in: ["pending", "processing"] },
    createdAt: { $gte: windowStart },
  });
  if (existing) {
    console.log(
      `📤 Webhook duplicate skipped (idempotency): ${queueDoc.operation} ${queueDoc.entityType}${
        queueDoc.entityId ? ` (${queueDoc.entityId})` : ""
      }`
    );
    await client.close();
    return;
  }

  await coll.insertOne(queueDoc);
  await client.close();
  console.log(
    `📤 Webhook enqueued: ${queueDoc.operation} ${queueDoc.entityType}${
      queueDoc.entityId ? ` (${queueDoc.entityId})` : ""
    }`
  );
}

function triggerWebhookProcessNow() {
  if (!WEBHOOK_PROCESS_URL || !WEBHOOK_CRON_SECRET) return;
  const url = `${WEBHOOK_PROCESS_URL}?secret=${encodeURIComponent(WEBHOOK_CRON_SECRET)}`;
  // Fire-and-forget – no await
  try {
    // Node 18+ has global fetch
    fetch(url).catch(() => {});
  } catch (_) {
    // ignore if fetch not available
  }
}

// Public API used by cron/processImportData.js
async function callMakeWebhook(
  entityType,
  operation,
  data,
  responseData = null,
  entityId = null,
  actiontype = null,
  headers = {}
) {
  console.log("callMakeWebhook (queue)", entityType, operation, data, responseData, entityId);

  let finalEntityId = entityId;
  if (!finalEntityId && responseData) {
    finalEntityId = responseData?.id || responseData?._id || data?._id;
  }

  const { regularData, files } = separateFileFields(data || {}, entityType);

  await enqueueWebhookEvent({
    entityType,
    operation,
    data: regularData,
    responseData,
    entityId: finalEntityId,
    actionType: actiontype,
    files,
    headers: headers || {},
  });

  triggerWebhookProcessNow();
}

module.exports = {
  callMakeWebhook,
};

