
require("dotenv").config();
const crypto = require("crypto");
const { MongoClient } = require("mongodb");

const QUEUE_COLLECTION = "webhookQueue";


let _sharedClient = null;
let _sharedDb = null;
let _connectingPromise = null;

async function getDb() {
  if (_sharedDb) return _sharedDb;
  if (_connectingPromise) return _connectingPromise;

  _connectingPromise = (async () => {
    _sharedClient = new MongoClient(process.env.MONGODB_CONNECTION_STRING, {
      maxPoolSize: 5,
      minPoolSize: 1,
      serverSelectionTimeoutMS: 5000,
    });
    await _sharedClient.connect();
    _sharedDb = _sharedClient.db(process.env.DB_NAME);
    _connectingPromise = null;
    console.log(" Webhook DB connection established (shared)");
    return _sharedDb;
  })();

  return _connectingPromise;
}

async function closeWebhookDb() {
  if (_sharedClient) {
    await _sharedClient.close();
    _sharedClient = null;
    _sharedDb = null;
    console.log(" Webhook DB connection closed");
  }
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

function buildIdempotencyKey(entityType, operation, entityId) {
  const raw = `${entityType}:${operation}:${entityId || ""}`;
  return crypto.createHash("sha256").update(raw, "utf8").digest("hex").substring(0, 32);
}

function buildQueueDoc(options) {
  const {
    entityType, operation, data, responseData = null,
    entityId = null, actionType = null, files = [], headers = {},
  } = options;

  let finalEntityId = entityId;
  if (!finalEntityId && responseData) {
    finalEntityId = responseData?.id || responseData?._id || data?._id;
  }
  if (finalEntityId && typeof finalEntityId === "object" && finalEntityId.toString) {
    finalEntityId = finalEntityId.toString();
  }

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
    idempotencyKey: buildIdempotencyKey(entityType, operation, finalEntityId),
    createdAt: new Date(),
  };
}

async function enqueueWebhookEvent(options) {
  const queueDoc = buildQueueDoc(options);

  try {
    const db = await getDb();
    const coll = db.collection(QUEUE_COLLECTION);

    const windowStart = new Date(Date.now() - 60 * 1000);
    const existing = await coll.findOne({
      idempotencyKey: queueDoc.idempotencyKey,
      status: { $in: ["pending", "processing"] },
      createdAt: { $gte: windowStart },
    });

    if (existing) {
      console.log(`📤 Webhook duplicate skipped: ${queueDoc.operation} ${queueDoc.entityType}${queueDoc.entityId ? ` (${queueDoc.entityId})` : ""}`);
      return;
    }

    await coll.insertOne(queueDoc);
    console.log(`📤 Webhook enqueued: ${queueDoc.operation} ${queueDoc.entityType}${queueDoc.entityId ? ` (${queueDoc.entityId})` : ""}`);
  } catch (err) {
    console.error("Webhook enqueue error:", err.message);
    _sharedDb = null;
    _sharedClient = null;
    throw err;
  }
}

async function callMakeWebhook(
  entityType,
  operation,
  data,
  responseData = null,
  entityId = null,
  actiontype = null,
  headers = {}
) {
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
}

module.exports = {
  callMakeWebhook,
  closeWebhookDb,
};