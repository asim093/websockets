require("dotenv").config();
const axios = require("axios");
const { MongoClient } = require("mongodb");

const WEBHOOK_URL = process.env.MAKE_WEBHOOK_URL || "";
const WEBHOOK_API_KEY = process.env.MAKE_WEBHOOK_API_KEY || "";

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

function buildWebhookPayload(entityType, operation, data, entityId = null, actionType = null) {
  let endpoint = `/${entityType}`;
  if (entityId && (operation === "PUT" || operation === "DELETE")) {
    endpoint = `/${entityType}/${entityId}`;
  }
  const dataWithId = data && typeof data === "object" ? { ...data } : {};
  if (!dataWithId._id && entityId) {
    dataWithId._id = entityId;
  }
  const webhookData = {
    request_type: operation,
    endpoint,
    data: dataWithId,
  };
  if (actionType) webhookData.actiontype = actionType;
  return serializeForWebhook(webhookData);
}

async function saveWebhookLog(level, category, operation, entityType, entityId, message, data, status) {
  try {
    const client = new MongoClient(process.env.MONGODB_CONNECTION_STRING);
    await client.connect();
    const database = client.db(process.env.DB_NAME);
    const logEntry = {
      operation,
      entityType,
      entityId,
      data,
      status,
      timestamp: new Date(),
      createdAt: new Date(),
    };
    await database.collection("WebhookLogs").insertOne(logEntry);
    await client.close();
    console.log(`Webhook log saved: ${level} - ${message}`);
  } catch (error) {
    console.error("Error saving webhook log:", error.message);
  }
}

async function sendWebhookToMake(payload) {
  if (!WEBHOOK_URL || !WEBHOOK_API_KEY) {
    console.error("Webhook not configured: MAKE_WEBHOOK_URL / MAKE_WEBHOOK_API_KEY missing");
    return { success: false, error: "Webhook not configured" };
  }
  try {
    const response = await axios.post(WEBHOOK_URL, payload, {
      headers: {
        "x-make-apikey": WEBHOOK_API_KEY,
        "Content-Type": "application/json",
      },
      timeout: 30000,
    });
    return { success: true, data: response.data, status: response.status };
  } catch (error) {
    return { success: false, error: error.message, stack: error.stack };
  }
}

async function callMakeWebhook(entityType, operation, data, responseData = null, entityId = null, actiontype = null) {
  let finalEntityId = entityId;
  if (!finalEntityId && responseData) {
    finalEntityId = responseData?.id || responseData?._id || data?._id;
  }
  const payload = buildWebhookPayload(entityType, operation, data || {}, finalEntityId, actiontype);
  console.log("Webhook Data:", payload);

  const result = await sendWebhookToMake(payload);
  const logData = {
    status: result.status,
    responseData: result.data,
    error: result.error,
  };

  if (result.success) {
    await saveWebhookLog(
      "INFO",
      "WEBHOOK",
      operation,
      entityType,
      finalEntityId,
      `Webhook API Response for ${operation} ${entityType}`,
      logData,
      "SUCCESS"
    );
    console.log(
      `Webhook sent for ${operation} ${entityType}${finalEntityId ? ` (ID: ${finalEntityId})` : ""}`
    );
    return result.data;
  }

  await saveWebhookLog(
    "ERROR",
    "WEBHOOK",
    operation,
    entityType,
    finalEntityId,
    `Error calling Make webhook for ${operation} ${entityType}`,
    logData,
    "FAILED"
  );
  console.error(`Error calling Make webhook for ${operation} ${entityType}:`, result.error);
  return null;
}

module.exports = {
  callMakeWebhook,
};

