require("dotenv").config();
const { MongoClient } = require("mongodb");
const axios = require("axios");

async function saveWebhookLog(level, category, operation, entityType, entityId, message, data, status) {
  try {
    console.log("start webhooklog")
    const dbClient = new MongoClient(process.env.MONGODB_CONNECTION_STRING);
    await dbClient.connect();
    const database = dbClient.db(process.env.DB_NAME);
    const logEntry = {
      operation: operation,
      entityType: entityType,
      entityId: entityId,
      data: data,
      status: status,
      timestamp: new Date(),
      createdAt: new Date()
    };
    await database.collection("WebhookLogs").insertOne(logEntry);
    console.log(`Webhook log saved: ${level} - ${message}`);
    await dbClient.close();
  } catch (error) {
    console.error('Error saving webhook log:', error.message);
  }
}

async function callMakeWebhook(entityType, operation, data, responseData = null, entityId = null, actiontype = null) {
  console.log("callMakeWebhook", entityType, operation, data, responseData, entityId);
  const webhookUrl = 'https://hook.us2.make.com/h7ul56s43qdaq3cyn1orhu6797ilhs9i';
  const apiKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiI2NzYwODMxNjVkZjhiYWZhNDE2NDUyOWIiLCJuYW1lIjoiaW5mb0BleGNlbC1wcm9zLmNvbSIsImVtYWlsIjoiaW5mb0BleGNlbC1wcm9zLmNvbSIsInJvbGUiOiJBZG1pbiIsImNsaWVudElkIjoiIiwicHJvZmlsZUltYWdlIjoiaHR0cHM6Ly9maXJlYmFzZXN0b3JhZ2UuZ29vZ2xlYXBpcy5jb20vdjAvYi9jc20tYmUuZmlyZWJhc2VzdG9yYWdlLmFwcC9vL3VwbG9hZHMlMkYxNzUxNjEyNjcwNjM4X2ltYWdlX3BhZ2U0XzMucG5nP2FsdD1tZWRpYSZ0b2tlbj1lODU0NjFjYS02NDMzLTRhNTUtOTUyOS1iNDJiYWMxMzkxYmUiLCJjcmVhdGVkQXQiOiIyMDI0LTEyLTE2VDE5OjQ0OjIwLjk0NloiLCJpYXQiOj';

  let finalEntityId = entityId;

  if (!finalEntityId && responseData) {
    if (responseData?.id) {
      finalEntityId = responseData.id;
    } else if (responseData?._id) {
      finalEntityId = responseData._id;
    } else if (data?._id) {
      finalEntityId = data._id;
    }
  }

  let endpoint = `/${entityType}`;
  if (finalEntityId && (operation === 'PUT' || operation === 'DELETE')) {
    endpoint = `/${entityType}/${finalEntityId}`;
  }

  let dataWithId = { ...data };
  if (!dataWithId._id) {
    if (finalEntityId) {
      dataWithId._id = finalEntityId;
    } else if (responseData?.id) {
      dataWithId._id = responseData?.id;
    }
  }

  const webhookData = {
    request_type: operation,
    endpoint: endpoint,
    data: dataWithId
  };

  if (actiontype) {
    webhookData.actiontype = actiontype;
  }
  console.log(`Webhook Data:`, webhookData);

  try {
    const response = await axios.post(webhookUrl, webhookData, {
      headers: {
        'x-make-apikey': apiKey,
        'Content-Type': 'application/json'
      }
    });

    console.log(`✅ Webhook sent successfully for ${operation} ${entityType}${finalEntityId ? ` (ID: ${finalEntityId})` : ''}`);

    try {
      await saveWebhookLog('INFO', 'WEBHOOK', operation, entityType, finalEntityId, `Webhook API Response for ${operation} ${entityType}`, {
        status: response.status,
        statusText: response.statusText,
        responseData: response.data
      }, 'SUCCESS');
    } catch (logError) {
      console.error('Error saving webhook log:', logError.message);
    }

    return response.data;

  } catch (error) {
    console.error(`Error calling Make webhook for ${operation} ${entityType}:`, error.message);

    try {
      await saveWebhookLog('ERROR', 'WEBHOOK', operation, entityType, finalEntityId, `Error calling Make webhook for ${operation} ${entityType}`, {
        error: error.message,
        stack: error.stack
      }, 'FAILED');
    } catch (logError) {
      console.error('Error saving webhook error log:', logError.message);
    }

    return null;
  }
}

module.exports = {
  callMakeWebhook,
  saveWebhookLog
};

