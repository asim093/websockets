const { getAggregatedData } = require('../EntityHandler/READ');
const createEntity = require('../EntityHandler/CREATE');
const { MongoClient, ObjectId } = require('mongodb');
const admin = require("firebase-admin");
require('dotenv').config();


const fetchEntityDataRaw = async (entityType, objectId) => {
    const client = new MongoClient(process.env.MONGODB_CONNECTION_STRING);
    try {
        await client.connect();
        const db = client.db(process.env.DB_NAME);
        const collection = db.collection(entityType);
        const id = typeof objectId === 'string' && ObjectId.isValid(objectId)
            ? new ObjectId(objectId)
            : objectId;
        const doc = await collection.findOne({ _id: id });
        return doc || null;
    } finally {
        await client.close();
    }
};


const fetchTemplateByName = async (templateName) => {
    const client = new MongoClient(process.env.MONGODB_CONNECTION_STRING);
    try {
        await client.connect();
        const db = client.db(process.env.DB_NAME);
        const template = await db.collection("Notificationtemplates").findOne({ name: templateName, isActive: true });
        if (template) {
            console.log(` Template found: "${templateName}"`);
        } else {
            console.warn(`  Template not found or inactive: "${templateName}"`);
        }
        return template;
    } catch (error) {
        console.error(`Error fetching template "${templateName}":`, error);
        return null;
    } finally {
        await client.close();
    }
};


const applyTemplate = (template, vars = {}) => {
    const replace = (str = '') =>
        str.replace(/\{\{(\w+)\}\}/g, (_, key) => vars[key] ?? '');

    return {
        title:    replace(template.title),
        body:     replace(template.body),
        deeplink: replace(template.deeplink),
    };
};


const toRepIdString = (id) => {
    if (id == null) return null;
    if (typeof id === 'string') return id;
    if (id && typeof id === 'object' && id.toString) return id.toString();
    return String(id);
};

const extractRepIds = (entity, recipientsIds) => {
    const raw = entity?.repId ?? entity?.rep;
    if (raw == null) return;
    const ids = Array.isArray(raw) ? raw : [raw];
    for (const id of ids) {
        const sid = toRepIdString(id);
        if (sid && !recipientsIds.includes(sid)) {
            recipientsIds.push(sid);
        }
    }
};


const handleRequestType = async (objectId, recipientsIds) => {
    const requestData = await fetchEntityDataRaw("Request", objectId);
    if (!requestData) {
        console.warn("  Request not found:", objectId);
        return {};
    }
    console.log("📋 Request repId count:", Array.isArray(requestData.repId) ? requestData.repId.length : requestData.repId ? 1 : 0);
    extractRepIds(requestData, recipientsIds);
    return { request: requestData };
};

const handleDesignVersionType = async (objectId, recipientsIds) => {
    const designVersionData = await fetchEntityDataRaw("DesignVersion", objectId);
    if (!designVersionData?.designId) {
        console.warn("  DesignVersion not found or missing designId:", objectId);
        return {};
    }

    const designData = await fetchEntityDataRaw("Design", designVersionData.designId);
    if (!designData?.requestId) {
        console.warn("  Design not found or missing requestId:", designVersionData.designId);
        return { designVersion: designVersionData, design: designData };
    }

    const requestData = await fetchEntityDataRaw("Request", designData.requestId);
    extractRepIds(requestData, recipientsIds);
    return { designVersion: designVersionData, design: designData, request: requestData };
};

const handleSampleLineItemType = async (objectId, recipientsIds) => {
    const lineItemData = await fetchEntityDataRaw("lineItem", objectId);
    if (!lineItemData?.orderId) {
        console.warn("  lineItem not found or missing orderId:", objectId);
        return {};
    }

    const orderData = await fetchEntityDataRaw("Order", lineItemData.orderId);
    if (!orderData) {
        console.warn("  Order not found for lineItem:", lineItemData.orderId);
        return { lineItem: lineItemData };
    }

    // Support both "rep" and "repId" field names on Order document
    const orderReps = orderData.rep ?? orderData.repId;
    if (orderReps && Array.isArray(orderReps) && orderReps.length > 0) {
        extractRepIds(orderData, recipientsIds);
        console.log(`📋 SampleLineItem → Order (${lineItemData.orderId}) → ${orderReps.length} rep(s) found`);
        return { lineItem: lineItemData, order: orderData };
    }

    if (orderData.requestId) {
        const requestData = await fetchEntityDataRaw("Request", orderData.requestId);
        extractRepIds(requestData, recipientsIds);
        return { lineItem: lineItemData, order: orderData, request: requestData };
    }

    console.warn("  No rep/repId found on Order or linked Request for SampleLineItem:", objectId);
    return { lineItem: lineItemData, order: orderData };
};





const TEMPLATE_CONFIG = {
    "Request": {
        templateName: "clientRequestMessage",
        buildVars: (entityData, objectId, deeplink) => ({
            name:        entityData.request?.name || entityData.request?.title || "Request",
            requestName: entityData.request?.name || entityData.request?.title || "Request",
            link:        deeplink,
            objectId,
        }),
    },
    "DesignVersion": {
        templateName: "clientDesignVersionMessage",
        buildVars: (entityData, objectId, deeplink) => ({
            name:          entityData.design?.name || "Design",
            designName:    entityData.design?.name || "Design",
            versionNumber: entityData.designVersion?.versionNumber || "1",
            link:          deeplink,
            objectId,
        }),
    },
    "SampleLineItem": {
        templateName: "clientSampleLineItemMessage",
        buildVars: (entityData, objectId, deeplink) => ({
            name:      entityData.lineItem?.name || entityData.order?.name || "Sample",
            orderName: entityData.order?.name    || "Order",
            itemName:  entityData.lineItem?.name || "Line Item",
            link:      deeplink,
            objectId,
        }),
    },
};


const getDeeplink = (objectType, objectId) => {
    const baseUrl = "https://csm-be.web.app";
    const deeplinkMap = {
        "Request":        `${baseUrl}/ViewRequest/${objectId}`,
        "DesignVersion":  `${baseUrl}/Viewdesign/${objectId}`,
        "SampleLineItem": `${baseUrl}/SampleDetail/${objectId}`,
    };
    return deeplinkMap[objectType] || baseUrl;
};


const fetchUsersFromRepIds = async (repIds) => {
    if (!repIds || repIds.length === 0) return [];

    const uniqueIds = [...new Set(repIds.map(id => toRepIdString(id)).filter(Boolean))];
    const objectIds = [];
    for (const id of uniqueIds) {
        try {
            if (ObjectId.isValid(id)) objectIds.push(new ObjectId(id));
        } catch (e) {
            console.warn("Invalid repId skipped:", id, e.message);
        }
    }

    if (objectIds.length === 0) return [];

    try {
        const result = await getAggregatedData({
            entityType: "User",
            filter: { _id: { $in: objectIds } },
            pagination: { page: 1, pageSize: Math.max(objectIds.length, 500) }
        });
        const users = result?.data || [];
        console.log(`👤 Users fetched: ${uniqueIds.length} repIds → ${users.length} users`);
        return users;
    } catch (error) {
        console.error("Error fetching users from repIds:", error);
        return [];
    }
};


const sendFirebaseNotification = async (token, notification) => {
    if (!token) return false;

    try {
        if (!admin.apps.length) {
            const path = require('path');
            const firebaseConfigPath = path.join(__dirname, '../../csm-be-latest/api/sendar/csm-be-firebase-adminsdk-fbsvc-62b8bff808.json');
            try {
                const firebaseConfig = require(firebaseConfigPath);
                admin.initializeApp({ credential: admin.credential.cert(firebaseConfig) });
            } catch (initError) {
                console.error("Firebase init error:", initError);
                return false;
            }
        }

        const response = await admin.messaging().send({
            notification: { title: notification.title, body: notification.body },
            token,
        });
        console.log(" Firebase push sent:", response);
        return true;
    } catch (error) {
        if (error.code === "messaging/message-rate-exceeded") {
            console.error("Rate limit exceeded for token:", token);
        } else if (["messaging/invalid-registration-token", "messaging/registration-token-not-registered"].includes(error.code)) {
            console.error("Invalid/unregistered token:", token);
        } else {
            console.error("Firebase error:", error);
        }
        return false;
    }
};


const sendNotificationtoreps = async (senderRole, objectType, objectId, io = null) => {
    const recipientsIds = [];

    if (senderRole !== "Client" || !objectType || !objectId) {
        return recipientsIds;
    }

    const handlers = {
        "Request":        handleRequestType,
        "DesignVersion":  handleDesignVersionType,
        "SampleLineItem": handleSampleLineItemType,
    };

    const handler = handlers[objectType];
    let entityData = {};

    if (handler) {
        entityData = await handler(objectId, recipientsIds) || {};
    } else {
        console.warn(`  No handler registered for objectType: "${objectType}"`);
    }

    if (recipientsIds.length === 0) {
        console.log(`ℹ️  No repIds found for ${objectType} → ${objectId}. Notification skipped.`);
        return recipientsIds;
    }

    try {
        console.log(`📋 repIds collected: ${recipientsIds.length}`, recipientsIds);

        const users = await fetchUsersFromRepIds(recipientsIds);
        if (users.length === 0) {
            console.log("No users found for repIds:", recipientsIds);
            return recipientsIds;
        }

        const receivers = users.map(user => ({
            userId: new ObjectId(user._id),
            read:   false
        }));

        if (receivers.length !== recipientsIds.length) {
            console.warn(`  receivers (${receivers.length}) vs repIds (${recipientsIds.length}) mismatch`);
        }

        const config    = TEMPLATE_CONFIG[objectType];
        const deeplink  = getDeeplink(objectType, objectId);
        let notifContent = null;

        if (config) {
            const template = await fetchTemplateByName(config.templateName);
            if (template) {
                const vars   = config.buildVars(entityData, objectId, deeplink);
                notifContent = applyTemplate(template, vars);
                console.log(`📨 Template "${config.templateName}" applied → "${notifContent.title}"`);
            }
        }

        if (!notifContent) {
            console.warn(`  Using fallback notification for objectType: ${objectType}`);
            notifContent = {
                title:    "New Message from Client",
                body:     `You have received a new message in ${objectType} from the client. Please check for updates.`,
                deeplink: deeplink,
            };
        }

        const notificationData = {
            title:     notifContent.title,
            body:      notifContent.body,
            deeplink:  notifContent.deeplink,
            sender:    null,
            receivers: [...receivers],
            data: {
                objectType,
                objectId,
            }
        };

        const createdNotification = await createEntity("Notification", notificationData);
        console.log(` Notification saved — ${receivers.length} receivers, type: ${objectType}, id: ${objectId}`);

        const pushPromises = users.map(async (user) => {
            if (user.token) {
                await sendFirebaseNotification(user.token, notificationData);
            }
        });
        await Promise.allSettled(pushPromises);

        if (io) {
            const notificationToEmit = {
                ...notificationData,
                _id:       createdNotification?.id || createdNotification?._id,
                createdAt: new Date().toISOString(),
                receivers: receivers.map(r => ({
                    userId: r.userId.toString(),
                    read:   r.read
                }))
            };

            receivers.forEach((receiver) => {
                io.to(`user-${receiver.userId.toString()}`).emit("newNotification", notificationToEmit);
            });

            console.log(`📡 WebSocket emitted to ${receivers.length} users`);
        }

        return recipientsIds;
    } catch (error) {
        console.error("Error in sendNotificationtoreps:", error);
        return recipientsIds;
    }
};

module.exports = sendNotificationtoreps;
