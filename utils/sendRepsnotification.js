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


const toRepIdString = (id) => {
    if (id == null) return null;
    if (typeof id === 'string') return id;
    if (id && typeof id === 'object' && id.toString) return id.toString();
    return String(id);
};

const extractRepIds = (entity, recipientsIds) => {
    const raw = entity?.repId;
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
    if (requestData) {
        console.log("📋 Request raw repId:", Array.isArray(requestData.repId) ? requestData.repId.length : requestData.repId ? 1 : 0, JSON.stringify(requestData.repId));
    }
    extractRepIds(requestData, recipientsIds);
};


const handleDesignVersionType = async (objectId, recipientsIds) => {
    const designVersionData = await fetchEntityDataRaw("DesignVersion", objectId);
    if (!designVersionData?.designId) return;

    const designData = await fetchEntityDataRaw("Design", designVersionData.designId);
    if (!designData?.requestId) return;

    const requestData = await fetchEntityDataRaw("Request", designData.requestId);
    extractRepIds(requestData, recipientsIds);
};


const handleSampleLineItemType = async (objectId, recipientsIds) => {
    const lineItemData = await fetchEntityDataRaw("lineItem", objectId);
    if (!lineItemData?.orderId) return;

    const orderData = await fetchEntityDataRaw("Order", lineItemData.orderId);
    if (orderData?.repId && Array.isArray(orderData.repId) && orderData.repId.length > 0) {
        extractRepIds(orderData, recipientsIds);
        return;
    }
    if (orderData?.requestId) {
        const requestData = await fetchEntityDataRaw("Request", orderData.requestId);
        extractRepIds(requestData, recipientsIds);
    }
};


const handleOrderType = async (objectId, recipientsIds) => {
    const orderData = await fetchEntityDataRaw("Order", objectId);
    if (!orderData) return;

    if (orderData.repId && Array.isArray(orderData.repId) && orderData.repId.length > 0) {
        extractRepIds(orderData, recipientsIds);
        return;
    }
    if (orderData.requestId) {
        const requestData = await fetchEntityDataRaw("Request", orderData.requestId);
        extractRepIds(requestData, recipientsIds);
    }
};


const getDeeplink = (objectType, objectId) => {
    const baseUrl = "https://csm-be.web.app";
    const deeplinkMap = {
        "Request": `${baseUrl}/ViewRequest/${objectId}`,
        "DesignVersion": `${baseUrl}/Viewdesign/${objectId}`,
        "SampleLineItem": `${baseUrl}/SampleDetail/${objectId}`,
    };
    return deeplinkMap[objectType] || `${baseUrl}`;
};

const fetchUsersFromRepIds = async (repIds) => {
    if (!repIds || repIds.length === 0) {
        return [];
    }

    const uniqueIds = [...new Set(repIds.map(id => toRepIdString(id)).filter(Boolean))];
    const objectIds = [];
    for (const id of uniqueIds) {
        try {
            if (ObjectId.isValid(id)) {
                objectIds.push(new ObjectId(id));
            }
        } catch (e) {
            console.warn("Invalid repId skipped:", id, e.message);
        }
    }

    if (objectIds.length === 0) {
        return [];
    }

    try {
        const result = await getAggregatedData({
            entityType: "User",
            filter: {
                _id: { $in: objectIds }
            },
            pagination: { page: 1, pageSize: Math.max(objectIds.length, 500) }
        });
        const users = result?.data || [];
        console.log("Users fetched from repIds:", uniqueIds.length, "ids ->", users.length, "users");
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
                admin.initializeApp({
                    credential: admin.credential.cert(firebaseConfig),
                });
            } catch (initError) {
                console.error("Firebase initialization error:", initError);
                return false;
            }
        }

        const message = {
            notification: { 
                title: notification.title, 
                body: notification.body 
            },
            token: token,
        };

        const response = await admin.messaging().send(message);
        console.log("✅ Firebase notification sent successfully:", response);
        return true;
    } catch (error) {
        if (error.code === "messaging/message-rate-exceeded") {
            console.error("Rate limit exceeded for token:", token);
        } else if (error.code === "messaging/invalid-registration-token" || error.code === "messaging/registration-token-not-registered") {
            console.error("Invalid or unregistered token:", token);
        } else {
            console.error("Error sending Firebase notification:", error);
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
        "Request": handleRequestType,
        "DesignVersion": handleDesignVersionType,
        "SampleLineItem": handleSampleLineItemType,
        "Order": handleOrderType
    };

    const handler = handlers[objectType];
    if (handler) {
        await handler(objectId, recipientsIds);
    }

    if (recipientsIds.length > 0) {
        try {
            console.log(`📋 repIds collected: ${recipientsIds.length}`, recipientsIds);

            const users = await fetchUsersFromRepIds(recipientsIds);

            if (users.length === 0) {
                console.log("No users found for repIds:", recipientsIds);
                return recipientsIds;
            }

            const receivers = users.map(user => ({
                userId: new ObjectId(user._id),
                read: false
            }));

            if (receivers.length !== recipientsIds.length) {
                console.warn(`⚠️ receivers count (${receivers.length}) differs from repIds count (${recipientsIds.length}) - some users may not exist in DB`);
            }

            const deeplink = getDeeplink(objectType, objectId);

            const notificationData = {
                title: "New Message from Client",
                body: `You have received a new message in ${objectType} from the client. Please check for updates.`,
                deeplink: deeplink,
                sender: null,
                receivers: [...receivers],
                data: {}
            };

            const createdNotification = await createEntity("Notification", notificationData);
            console.log(`✅ Notification created for ${receivers.length} receivers - ObjectType: ${objectType}, ObjectId: ${objectId}`);

            const pushPromises = users.map(async (user) => {
                if (user.token) {
                    await sendFirebaseNotification(user.token, notificationData);
                }
            });
            await Promise.allSettled(pushPromises);

            if (io) {
                const notificationToEmit = {
                    ...notificationData,
                    _id: createdNotification.id || createdNotification._id,
                    createdAt: new Date().toISOString(),
                    receivers: receivers.map(receiver => ({
                        userId: receiver.userId.toString(),
                        read: receiver.read
                    }))
                };
                
                receivers.forEach((receiver) => {
                    const userId = receiver.userId.toString();
                    io.to(`user-${userId}`).emit("newNotification", notificationToEmit);
                });
                
                console.log(`📡 Websocket notification emitted to ${receivers.length} users`);
            }

            return recipientsIds;
        } catch (error) {
            console.error("Error creating notification:", error);
            return recipientsIds;
        }
    }

    return recipientsIds;
};

module.exports = sendNotificationtoreps;
