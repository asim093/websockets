require("dotenv").config();
const getSchema = require("../utils/getSchema");
const validatePayload = require("../utils/validatePayload");
const hashSensitiveFields = require("../utils/hash");
const { MongoClient, ObjectId } = require("mongodb");

const client = new MongoClient(process.env.MONGODB_CONNECTION_STRING);

async function createEntity(entityType, data) {
  try {
    const schema = await getSchema(entityType);
    if (!schema)
      throw new Error(`No schema found for entity type: ${entityType}`);

    const errors = validatePayload(schema, data, true);
    if (errors.length > 0) {
      return { success: false, message: "Validation failed", errors };
    }

    data = await hashSensitiveFields(data, schema);

    const customFields = {};
    for (const field in schema.customFields) {
      if (data[field]) {
        customFields[field] = data[field];
        delete data[field];
      }
    }
    if (Object.keys(customFields).length > 0) {
      data.customFields = customFields;
    }

    const currentDate = new Date();
    data.createdAt = currentDate;
    data.updatedAt = currentDate;
    await client.connect();
    const database = client.db(process.env.DB_NAME);
    const collection = database.collection(entityType);

    const result = await collection.insertOne(data);
    const insertedDocument = await collection.findOne({
      _id: result.insertedId,
    });
    

    if (entityType === "Design" && insertedDocument?.requestId) {
     
      const requestCollection = database.collection("Request");
      const requestDocument = await requestCollection.findOne({
        _id: insertedDocument.requestId,
      });


    }

    return {
      success: true,
      message: "Entity created successfully",
      id: result.insertedId,
      entityType,
    };
  } catch (error) {
    console.error("Error creating entity:", error);
    return {
      success: false,
      message: "Error creating entity",
      error: error.message,
    };
  } finally {
    await client.close();
  }
}

module.exports = createEntity;