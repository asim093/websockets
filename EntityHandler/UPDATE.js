const getSchema = require("../utils/getSchema");
const validatePayload = require("../utils/validatePayload");
const hashSensitiveFields = require("../utils/hash");
const { MongoClient, ObjectId } = require("mongodb");
const processDocumentAndSendMail = require("../../sendar/processdocumentandsendmail");
const client = new MongoClient(process.env.MONGODB_CONNECTION_STRING);

async function updateEntity(entityType, id, data, action = "replace") {
  try {
    // Step 1: Retrieve schema for the Entity
    const schema = await getSchema(entityType);
    if (!schema) {
      throw new Error(`No schema found for entity type: ${entityType}`);
    }

    // Step 2: Validate payload
    const errors = validatePayload(schema, data, false);
    if (errors.length > 0) {
      return { success: false, message: "Validation failed", errors };
    }

    // Step 3: Hash Sensitive Fields
    data = await hashSensitiveFields(data, schema);

    // Step 4: Map custom fields
    const customFields = {};
    for (const field in schema.customFields) {
      if (data[field]) {
        customFields[field] = data[field];
        delete data[field]; // Remove from main data object
      }
    }
    if (Object.keys(customFields).length > 0) {
      data.customFields = customFields;
    }

    // Step 5: Update `updatedAt` timestamp
    data.updatedAt = new Date();

    // Step 6: Connect to the database and prepare update payload
    await client.connect();
    const database = client.db(process.env.DB_NAME);
    const collection = database.collection(entityType);
    const userCollection = database.collection("User"); // User collection

    // Convert id to ObjectId if valid
    const objectId = ObjectId.isValid(id) ? new ObjectId(String(id)) : id;

    // Step 7: Prepare update operation
    let updateData = {};
    let Userid; // For passing to mail function (array or string)

    if (action === "append") {
      let pushData = {};
      const allFields = { ...schema.basicFields, ...schema.customFields };

      // Check and prepare $push for array fields in main data
      for (const field in data) {
        const expectedType = allFields[field];
        if (expectedType === "array" && Array.isArray(data[field])) {
          Userid = data[field]; // Assign for mail
          pushData[field] = { $each: data[field] };
          delete data[field];
        }
      }

      // Check and prepare $push for array fields in customFields
      if (data.customFields) {
        for (const field in data.customFields) {
          const expectedType = allFields[field];
          if (
            expectedType === "array" &&
            Array.isArray(data.customFields[field])
          ) {
            Userid = data.customFields[field]; // Assign for mail
            pushData[field] = { $each: data.customFields[field] };
            delete data.customFields[field];
          }
        }
      }

      updateData = { $push: pushData, $set: data };

      // Populate User documents from IDs before sending mail
      let userDocs = [];
      if (Array.isArray(Userid)) {
        const objectIds = Userid.filter(ObjectId.isValid).map(
          (id) => new ObjectId(id)
        );
        userDocs = await userCollection
          .find({ _id: { $in: objectIds } })
          .toArray();
      } else if (Userid && ObjectId.isValid(String(Userid))) {
        const userDoc = await userCollection.findOne({
          _id: new ObjectId(String(Userid)),
        });
        if (userDoc) userDocs.push(userDoc);
      }

      console.log(entityType, id, Userid);
      await processDocumentAndSendMail({
        schemaName: entityType,
        docId: id,
        userId: Userid,
        mailDesignName: "designer_access",
        fieldToPopulate: "designers",
      });

    } else {
      // For replace or other actions
      updateData = { $set: data };

      Userid =
        data.designers || (data.customFields && data.customFields.designers);
      // Populate User documents from IDs before sending mail
      let userDocs = [];
      if (Array.isArray(Userid)) {
        const objectIds = Userid.filter(ObjectId.isValid).map(
          (id) => new ObjectId(id)
        );
        userDocs = await userCollection
          .find({ _id: { $in: objectIds } })
          .toArray();
      } else if (Userid && ObjectId.isValid(String(Userid))) {
        const userDoc = await userCollection.findOne({
          _id: new ObjectId(String(Userid)),
        });
        if (userDoc) userDocs.push(userDoc);
      }
    }

    const result = await collection.updateOne({ _id: objectId }, updateData);

    if (result.modifiedCount === 1) {
      return {
        success: true,
        message: "Entity updated successfully",
        id: objectId,
      };
    } else {
      return {
        success: false,
        message: "No entity found with the provided ID.",
      };
    }
  } catch (error) {
    console.error("Error updating entity:", error);
    return {
      success: false,
      message: "Error updating entity",
      error: error.message,
    };
  } finally {
    await client.close();
  }
}

module.exports = updateEntity;