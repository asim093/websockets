const { MongoClient } = require('mongodb');
const client = new MongoClient(process.env.MONGODB_URI);

async function getSchema(entityType) {
  try {
    await client.connect();
    const database = client.db(process.env.DB_NAME);
    const schemaCollection = database.collection('Schema');

    // Retrieve schema for the given entity type
    const schema = await schemaCollection.findOne({ entity: entityType });
    return schema;
  } catch (error) {
    console.error("Error fetching schema:", error);
    throw new Error("Schema not found");
  }
}

module.exports = getSchema;