const getSchema = require("../utils/getSchema");
const { MongoClient, ObjectId } = require("mongodb");

async function getAggregatedData(request) {
  const {
    entityType,
    filter,
    sort = [],
    $project,
    $lookup,
    $unwind,
    pagination = { page: 1, pageSize: 100 },
  } = request;

  const client = new MongoClient(process.env.MONGODB_CONNECTION_STRING);

  const schema = await getSchema(entityType);

  const pipeline = [];
  const matchStage = { $match: {} };

  if (filter) {
    Object.keys(filter).forEach((field) => {
      let value = filter[field];
      let isCustomField = false;

      // ✅ Check custom field
      if (schema?.customFields && schema?.customFields[field]) {
        isCustomField = true;
      }

      if (isCustomField) {
        field = "customFields." + field;
      }

      const rawField = isCustomField
        ? field.replace("customFields.", "")
        : field;

      if (
        schema?.dateFields &&
        schema?.dateFields?.includes(
          isCustomField ? field.replace("customFields.", "") : field
        )
      ) {
        if (value.$gte || value.$lte || value.$in) {
          if (typeof value.$gte === "string") value.$gte = new Date(value.$gte);
          if (typeof value.$lte === "string") value.$lte = new Date(value.$lte);
          matchStage.$match[field] = value;
        }
      } else if (Array.isArray(value)) {
        matchStage.$match[field] = { $in: value };
      } else if (
        field === "_id" ||
        schema.basicFields?.[rawField] === "ObjectId"
      ) {
        if (typeof value === "object" && value !== null) {
          if (value.$eq && ObjectId.isValid(value.$eq)) {
            value.$eq = new ObjectId(value.$eq);
          }
          if (value.$in && Array.isArray(value.$in)) {
            value.$in = value.$in
              .filter((v) => ObjectId.isValid(v))
              .map((v) => new ObjectId(v));
          }
          matchStage.$match[field] = value;
        } else if (ObjectId.isValid(value)) {
          matchStage.$match[field] = { $eq: new ObjectId(value) };
        } else {
          console.warn(`Invalid ObjectId for field ${field}:`, value);
        }
      } else {
        matchStage.$match[field] = value;
      }
    });
  }

  pipeline.push(matchStage);

  if (sort.length > 0) {
    const sortStage = { $sort: {} };
    sort.forEach((item) => {
      let [field, direction] = Object.entries(item)[0];
      if (schema.customFields?.[field]) {
        field = "customFields." + field;
      }
      sortStage.$sort[field] = direction === "desc" ? -1 : 1;
    });
    pipeline.push(sortStage);
  }

  if ($lookup) pipeline.push($lookup);
  if ($unwind) pipeline.push($unwind);

  if ($project) {
    console.log("Project within Request:", $project);
    pipeline.push($project);
  } else {
    console.log(
      "No project within Request, looking for defaultProject from schema:",
      schema?.defaultProject
    );
    if (schema && "defaultProject" in schema) {
      pipeline.push({ $project: schema.defaultProject });
      console.log("Default project:", schema.defaultProject);
    }
  }

  pipeline.push({ $skip: (pagination.page - 1) * pagination.pageSize });
  pipeline.push({ $limit: pagination.pageSize });

  console.log("pipeline", pipeline);

  try {
    await client.connect();
    const database = client.db(process.env.DB_NAME);
    const collection = database.collection(entityType);
    const results = await collection.aggregate(pipeline).toArray();
    return { data: results };
  } catch (err) {
    console.error("Error during aggregation:", err);
    throw err;
  } finally {
    if (client) {
      await client.close();
    }
  }
}

async function runQuery(entityType, id, query, filters = false) {
  const client = new MongoClient(process.env.MONGODB_CONNECTION_STRING);
  try {
    query.unshift({ $match: { _id: { $eq: id } } });

    if (filters) {
      const unwindIndex = query.findIndex((stage) => stage["$unwind"]);
      const matchIndex = query.findIndex(
        (stage, index) => index > unwindIndex && stage["$match"]
      );

      if (matchIndex !== -1) {
        const existingMatch = query[matchIndex]["$match"];
        for (const [key, value] of Object.entries(filters["$match"])) {
          if (!existingMatch[key]) {
            existingMatch[key] = value;
          }
        }
      } else {
        query.splice(unwindIndex + 1, 0, filters);
      }
    }

    console.log("query sent to MongoDB:", JSON.stringify(query, null, 2));

    await client.connect();
    const database = client.db(process.env.DB_NAME);
    const collection = database.collection(entityType);
    const results = await collection.aggregate(query).toArray();
    return { data: results };
  } catch (err) {
    console.error("Error during aggregation:", err);
    throw err;
  } finally {
    if (client) {
      await client.close();
    }
  }
}

module.exports = { getAggregatedData, runQuery };