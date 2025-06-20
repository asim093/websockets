
const { MongoClient, ObjectId } = require("mongodb");
const getSchema = require("../utils/getSchema"); // adjust path as needed 
const safeParseJSON = require("../utils/parseJson");

async function globalSearch(request) {
  const {
    entityType,
    searchTerm,
    searchFields = [],
    filter,
    sort = [],
    $project,
    $lookup,
    $unwind,
    pagination = { page: 1, pageSize: 100 },
  } = request;

  const client = new MongoClient(process.env.MONGODB_CONNECTION_STRING);
  const pipeline = [];

  try {
    await client.connect();
    const schema = await getSchema(entityType);

    console.log("Schema:", schema);

    const matchStage = { $match: {} };

    let parsedFilter = safeParseJSON(filter, {})
    let parsedSearchFields = safeParseJSON(searchFields, [])
    if (searchTerm && searchTerm.trim() !== "") {
      const orConditions = [];
      console.log("Search Term:", searchFields);
      const searchableFields =
      parsedSearchFields.length > 0
          ? parsedSearchFields
          : [...Object.keys(schema.basicFields || {}), ...Object.keys(schema.customFields || {})];

      for (const field of searchableFields) {
        const dbField = schema.customFields?.[field]
          ? `customFields.${field}`
          : field;

        if (field === "_id") {
          try {
            orConditions.push({ _id: new ObjectId(searchTerm) });
          } catch (e) {
            console.error("Invalid ObjectId format:", searchTerm);
          }
        } else {
          orConditions.push({
            [dbField]: { $regex: searchTerm, $options: "i" },
          });
        }
      }

      if (orConditions.length > 0) {
        matchStage.$match.$or = orConditions;
      }
    }

    if (parsedFilter) {
      Object.keys(parsedFilter).forEach((field) => {
        let value = parsedFilter[field];
        let isCustomField = schema.customFields?.[field];
        let dbField = isCustomField ? `customFields.${field}` : field;

        if (
          schema.dateFields &&
          schema.dateFields.includes(isCustomField ? field : dbField)
        ) {
          if (value.$gte || value.$lte || value.$in) {
            if (typeof value.$gte === "string") value.$gte = new Date(value.$gte);
            if (typeof value.$lte === "string") value.$lte = new Date(value.$lte);
            matchStage.$match[dbField] = value;
          }
        } else if (Array.isArray(value)) {
          matchStage.$match[dbField] = { $in: value };
        } else if (field === "_id" || schema.basicFields?.[field] === "ObjectId") {
          matchStage.$match[dbField] = { $eq: new ObjectId(String(value)) };
        } else {
          matchStage.$match[dbField] = value;
        }
      });
    }

    pipeline.push(matchStage);

    if ($lookup) pipeline.push($lookup);
    if ($unwind) pipeline.push($unwind);

    if (sort.length > 0) {
      const sortStage = { $sort: {} };
      sort.forEach((item) => {
        let [field, direction] = Object.entries(item)[0];
        if (schema.customFields?.[field]) {
          field = `customFields.${field}`;
        }
        sortStage.$sort[field] = direction === "desc" ? -1 : 1;
      });
      pipeline.push(sortStage);
    }

    if ($project) {
      pipeline.push($project);
    } else if (schema.defaultProject) {
      pipeline.push({ $project: schema.defaultProject });
    }

    pipeline.push({ $skip: (pagination.page - 1) * pagination.pageSize });
    pipeline.push({ $limit: pagination.pageSize });

    console.log("Global Search Pipeline:", JSON.stringify(pipeline, null, 2));

    const database = client.db(process.env.DB_NAME);
    const collection = database.collection(entityType);
    const results = await collection.aggregate(pipeline).toArray();

    return { data: results };
  } catch (err) {
    console.error("Global Search Error:", err);
    throw err;
  } finally {
    await client.close();
  }
}

module.exports = {globalSearch};

