const { MongoClient, ObjectId } = require('mongodb');
require('dotenv').config();
const client = new MongoClient(process.env.MONGODB_CONNECTION_STRING);

async function deleteEntity(entityType, id) {
  try {
    await client.connect();
    const database = client.db(process.env.DB_NAME);
    const collection = database.collection(entityType);

    const cleanedId = id.trim();
    const objectId = ObjectId.isValid(cleanedId) ? new ObjectId(cleanedId) : null;

    if (!objectId) {
      return { success: false, message: `Invalid ID format: '${id}'` }; 
    }

    // Attempt to delete the document
    const result = await collection.deleteOne({ _id: objectId });

    if (result.deletedCount === 1) {
      return { success: true, message: `Entity with ID ${id} deleted successfully.` };
    } else {
      return { success: false, message: `No entity found with ID ${id}.` };
    }
  } catch (error) {
    console.error("Error deleting entity:", error);
    return { success: false, message: "Error deleting entity", error: error.message };
  } finally {
    await client.close();
  }
}

async function deleteArrayItem(entityType, id, field, data) {
  try {
    await client.connect();
    const database = client.db(process.env.DB_NAME);
    const collection = database.collection(entityType);

    // Check if `id` is a valid ObjectId string
    const objectId = ObjectId.isValid(id) ? new ObjectId(String(id)) : id;

    // Attempt to delete the document
    const result = await collection.updateOne(
        { _id: objectId },
        { $pull: { [field]: data } }  // Remove the designerId from the designers array
    );
    if (result.modifiedCount === 1) {
      return { success: true, message: `Data Removed from Entity with ID ${id}.` };
    } else {
      return { success: false, message: `No entity found with ID ${id}.` };
    }
  } catch (error) {
    console.error("Error deleting entity:", error);
    return { success: false, message: "Error deleting entity", error: error.message };
  } finally {
    await client.close();
  }
}

async function deleteChart(dashboardId, chartPos) {  

  try {
    return deleteArrayItem("Dashboard", dashboardId, "charts", { pos: chartPos.toString().trim() });
    

  } catch (error) {
    console.error("Error deleting chart:", error);
    return { success: false, message: "Internal server error" };
  } finally {
    await client.close();
  }
}

async function deleteFilter(dashboardId, chartPos) {  

  try {
    return deleteArrayItem("Dashboard", dashboardId, "filters", { pos: chartPos.toString().trim() });
    

  } catch (error) {
    console.error("Error deleting chart:", error);
    return { success: false, message: "Internal server error" };
  } finally {
    await client.close();
  }
}



module.exports = {deleteEntity, deleteArrayItem,deleteChart,deleteFilter};