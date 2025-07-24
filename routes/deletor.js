const express = require("express");
const { MongoClient, ObjectId } = require("mongodb");

const router = express.Router();

router.delete("/Message/:id", async (req, res) => {
  try {
    console.log("🗑️ Deleting message:", req.params.id);
    
    const messageId = req.params.id;

    // Validate ObjectId
    if (!ObjectId.isValid(messageId)) {
      return res.status(400).json({ 
        success: false, 
        message: "Invalid message ID" 
      });
    }

    const client = new MongoClient(process.env.MONGODB_CONNECTION_STRING);
    
    try {
      await client.connect();
      const db = client.db(process.env.DB_NAME);
      const collection = db.collection("Message");

      // Get message info before deleting for potential socket broadcast
      const messageToDelete = await collection.findOne({ _id: new ObjectId(messageId) });
      
      const deleteResult = await collection.deleteOne({ _id: new ObjectId(messageId) });

      if (deleteResult.deletedCount === 1) {
        console.log("✅ Message deleted successfully");
        
        // Broadcast deletion to socket room if needed
        if (req.io && messageToDelete) {
          const roomName = `${messageToDelete.objectType}-${messageToDelete.object}`;
          req.io.to(roomName).emit("messageDeleted", {
            messageId: messageId,
            objectType: messageToDelete.objectType,
            object: messageToDelete.object.toString()
          });
          console.log("📡 Broadcasting message deletion to room:", roomName);
        }
        
        return res.status(200).json({ 
          success: true, 
          message: "Message deleted successfully" 
        });
      } else {
        return res.status(404).json({ 
          success: false, 
          message: "Message not found" 
        });
      }
    } catch (dbError) {
      console.error("💥 Database error:", dbError);
      return res.status(500).json({ 
        success: false, 
        message: dbError.message 
      });
    } finally {
      await client.close();
    }
  } catch (error) {
    console.error("💥 Error deleting message:", error);
    return res.status(500).json({ 
      success: false, 
      message: error.message 
    });
  }
});

module.exports = router;