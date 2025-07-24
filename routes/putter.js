const express = require('express');
const authenticateToken = require('../utils/auth/token');
const updateEntity = require('../EntityHandler/UPDATE');
const { ObjectId, MongoClient } = require('mongodb');

const router = express.Router();

// Add debugging middleware
router.use((req, res, next) => {
  console.log(`🔍 PUTTER.JS - ${req.method} ${req.path}`);
  next();
});


router.put("/Message/:id/visibility", async (req, res) => {
  console.log("🎯 VISIBILITY ROUTE HIT:", req.params.id);
  
  try {
    console.log("🔄 Updating message visibility:", req.params.id, req.body);

    const messageId = req.params.id;
    const { targetRole, visibleToRoles } = req.body.data;

    if (!ObjectId.isValid(messageId)) {
      return res.status(400).json({
        success: false,
        message: "Invalid message ID"
      });
    }

    const client = new MongoClient(process.env.MONGODB_CONNECTION_STRING);
    await client.connect();
    const db = client.db(process.env.DB_NAME);
    const collection = db.collection("Message");

    const currentMessage = await collection.findOne({ _id: new ObjectId(messageId) });

    if (!currentMessage) {
      await client.close();
      return res.status(404).json({
        success: false,
        message: "Message not found"
      });
    }

    const updateResult = await collection.updateOne(
      { _id: new ObjectId(messageId) },
      {
        $set: {
          targetRole: targetRole,
          visibleToRoles: visibleToRoles,
          updatedAt: new Date()
        }
      }
    );

    if (updateResult.modifiedCount === 1) {
      const updatedMessage = await collection.findOne({ _id: new ObjectId(messageId) });
      await client.close();

      // Emit visibility change if socket.io is available
      if (req.io) {
        const roomName = `${currentMessage.objectType}-${currentMessage.object}`;
        const visibilityChangeData = {
          messageId: messageId,
          targetRole: targetRole,
          visibleToRoles: visibleToRoles,
          objectType: currentMessage.objectType,
          object: currentMessage.object.toString()
        };
        req.io.to(roomName).emit("messageVisibilityChanged", visibilityChangeData);
        console.log("🔄 Broadcasting visibility change to room:", roomName);
      }

      console.log("✅ Message visibility updated successfully");
      return res.status(200).json({
        success: true,
        message: "Visibility updated successfully",
        data: updatedMessage
      });
    } else {
      await client.close();
      return res.status(404).json({
        success: false,
        message: "Message not found or no changes made"
      });
    }
  } catch (error) {
    console.error("💥 Error updating message visibility:", error);
    return res.status(500).json({
      success: false,
      message: error.message
    });
  }
});

// This route handles /Message/:id (regular message updates)
router.put("/Message/:id", async (req, res) => {
  console.log("🎯 REGULAR MESSAGE UPDATE ROUTE HIT:", req.params.id);
  
  try {
    console.log("📝 Updating message:", req.params.id, req.body);
    
    const messageId = req.params.id;
    const updateData = req.body.data;
    
    if (!ObjectId.isValid(messageId)) {
      return res.status(400).json({ 
        success: false, 
        message: "Invalid message ID" 
      });
    }
    
    const client = new MongoClient(process.env.MONGODB_CONNECTION_STRING);
    await client.connect();
    const db = client.db(process.env.DB_NAME);
    const collection = db.collection("Message");
    
    const updateResult = await collection.updateOne(
      { _id: new ObjectId(messageId) },
      { 
        $set: { 
          ...updateData,
          updatedAt: new Date()
        } 
      }
    );
    
    await client.close();
    
    if (updateResult.modifiedCount === 1) {
      console.log("✅ Message updated successfully");
      return res.status(200).json({ 
        success: true, 
        message: "Message updated successfully"
      });
    } else {
      return res.status(404).json({ 
        success: false, 
        message: "Message not found or no changes made" 
      });
    }
  } catch (error) {
    console.error("💥 Error updating message:", error);
    return res.status(500).json({ 
      success: false, 
      message: error.message 
    });
  }
});

// Generic routes come LAST
router.put('/', authenticateToken, async (req, res) => {
  console.log("🎯 GENERIC ROOT PUT ROUTE HIT");
  const result = await updateEntity(req.body.type, req.body.id, req.body.data, req.body?.action);
  res.json(result);
});

router.put('/:type/:id', authenticateToken, async (req, res) => {
  console.log("🎯 GENERIC TYPE/ID PUT ROUTE HIT:", req.params.type, req.params.id);
  const { type, id } = req.params;
  const result = await updateEntity(type, id, req.body.data, req.body?.action);
  res.json(result);
});

// Add a test route to verify the router is working
router.get("/test-putter", (req, res) => {
  res.json({ 
    message: "Putter router is working!",
    timestamp: new Date().toISOString()
  });
});

module.exports = router;