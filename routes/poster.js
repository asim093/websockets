const express = require('express');
const authenticateToken = require('../utils/auth/token');
const createEntity = require('../EntityHandler/CREATE');
const { ObjectId } = require('mongodb');

const router = express.Router();
router.post('/Message', authenticateToken, async (req, res) => {
  try {
    const payload = {
      ...req.body,
      object: new ObjectId(req.body.object), // Ensure object is ObjectId
      sender: new ObjectId(req.body.sender), // Ensure sender is ObjectId
      createdAt: new Date()
    };

    const result = await createEntity("Message", payload);
    if (result.success) {
      // ✅ Emit socket event

      console.log("Emitting newMessage event to:", payload,req.body.object);
      
      req.io.to(req.body.object).emit("newMessage", result.message);
      return res.status(201).json(result);
    } else {
      return res.status(400).json(result);
    }
  } catch (error) {
    console.error("Error creating message:", error);
    return res.status(500).json({ success: false, message: error.message });
  }
});

router.post('/:type', authenticateToken, async (req, res) => {
  const { type } = req.params; 
  const result = await createEntity(type, req.body);
  if (result.success) {
    res.status(201).json(result);
  } else {  
    res.status(400).json(result);
  }
});

module.exports = router;
