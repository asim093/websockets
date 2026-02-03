const express = require("express");
const authenticateToken = require("../utils/auth/token");
const createEntity = require("../EntityHandler/CREATE");
const { ObjectId, MongoClient } = require("mongodb");
const { getUserRole } = require("../utils/getuserRole");
const sendNotificationtoreps = require("../utils/sendRepsnotification");

const router = express.Router();

const calculateVisibleToRoles = (senderRole, targetRole) => {
  let visibleToRoles = [];

  if (senderRole) {
    visibleToRoles.push(senderRole);
  }

  const targetRoles = Array.isArray(targetRole) ? targetRole : [targetRole];

  if (!targetRole) {
    visibleToRoles.push("Admin");
  }

  if (targetRoles.includes("All")) {
    return ["Admin", "Client", "Designer"];
  }

  if (targetRoles.includes("Internal")) {
    return [senderRole];
  }

  targetRoles.forEach((role) => {
    if (role && !visibleToRoles.includes(role)) {
      visibleToRoles.push(role);
    }
  });

  return [...new Set(visibleToRoles)];
};

router.post("/Message", async (req, res) => {
  try {
    console.log("📨 Incoming message request:", req.body);

    const senderRole = await getUserRole(req.body.sender);
    if (!senderRole) {
      return res
        .status(400)
        .json({ success: false, message: "Invalid sender" });
    }

    sendNotificationtoreps(senderRole, req.body.objectType, req.body.object, req.io).catch((err) => {
      console.error("sendNotificationtoreps error:", err);
    });

    const visibleToRoles = calculateVisibleToRoles(
      senderRole,
      req.body.targetRole
    );
    console.log("👥 Visible to roles:", visibleToRoles);

    const payload = {
      ...req.body,
      object: new ObjectId(req.body.object),
      sender: new ObjectId(req.body.sender),
      senderRole: senderRole,
      visibleToRoles: visibleToRoles,
      readBy: [],
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    const result = await createEntity("Message", payload);
    console.log("Database result:", result);

    if (result.success) {
      console.log("✅ Message saved to DB:", result.data?._id);

      const roomName = `${req.body.objectType}-${req.body.object}`;
      console.log("🏠 Emitting to room:", roomName);

      const socketsInRoom = await req.io.in(roomName).fetchSockets();
      console.log(
        `📡 Found ${socketsInRoom.length} sockets in room ${roomName}`
      );

      if (socketsInRoom.length === 0) {
        console.log(
          "⚠️ No sockets found in room. Check room name and socket connections."
        );
      }

      const messageToEmit = {
        ...payload,
        _id: result.id,
        visibleToRoles: visibleToRoles,
        tempId: req.body.tempId,
      };

      console.log(
        "📤 Message to emit:",
        JSON.stringify(messageToEmit, null, 2)
      );

      let emittedCount = 0;
      socketsInRoom.forEach((socket) => {
        console.log(`🔍 Checking socket ${socket.id}:`);
        console.log(`   - User ID: ${socket.userId}`);
        console.log(`   - User Role: ${socket.userRole}`);
        console.log(`   - Object Type: ${socket.objectType}`);
        console.log(`   - Object ID: ${socket.objectId}`);

        if (socket.userRole && visibleToRoles.includes(socket.userRole)) {
          socket.emit("newMessage", messageToEmit);
          emittedCount++;
        } else {
          console.log(
            `❌ Socket ${socket.id} role ${socket.userRole} not in visibleToRoles:`,
            visibleToRoles
          );
        }
      });


      return res.status(201).json(result);
    } else {
      console.error("❌ Failed to save message:", result);
      return res.status(400).json(result);
    }
  } catch (error) {
    console.error("💥 Error creating message:", error);
    return res.status(500).json({ success: false, message: error.message });
  }
});


module.exports = router;