const express = require("express");
const createEntity = require("../EntityHandler/CREATE");
const { ObjectId} = require("mongodb");
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

    if (result.success) {

      const roomName = `${req.body.objectType}-${req.body.object}`;

      const socketsInRoom = await req.io.in(roomName).fetchSockets();
   

      

      const messageToEmit = {
        ...payload,
        _id: result.id,
        visibleToRoles: visibleToRoles,
        tempId: req.body.tempId,
      };

     

      let emittedCount = 0;
      socketsInRoom.forEach((socket) => {
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