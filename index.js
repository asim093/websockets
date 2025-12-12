require("dotenv").config();
const { checkAndProcessImportData } = require("./cron/processImportData");

const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");
const cron = require("node-cron");

const { getUserRole } = require("./utils/getuserRole.js");
const emRoutes = require("./em");

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3005;

console.log("🔍 Environment check:");
console.log(
  "MONGODB_CONNECTION_STRING:",
  process.env.MONGODB_CONNECTION_STRING ? "EXISTS" : "MISSING"
);
console.log("DB_NAME:", process.env.DB_NAME ? "EXISTS" : "MISSING");

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
  transports: ["websocket", "polling"],
});

io.on("connection", (socket) => {
  console.log("🟢 Socket connected:", socket.id);

  socket.on("join", async ({ objectType, objectId, userId }) => {
    try {
      console.log(
        `🔵 Socket ${socket.id} joining room for ${objectType} with ID ${objectId}, userId: ${userId}`
      );

      const room = `${objectType}-${objectId}`;
      socket.join(room);

      let userRole;
      try {
        userRole = await getUserRole(userId);
        if (!userRole) {
          console.warn(
            `⚠️ No role found for user ${userId}, defaulting to Client`
          );
          userRole = "Client";
        }
      } catch (roleError) {
        console.error("❌ Error getting user role:", roleError.message);
        userRole = "Client";
      }

      socket.userRole = userRole;
      socket.userId = userId;
      socket.objectType = objectType;
      socket.objectId = objectId;

      console.log(`✅ Socket ${socket.id} joined room ${room} as ${userRole}`);

      socket.emit("joined", {
        room,
        userRole,
        message: `Successfully joined room ${room} as ${userRole}`,
      });
    } catch (error) {
      console.error("❌ Error in socket join:", error);
      socket.emit("error", { message: "Failed to join room" });
    }
  });

  socket.on("sendMessage", (messageData) => {
    console.log("📩 Received sendMessage:", messageData);

    if (!messageData || !messageData.objectType || !messageData.object) {
      console.error("❌ Invalid message data:", messageData);
      return;
    }

    const room = `${messageData.objectType}-${messageData.object}`;
    const targetRole = messageData.targetRole;
    const targetRoles = messageData.targetRoles; // NEW: Handle array
    const visibleToRoles = messageData.visibleToRoles; // NEW: Handle visibility array

    console.log(`🎯 Target Role: ${targetRole}`);
    console.log(`🎯 Target Roles Array: ${JSON.stringify(targetRoles)}`);
    console.log(`👀 Visible To Roles: ${JSON.stringify(visibleToRoles)}`);

    if (targetRole === "All") {
      socket.to(room).emit("newMessage", messageData);
      console.log(`📡 Message sent to ALL users in room ${room}`);
    } else if (targetRole === "Internal") {
      console.log(`🔒 Internal message - not broadcasting to other users`);
    } else {
      const roomSockets = io.sockets.adapter.rooms.get(room);

      if (roomSockets) {
        let sentCount = 0;
        roomSockets.forEach((socketId) => {
          const targetSocket = io.sockets.sockets.get(socketId);

          if (targetSocket && targetSocket.id !== socket.id) {
            let shouldSendMessage = false;

            // NEW: Use visibleToRoles array if available
            if (visibleToRoles && Array.isArray(visibleToRoles)) {
              shouldSendMessage = visibleToRoles.includes(
                targetSocket.userRole
              );
            }
            // NEW: Use targetRoles array if available
            else if (targetRoles && Array.isArray(targetRoles)) {
              shouldSendMessage = targetRoles.includes(targetSocket.userRole);
            }
            // FIXED: Old logic with correct method name
            else if (targetRole && targetSocket.userRole) {
              // FIXED: Changed from .include() to .includes() - this was the typo!
              shouldSendMessage =
                targetSocket.userRole.includes(targetRole) ||
                targetSocket.userRole === targetRole;
            }

            if (shouldSendMessage) {
              targetSocket.emit("newMessage", messageData);
              console.log(
                `📡 Message sent to ${targetSocket.userRole}: ${targetSocket.id}`
              );
              sentCount++;
            } else {
              console.log(
                `🚫 Message NOT sent to ${targetSocket.userRole}: ${targetSocket.id}`
              );
            }
          }
        });

        console.log(
          `🎯 Message targeted - sent to ${sentCount} sockets out of ${roomSockets.size} total`
        );
      } else {
        console.log(`⚠️ No sockets found in room ${room}`);
      }
    }
  });

  socket.on("messageVisibilityChanged", (data) => {
    console.log("🔄 Received visibility change:", data);

    if (!data || !data.objectType || !data.object) {
      console.error("❌ Invalid visibility change data:", data);
      return;
    }

    const room = `${data.objectType}-${data.object}`;
    socket.to(room).emit("messageVisibilityChanged", data);
    console.log(`🔄 Visibility change broadcasted to room ${room}`);
  });

  socket.on("disconnect", () => {
    console.log("🔴 Socket disconnected:", socket.id);
  });

  socket.on("error", (error) => {
    console.error("❌ Socket error:", error);
  });
});

app.use(cors());
app.use(express.json());

app.use("/em", emRoutes(io));

app.use((err, req, res, next) => {
  console.error("Error:", err);
  res.status(500).json({ error: "Internal server error" });
});

server.listen(PORT, () => {
  console.log(`🚀 Server running locally on http://localhost:${PORT}`);
  console.log(`📡 Socket.IO server is ready for connections`);


  checkAndProcessImportData(io).catch((error) => {
    console.error('❌ Error in initial ImportData processing:', error);
  });

  cron.schedule("*/2 * * * *", async () => {
    console.log("⏰ Running ImportData processing cron job...");
    try {
      await checkAndProcessImportData(io);
    } catch (error) {
      console.error('❌ Error in cron job execution:', error);
    }
  });

  console.log(`⏰ ImportData processing cron job scheduled (runs every 2 minutes)`);
});

module.exports = app;
