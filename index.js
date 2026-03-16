require("dotenv").config();
const { checkAndProcessImportData, checkAndProcessBulkUpdateImportData } = require("./cron/processImportData");

const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");
const cron = require("node-cron");

const { getUserRole } = require("./utils/getuserRole.js");
const emRoutes = require("./em");
const { closeWebhookDb } = require("./utils/webhook.js");

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3005;


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
        console.error(" Error getting user role:", roleError.message);
        userRole = "Client";
      }

      socket.userRole = userRole;
      socket.userId = userId;
      socket.objectType = objectType;
      socket.objectId = objectId;

      const userRoom = `user-${userId}`;
      socket.join(userRoom);

      console.log(` Socket ${socket.id} joined room ${room} as ${userRole} and user room ${userRoom}`);
    } catch (error) {
      console.error(" Error in socket join:", error);
      socket.emit("error", { message: "Failed to join room" });
    }
  });

  socket.on("sendMessage", (messageData) => {
    console.log("📩 Received sendMessage:", messageData);

    if (!messageData || !messageData.objectType || !messageData.object) {
      console.error(" Invalid message data:", messageData);
      return;
    }

    const room = `${messageData.objectType}-${messageData.object}`;
    const targetRole = messageData.targetRole;
    const targetRoles = messageData.targetRoles;
    const visibleToRoles = messageData.visibleToRoles;
    if (targetRole === "All") {
      socket.to(room).emit("newMessage", messageData);
    } else if (targetRole === "Internal") {
    } else {
      const roomSockets = io.sockets.adapter.rooms.get(room);

      if (roomSockets) {
        let sentCount = 0;
        roomSockets.forEach((socketId) => {
          const targetSocket = io.sockets.sockets.get(socketId);

          if (targetSocket && targetSocket.id !== socket.id) {
            let shouldSendMessage = false;

            if (visibleToRoles && Array.isArray(visibleToRoles)) {
              shouldSendMessage = visibleToRoles.includes(
                targetSocket.userRole
              );
            }
            else if (targetRoles && Array.isArray(targetRoles)) {
              shouldSendMessage = targetRoles.includes(targetSocket.userRole);
            }
            else if (targetRole && targetSocket.userRole) {
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
      console.error(" Invalid visibility change data:", data);
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
    console.error(" Socket error:", error);
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
  console.log(`🚀 Server running on http://localhost:${PORT}`);

  checkAndProcessImportData(io).catch(console.error);
  checkAndProcessBulkUpdateImportData(io).catch(console.error);

  cron.schedule("*/30 * * * * *", async () => {
    try { await checkAndProcessImportData(io); } catch (e) { console.error(e); }
    try { await checkAndProcessBulkUpdateImportData(io); } catch (e) { console.error(e); }
  });

  console.log("⏰ ImportData cron scheduled (every 30 seconds)");
});

const shutdown = async (signal) => {
  console.log(`${signal} received, shutting down...`);
  await closeWebhookDb();
  server.close(() => process.exit(0));
};
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

module.exports = app;
