require('dotenv').config();

const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");

const { getUserRole } = require("./utils/getuserRole.js");
const emRoutes = require("./em");

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3005;

console.log('🔍 Environment check:');
console.log('MONGODB_CONNECTION_STRING:', process.env.MONGODB_CONNECTION_STRING ? 'EXISTS' : 'MISSING');
console.log('DB_NAME:', process.env.DB_NAME ? 'EXISTS' : 'MISSING');

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
      console.log(`🔵 Socket ${socket.id} joining room for ${objectType} with ID ${objectId}, userId: ${userId}`);
      
      const room = `${objectType}-${objectId}`;
      socket.join(room);
      
      // ✅ Get user role with error handling
      let userRole;
      try {
        userRole = await getUserRole(userId);
        if (!userRole) {
          console.warn(`⚠️ No role found for user ${userId}, defaulting to Client`);
          userRole = 'Client';
        }
      } catch (roleError) {
        console.error('❌ Error getting user role:', roleError.message);
        userRole = 'Client'; 
      }
      
      socket.userRole = userRole;
      socket.userId = userId;
      socket.objectType = objectType;
      socket.objectId = objectId;
      
      console.log(`✅ Socket ${socket.id} joined room ${room} as ${userRole}`);
      
      socket.emit('joined', {
        room,
        userRole,
        message: `Successfully joined room ${room} as ${userRole}`
      });
    } catch (error) {
      console.error('❌ Error in socket join:', error);
      socket.emit('error', { message: 'Failed to join room' });
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
    
    console.log(`🎯 Target Role: ${targetRole}`);
    
    if (targetRole === 'All') {
      socket.to(room).emit("newMessage", messageData);
      console.log(`📡 Message sent to ALL users in room ${room}`);
    } else {
      const roomSockets = io.sockets.adapter.rooms.get(room);
      
      if (roomSockets) {
        roomSockets.forEach(socketId => {
          const targetSocket = io.sockets.sockets.get(socketId);
          
          if (targetSocket && 
              targetSocket.id !== socket.id && 
              targetSocket.userRole === targetRole) {
            
            targetSocket.emit("newMessage", messageData);
            console.log(`📡 Message sent to ${targetRole}: ${targetSocket.id}`);
          }
        });
      }
      
      console.log(`🎯 Message targeted to ${targetRole} users only`);
    }
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

app.get("/api/test", (req, res) => {
  res.json({ message: "API is working!", timestamp: new Date().toISOString() });
});

app.get("/", (req, res) => {
  res.send("Server is running and accessible!");
});

app.use((err, req, res, next) => {
  console.error('Error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

server.listen(PORT, () => {
  console.log(`🚀 Server running locally on http://localhost:${PORT}`);
  console.log(`📡 Socket.IO server is ready for connections`);
});

module.exports = app;