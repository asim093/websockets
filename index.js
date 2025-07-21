const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");

const emRoutes = require("./em");
const { getUserRole } = require("./utils/getuserRole");

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

  socket.on("join", async ({ objectType, objectId, userId, token }) => {
    try {
      console.log(`🔵 Socket ${socket.id} joining room for ${objectType} with ID ${objectId}, userId: ${userId}`);

      const room = `${objectType}-${objectId}`;
      socket.join(room);

      const userRole = await getUserRole(userId);
      socket.userRole = userRole;
      socket.userId = userId;
      socket.objectType = objectType;
      socket.objectId = objectId;

      console.log(`✅ Socket ${socket.id} joined room ${room} as ${userRole}`);

      // Confirm join to client
      socket.emit('joined', {
        room,
        userRole,
        message: `Successfully joined room ${room}`
      });

    } catch (error) {
      console.error('❌ Error in socket join:', error);
      socket.emit('error', { message: 'Failed to join room' });
    }
  });

  socket.on('sendMessage', (data) => {
    try {
      const room = `${data.objectType}-${data.object}`;
      console.log(`📤 Broadcasting message to room: ${room}`);
      
      // Broadcast to all sockets in the room
      io.to(room).emit('newMessage', data);
    } catch (error) {
      console.error('❌ Error sending message:', error);
      socket.emit('error', { message: 'Failed to send message' });
    }
  });

  socket.on("disconnect", () => {
    console.log("🔴 Socket disconnected:", socket.id);
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