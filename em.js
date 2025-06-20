const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");

const emRoutes = require("./"); // Import the EM routes

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3001;

// Setup Socket.IO
const io = new Server(server, {
  cors: {
    origin: "*", // ⛔ for development only, restrict this in prod
    methods: ["GET", "POST"],
  },
  transports: ["websocket", "polling"],
});

// 🔌 Handle Socket Events
io.on("connection", (socket) => {
  console.log("🟢 Socket connected:", socket.id);

  // Handle joining rooms
  socket.on("join", ({ objectType, objectId }) => { // 🔧 Fixed: using objectId
    console.log(`🔵 Socket ${socket.id} joining room for ${objectType} with ID ${objectId}`);
    const room = `${objectType}-${objectId}`;
    socket.join(room);
    console.log(`✅ Socket ${socket.id} joined room ${room}`);
  });

  // Handle sending messages
  socket.on("sendMessage", (messageData) => {
    console.log("📩 Received sendMessage:", messageData);
    const room = `${messageData.objectType}-${messageData.object}`;
    
    // Emit to all clients in the room (including the sender)
    io.to(room).emit("newMessage", messageData); // 🔧 Fixed: emit "newMessage"
    console.log(`📡 Message broadcasted to room ${room}`);
  });

  // Handle disconnection
  socket.on("disconnect", () => {
    console.log("🔴 Socket disconnected:", socket.id);
  });

  // Handle errors
  socket.on("error", (error) => {
    console.error("❌ Socket error:", error);
  });
});

// Middlewares
app.use(cors());
app.use(express.json());


// Pass io to /em routes
app.use("/em", emRoutes(io));

// Health check routes
app.get("/api/test", (req, res) => {
  res.json({ message: "API is working!", timestamp: new Date().toISOString() });
});

app.get("/", (req, res) => {
  res.send("Server is running and accessible!");
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('Error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

server.listen(PORT, () => {
  console.log(`🚀 Server running locally on http://localhost:${PORT}`);
  console.log(`📡 Socket.IO server is ready for connections`);
});

module.exports = app;