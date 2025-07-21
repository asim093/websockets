const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");
const jwt = require('jsonwebtoken'); // Add this

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

// Socket.IO authentication middleware
io.use((socket, next) => {
  try {
    // Get token from auth object or query
    const token = socket.handshake.auth.token || socket.handshake.query.token;
    
    if (!token) {
      return next(new Error('Authentication token required'));
    }

    // Verify JWT token
    const decoded = jwt.verify(token, process.env.ACCESS_TOKEN_SECRET);
    
    // Attach user info to socket
    socket.userId = decoded.userId;
    socket.token = token;
    
    console.log('✅ Socket authenticated for user:', decoded.userId);
    next();
  } catch (err) {
    console.error('❌ Socket authentication error:', err);
    next(new Error('Invalid authentication token'));
  }
});

io.on("connection", (socket) => {
  console.log("🟢 Socket connected:", socket.id, "User:", socket.userId);

  socket.on("join", async ({ objectType, objectId, userId, token }) => {
    try {
      // Use the authenticated userId from middleware instead of the one from client
      const authenticatedUserId = socket.userId;
      
      console.log(`🔵 Socket ${socket.id} joining room for ${objectType} with ID ${objectId}, userId: ${authenticatedUserId}`);

      const room = `${objectType}-${objectId}`;
      socket.join(room);

      const userRole = await getUserRole(authenticatedUserId);
      socket.userRole = userRole;
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
      // Verify the sender matches authenticated user
      if (data.sender !== socket.userId) {
        console.error('❌ Unauthorized message send attempt');
        socket.emit('error', { message: 'Unauthorized' });
        return;
      }

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
    console.log("🔴 Socket disconnected:", socket.id, "User:", socket.userId);
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