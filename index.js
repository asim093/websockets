const express = require("express");
const getterRoutes = require("./routes/getter");
const posterRoutes = require("./routes/poster");
const putterRoutes = require("./routes/putter");
const deletorRoutes = require("./routes/deletor");
const adminRoutes = require("./routes/admin");
const searchRoutes = require("./routes/search");

module.exports = function (io) {
  const router = express.Router();

  // Attach io to every req
  router.use((req, res, next) => {
    req.io = io;
    next();
  });

  // Attach routes
  router.use("/search", searchRoutes);
  router.use("/", getterRoutes);
  router.use("/", posterRoutes);
  router.use("/", putterRoutes);
  router.use("/", deletorRoutes);
  router.use("/admin", adminRoutes);

  return router;
};
