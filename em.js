const express = require("express");
const getterRoutes = require("./routes/getter");
const posterRoutes = require("./routes/poster");

module.exports = function (io) {
  const router = express.Router();

  // Attach io to every req
  router.use((req, res, next) => {
    req.io = io;
    next();
  });

  // Attach routes
  router.use("/", getterRoutes);
  router.use("/", posterRoutes);

  return router;
};