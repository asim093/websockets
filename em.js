const express = require("express");
const getterRoutes = require("./routes/getter");
const posterRoutes = require("./routes/poster");
const putterRoutes = require("./routes/putter");
const deletorRoutes = require("./routes/deletor");

module.exports = function (io) {
  const router = express.Router();

  router.use((req, res, next) => {
    req.io = io;
    next();
  });

  router.use("/", getterRoutes);
  router.use("/", posterRoutes);
  router.use("/", putterRoutes);
  router.use("/", deletorRoutes);

  return router;
};
