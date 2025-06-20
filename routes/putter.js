const express = require('express');
const authenticateToken = require('../utils/auth/token');
const updateEntity = require('../EntityHandler/UPDATE');

const router = express.Router();

router.put('/', authenticateToken, async (req, res) => {
  const result = await updateEntity(req.body.type, req.body.id, req.body.data, req.body?.action);
  res.json(result);
});

router.put('/:type/:id', authenticateToken, async (req, res) => {
  const { type, id } = req.params;
  const result = await updateEntity(type, id, req.body.data, req.body?.action);
  res.json(result);
});
module.exports = router;