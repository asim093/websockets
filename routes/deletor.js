const express = require('express');
const authenticateToken = require('../utils/auth/token');
const {deleteEntity, deleteArrayItem} = require('../EntityHandler/DELETE');

const router = express.Router();


router.delete('/:type/:id/:field/:item', authenticateToken, async (req, res) => {
  const { type, id, field, item} = req.params;
  const result = await deleteArrayItem(type, id, field, item);
 

  res.json(result);
});

router.delete('/:type/:id', authenticateToken, async (req, res) => {
  const { type, id } = req.params;
  const result = await deleteEntity(type, id);
    console.log("delete2")

  res.json(result);
});

router.delete('/', authenticateToken, async (req, res) => {
  const result = await deleteEntity(req.body.type, req.body.id);
    console.log("delete3")

  res.json(result);
});

module.exports = router;