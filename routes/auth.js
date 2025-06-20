const express = require('express');
const jwt = require('jsonwebtoken');
const createEntity = require('../EntityHandler/CREATE');
require('dotenv').config();


const router = express.Router();

router.post('/login', (req, res) => {
  // Validate user credentials
  const user = { id: 1, name: "User" };  // Example user data
  const token = jwt.sign(user, process.env.ACCESS_TOKEN_SECRET);
  res.json({ token });
});

router.post('/signup', async (req, res) => {
  // Call CREATE handler to create a new user
  const result = await createEntity("User", req.body);
  if (result.success) {
    res.status(201).json(result);  // User created successfully
  } else {
    res.status(400).json(result);  // Validation or other error
  }
});

module.exports = router;
