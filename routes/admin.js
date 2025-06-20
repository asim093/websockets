const express = require('express');
const jwt = require('jsonwebtoken');
const createEntity = require('../EntityHandler/CREATE');

const router = express.Router();

router.post('/startup', async (req, res) => {
  userSchema = {
    "entity": "User",
    "basicFields": {
      "name": "string",
      "email": "string",
      "password": "string",
      "organizationId": "string"
    },
    "customFields": {
      "advisorId": "number",
      "dateOfHire": "date"
    },
    "requiredFields": ["name", "email", "password", "organizationId", "advisorId"],
    "hashedFields": ["password"]
  }
  const result = await createEntity("Schema", userSchema);
  if (result.success) {
    res.status(201).json(result);  // User created successfully
  } else {
    res.status(400).json(result);  // Validation or other error
  }
});

module.exports = router;
