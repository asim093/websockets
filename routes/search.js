const express = require('express');
const authenticateToken = require('../utils/auth/token');
const { MongoClient, ObjectId } = require('mongodb');
const { globalSearch } = require('../EntityHandler/SEARCH');

const router = express.Router();

router.get('/:type', authenticateToken, async (req, res) => {
    try {
      const { type } = req.params;
      const { searchTerm, page = 1, filter,pageSize = 50, sort,searchFields } = req.query;
  
      const getRequest = {
        entityType: type,
        searchTerm,
        searchFields,
        filter,
        pagination: {
          page: Number(page),
          pageSize: Number(pageSize),
        }
      };
  
      if (sort) {
        try {
          getRequest.sort = JSON.parse(sort);
        } catch (e) {
          console.warn('Invalid sort JSON:', e.message);
        }
      }
  console.log("getRequest:", getRequest);
      const data = await globalSearch(getRequest);
      res.json(data);
    } catch (error) {
      console.log("/search/:type encountered an error: ", error);
      res.status(500).json({ error: error.message });
    }
  });
  
  module.exports = router;