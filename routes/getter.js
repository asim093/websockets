const express = require('express');
const authenticateToken = require('../utils/auth/token');
const { getAggregatedData } = require('../EntityHandler/READ');
const { MongoClient, ObjectId } = require('mongodb');
const getJoiningMethod = require('../utils/joiningMethods/joiningMethods');
const { getUserRole } = require('../utils/getuserRole');

const router = express.Router();

const getMessages = async (objectid, pinnedOnly = false, id, userRole) => {
  let getRequest = {
    entityType: "Message",
    filter: id
      ? {
        object: new ObjectId(objectid),
        _id: new ObjectId(id),
        visibleToRoles: { $in: [userRole] }
      }
      : {
        object: new ObjectId(objectid),
        visibleToRoles: { $in: [userRole] }
      }
  };

  if (pinnedOnly) {
    getRequest = {
      entityType: "Message",
      filter: {
        object: objectid,
        isPin: true,
        visibleToRoles: { $in: [userRole] }
      },
    };
  }


  console.log(getRequest);
  getRequest.$lookup = {
    $lookup: {
      from: "User", // Collection to join with
      localField: "sender", // Field in Messages
      foreignField: "_id", // Field in Users (should be ObjectId type)
      as: "userDetails", // Output array field
    },
  };

  getRequest.$unwind = {
    $unwind: "$userDetails", // Convert array to object (optional)
  };

  getRequest.$project = {
    $project: {
      _id: 1,
      objectType: 1,
      message: 1,
      sender: 1,
      attachments: 1,
      visibility: 1,
      createdAt: 1,
      top: 1,
      left: 1,
      isPin: 1,
      profileImage: "$userDetails.profileImage", // Get user profile image from lookup
      name: "$userDetails.name", // Get user name from lookup
    },
  };
  const data = await getAggregatedData(getRequest);
  return data;
};

router.get('/PinnedMessage/object/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const data = await getMessages(id, true);
    res.json(data);
  } catch (error) {
    console.log("/PinnedMessage/object/:id encountered an error: ", error);
    res.status(500).json({ error: error.message });
  }
});

router.get("/Message/object/:objectid/:id?", async (req, res) => {
  try {
    const { objectid, id } = req.params;
    const userRole = await getUserRole(req.user.userId);

    let data;
    if (id) {
      data = await getMessages(objectid, false, id, userRole);
    } else {
      data = await getMessages(objectid, false, null, userRole);
    }

    res.json(data);
  } catch (error) {
    console.error("/Message/object/:objectid/:id error:", error);
    res.status(500).json({ error: error.message });
  }
});

router.get("/Design/:id?", async (req, res) => {
  console.log("Design request received");
  const createdbyid = req.params.id;

  try {
    let getRequest = {
      entityType: "Design"
    };
    if (createdbyid) getRequest.filter = { createdBy: new ObjectId(createdbyid) };
    const joiningMethod = getJoiningMethod("/Design/requestId/:id");

    if (joiningMethod?.$lookup) {
      getRequest.$lookup = joiningMethod.$lookup;
    }
    if (joiningMethod?.$unwind) {
      getRequest.$lookup = joiningMethod.$lookup;
    }
    if (joiningMethod?.$project) {
      getRequest.$lookup = joiningMethod.$lookup;
    }

    const data = await getAggregatedData(getRequest);
    res.status(200).json(data);

  } catch (err) {
    console.error("Error fetching designs:", err);
    res.status(500).json({ success: false, error: "Internal Server Error" });
  }
});

router.get("/Design/requestId/:id", async (req, res) => {
  const requestId = req.params.id;
  const filer = req.query.filter ? JSON.parse(req.query.filter) : {};
  const type = req.query.type || "latest";
  try {
    let getRequest = {
      entityType: "Design",
      filter: { requestId: requestId, ...filer },
    };

    const joiningMethod = getJoiningMethod(type);

    if (joiningMethod?.$lookup) {
      getRequest.$lookup = joiningMethod.$lookup;
    }
    if (joiningMethod?.$unwind) {
      getRequest.$lookup = joiningMethod.$lookup;
    }
    if (joiningMethod?.$project) {
      getRequest.$lookup = joiningMethod.$lookup;
    }

    const data = await getAggregatedData(getRequest);
    res.status(200).json(data);

  } catch (err) {
    console.error("Error fetching designs:", err);
    res.status(500).json({ success: false, error: "Internal Server Error" });
  }
});


router.get('/:type/:field/:value', authenticateToken, async (req, res) => {
  try {
    const { type, field, value } = req.params;
    const getRequest = { entityType: type, filter: { [field]: value } };
    const data = await getAggregatedData(getRequest);
    res.json(data);
  } catch (error) {
    console.log("/:type/:field/:value encountered an error: ", error);
    res.status(500).json({ error: error.message });
  }
});


router.get('/:type/:id', authenticateToken, async (req, res) => {
  const { type, id } = req.params;
  const getRequest = { entityType: type, filter: { _id: id } };
  const data = await getAggregatedData(getRequest);
  res.json(data);
});

router.get('/:type', authenticateToken, async (req, res) => {
  try {
    console.log("working")
    const { type } = req.params;
    const getRequest = { entityType: type };
    if (req.query.filter) {
      const filter = JSON.parse(req.query.filter);
      getRequest.filter = filter;
    }
    const data = await getAggregatedData(getRequest);

    res.json(data);
  } catch (error) {
    console.log("/:type encountered an error: ", error);
    res.status(500).json({ error: error.message });
  }
});

router.get('/', authenticateToken, async (req, res) => {
  const data = await getAggregatedData(req.body);
  res.json(data);
});





module.exports = router;