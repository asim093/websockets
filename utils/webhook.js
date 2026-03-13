require("dotenv").config();

const {
  callMakeWebhook: coreCallMakeWebhook,
} = require("../../csm-be-latest/api/em/middleware/simpleAutoTrigger");

async function callMakeWebhook(
  entityType,
  operation,
  data,
  responseData = null,
  entityId = null,
  actiontype = null
) {
  return coreCallMakeWebhook(
    entityType,
    operation,
    data,
    responseData,
    entityId,
    actiontype,
    {}
  );
}

module.exports = {
  callMakeWebhook,
};

