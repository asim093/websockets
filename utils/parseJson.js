
 function safeParseJSON(value, fallback) {
    if (typeof value === "string") {
      try {
        return JSON.parse(value);
      } catch (err) {
        console.error("Invalid JSON:", value);
      }
    }
    return fallback;
  }
module.exports = safeParseJSON;
