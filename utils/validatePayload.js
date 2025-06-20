const { ObjectId } = require('mongodb');

function validatePayload(schema, data, checkRequired) {
    const errors = [];

    if (checkRequired) {
        // Check for required fields
        schema.requiredFields.forEach((field) => {
            if (!(field in data)) {
                errors.push(`${field} is required.`);
            }
        });
    }

    // Validate field types for basic and custom fields
    const allFields = { ...schema.basicFields, ...schema.customFields };
    for (const field in data) {
        const expectedType = allFields[field];
        const actualType = typeof data[field];
        if (expectedType) {
            if (expectedType === "array" && !Array.isArray(data[field])) {
                errors.push(`Field ${field} should be of type ${expectedType}, not ${actualType}`);
            } else if (expectedType === "date") {
                // Check if it's a valid Date object or a valid ISO date string
                const isValidDate = data[field] instanceof Date ||
                    (typeof data[field] === "string" && !isNaN(Date.parse(data[field])));
                if (!isValidDate) {
                    errors.push(`Field ${field} should be of type ${expectedType}, not ${actualType}`);
                } else {
                    // Convert to Date object if it's a valid ISO string
                    data[field] = new Date(data[field]);
                }
            } else if (expectedType === "ObjectId" && actualType !== expectedType) {
                // Check if it's a valid ObjectId
                const isValidObjectId = /^[0-9a-fA-F]{24}$/.test(data[field]);
                if (!isValidObjectId) {
                    errors.push(`Field ${field} should be of type ${expectedType}, not ${actualType}`);
                }
                else {
                    // Convert to ObjectId if it's a valid string
                    data[field] = new ObjectId(data[field]);
                }
            } else if (expectedType !== "array" && actualType !== expectedType) {
                errors.push(`Field ${field} should be of type ${expectedType}, not ${actualType}`);
            }
        }
    }

    return errors;
}

module.exports = validatePayload;