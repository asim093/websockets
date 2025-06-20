const bcrypt = require('bcrypt');

async function hashSensitiveFields(data, schema) {
    if (Array.isArray(schema.hashedFields)) {
        for (const field of schema.hashedFields) {
            if (data?.[field] != null) {
                data[field] = await bcrypt.hash(data[field], 10);
            }
        }
    }
    return data;
}

module.exports = hashSensitiveFields;
