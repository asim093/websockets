export async function validateRequiredField(fieldValue, fieldName, importDataRowsCollection, rowDoc, io, importDataId, fileName) {
    if (!fieldValue) {
        await importDataRowsCollection.updateOne(
            { _id: rowDoc._id },
            {
                $set: {
                    status: 'failure',
                    error: `Missing required fields: ${fieldName}`,
                    processedAt: new Date()
                }
            }
        );

        io.emit('importDataProgress', {
            importDataId: importDataId,
            fileName: fileName,
            rowId: rowDoc._id.toString(),
            status: 'failure',
            error: `Missing required fields: ${fieldName}`
        });
        return false;
    }
    return true;
}

export async function handleRowError(importDataRowsCollection, rowDoc, errorMessage, io, importDataId, fileName) {
    await importDataRowsCollection.updateOne(
        { _id: rowDoc._id },
        {
            $set: {
                status: 'failure',
                error: errorMessage,
                processedAt: new Date()
            }
        }
    );

    io.emit('importDataProgress', {
        importDataId: importDataId,
        fileName: fileName,
        rowId: rowDoc._id.toString(),
        status: 'failure',
        error: errorMessage
    });
}