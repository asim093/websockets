require("dotenv").config();
const { MongoClient, ObjectId } = require("mongodb");
const { getAggregatedData } = require("../EntityHandler/READ");
const { callMakeWebhook } = require("../utils/webhook");
const { getMappedValue } = require("../utils/columnMapping");

async function processImportDataRow(rowDoc, columnMapping, dbClient, database, io, importDataId, fileName) {
  const importDataRowsCollection = database.collection('ImportDataRows');
  const item = rowDoc.data;

  try {
    const poName = getMappedValue(item, 'POName', columnMapping) || getMappedValue(item, 'PONumber', columnMapping);
    const sku = getMappedValue(item, 'SKU', columnMapping);
    const shippingNumber = getMappedValue(item, 'shippingNumber', columnMapping);

    if (!poName) {
      await importDataRowsCollection.updateOne(
        { _id: rowDoc._id },
        {
          $set: {
            status: 'failure',
            error: 'Missing required fields: POName',
            processedAt: new Date()
          }
        }
      );

      io.emit('importDataProgress', {
        importDataId: importDataId,
        fileName: fileName,
        rowId: rowDoc._id.toString(),
        status: 'failure',
        error: 'Missing required fields POName'
      });
      return;
    }

    if (!shippingNumber) {
      await importDataRowsCollection.updateOne(
        { _id: rowDoc._id },
        {
          $set: {
            status: 'failure',
            error: 'Missing required fields: shippingNumber',
            processedAt: new Date()
          }
        }
      );

      io.emit('importDataProgress', {
        importDataId: importDataId,
        fileName: fileName,
        rowId: rowDoc._id.toString(),
        status: 'failure',
        error: 'Missing required fields shippingNumber'
      });
      return;
    }

    if (!sku) {
      await importDataRowsCollection.updateOne(
        { _id: rowDoc._id },
        {
          $set: {
            status: 'failure',
            error: 'Missing required fields: sku',
            processedAt: new Date()
          }
        }
      );

      io.emit('importDataProgress', {
        importDataId: importDataId,
        fileName: fileName,
        rowId: rowDoc._id.toString(),
        status: 'failure',
        error: 'Missing required fields sku'
      });
      return;
    }

    const poRequest = {
      entityType: 'PO',
      filter: { PONumber: poName },
      pagination: { page: 1, pageSize: 1 }
    };
    const poData = await getAggregatedData(poRequest);

    if (!poData?.data?.length) {
      await importDataRowsCollection.updateOne(
        { _id: rowDoc._id },
        {
          $set: {
            status: 'failure',
            error: `PO not found: ${poName}`,
            processedAt: new Date()
          }
        }
      );

      io.emit('importDataProgress', {
        importDataId: importDataId,
        fileName: fileName,
        rowId: rowDoc._id.toString(),
        status: 'failure',
        error: `PO not found: ${poName}`
      });
      return;
    }
    const po = poData.data[0];

    const productRequest = {
      entityType: 'Product',
      filter: { sku: sku },
      pagination: { page: 1, pageSize: 1 }
    };
    const productData = await getAggregatedData(productRequest);

    if (!productData?.data?.length) {
      await importDataRowsCollection.updateOne(
        { _id: rowDoc._id },
        {
          $set: {
            status: 'failure',
            error: `Product not found: ${sku}`,
            processedAt: new Date()
          }
        }
      );

      io.emit('importDataProgress', {
        importDataId: importDataId,
        fileName: fileName,
        rowId: rowDoc._id.toString(),
        status: 'failure',
        error: `Product not found: ${sku}`
      });
      return;
    }
    const product = productData.data[0];

    const lineItemsRequest = {
      entityType: 'lineItem',
      filter: {
        poId: { $eq: new ObjectId(po._id) },
        productId: { $eq: new ObjectId(product._id) }
      },
      pagination: { page: 1, pageSize: 1000 }
    };
    const lineItemsData = await getAggregatedData(lineItemsRequest);

    if (!lineItemsData?.data?.length) {
      await importDataRowsCollection.updateOne(
        { _id: rowDoc._id },
        {
          $set: {
            status: 'failure',
            error: 'No line items found',
            processedAt: new Date()
          }
        }
      );

      io.emit('importDataProgress', {
        importDataId: importDataId,
        fileName: fileName,
        rowId: rowDoc._id.toString(),
        status: 'failure',
        error: 'No line items found'
      });
      return;
    }
    const lineItems = lineItemsData.data;

    if (lineItems.length > 1) {
      await importDataRowsCollection.updateOne(
        { _id: rowDoc._id },
        {
          $set: {
            status: 'failure',
            error: `Multiple line items found for PO: ${poName} and SKU: ${sku}`,
            processedAt: new Date()
          }
        }
      );

      io.emit('importDataProgress', {
        importDataId: importDataId,
        fileName: fileName,
        rowId: rowDoc._id.toString(),
        status: 'failure',
        error: `Multiple line items found for PO: ${poName} and SKU: ${sku}`
      });
      return;
    }

    const lineItem = lineItems[0];
    const lineItemStatus = lineItem.status || '';

    if (lineItemStatus === 'Invoiced' || lineItemStatus === 'Delivered') {
      await importDataRowsCollection.updateOne(
        { _id: rowDoc._id },
        {
          $set: {
            status: 'failure',
            error: `Line item has invalid status (${lineItemStatus}). Cannot update line items with status Invoiced or Delivered.`,
            processedAt: new Date()
          }
        }
      );

      io.emit('importDataProgress', {
        importDataId: importDataId,
        fileName: fileName,
        rowId: rowDoc._id.toString(),
        status: 'failure',
        error: `Line item has invalid status (${lineItemStatus}). Cannot update line items with status Invoiced or Delivered.`
      });
      return;
    }

    const validLineItems = [lineItem];

    const shipmentRequest = {
      entityType: 'Shipment',
      filter: { shippingNumber: shippingNumber },
      pagination: { page: 1, pageSize: 10 }
    };
    const shipmentData = await getAggregatedData(shipmentRequest);

    if (shipmentData?.data?.length > 1) {
      await importDataRowsCollection.updateOne(
        { _id: rowDoc._id },
        {
          $set: {
            status: 'failure',
            error: `Multiple shipments found with shipping number: ${shippingNumber}`,
            processedAt: new Date()
          }
        }
      );

      io.emit('importDataProgress', {
        importDataId: importDataId,
        fileName: fileName,
        rowId: rowDoc._id.toString(),
        status: 'failure',
        error: `Multiple shipments found with shipping number: ${shippingNumber}`
      });
      return;
    }

    let shipmentId;
    if (shipmentData?.data?.length === 1) {
      shipmentId = shipmentData.data[0]._id;
    } else {
      const shipmentCollection = database.collection('Shipment');
      const newShipment = {
        shippingNumber: shippingNumber,
        shippingMode: getMappedValue(item, 'shippingMode', columnMapping) || null,
        arrivalDate: getMappedValue(item, 'arrivalDate', columnMapping) ? new Date(getMappedValue(item, 'arrivalDate', columnMapping)) : new Date(),
        createdAt: new Date(),
        updatedAt: new Date()
      };
      const shipmentResult = await shipmentCollection.insertOne(newShipment);
      shipmentId = shipmentResult.insertedId;

      try {
        const webhookResult = await callMakeWebhook('Shipment', 'POST', newShipment, { id: shipmentId }, shipmentId);
        console.log("webhookResult for Shipment POST:", webhookResult);
      } catch (webhookError) {
        console.error("Error calling webhook for Shipment:", webhookError);
      }

    }

    const lineItemCollection = database.collection('lineItem');
    const updateResult = await lineItemCollection.updateMany(
      { _id: { $in: validLineItems.map(li => new ObjectId(li._id)) } },
      {
        $set: {
          shipmentId: new ObjectId(shipmentId),
          shipmentName: shippingNumber,
          updatedAt: new Date()
        }
      }
    );

    if (updateResult.modifiedCount > 0) {
      const message = `Successfully updated ${updateResult.modifiedCount} line item(s)`;

      await importDataRowsCollection.updateOne(
        { _id: rowDoc._id },
        {
          $set: {
            status: 'success',
            message: message,
            processedAt: new Date()
          }
        }
      );

      for (const lineItem of validLineItems) {
        try {
          const webhookResult = await callMakeWebhook('lineItem', 'PUT', {
            shipmentId: new ObjectId(shipmentId),
            shipmentName: shippingNumber
          }, { id: lineItem._id }, lineItem._id);
          console.log("webhookResult for lineItem PUT:", webhookResult);
        } catch (webhookError) {
          console.error("Error calling webhook for lineItem:", webhookError);
        }
      }

      io.emit('importDataProgress', {
        importDataId: importDataId,
        fileName: fileName,
        rowId: rowDoc._id.toString(),
        status: 'success',
        message: message
      });
    } else {
      await importDataRowsCollection.updateOne(
        { _id: rowDoc._id },
        {
          $set: {
            status: 'failure',
            error: 'Failed to update line items',
            processedAt: new Date()
          }
        }
      );

      io.emit('importDataProgress', {
        importDataId: importDataId,
        fileName: fileName,
        rowId: rowDoc._id.toString(),
        status: 'failure',
        error: 'Failed to update line items'
      });
    }

  } catch (error) {
    console.error(`❌ Error processing row ${rowDoc._id}:`, error);

    await importDataRowsCollection.updateOne(
      { _id: rowDoc._id },
      {
        $set: {
          status: 'failure',
          error: error.message,
          errorStack: error.stack,
          processedAt: new Date()
        }
      }
    );

    io.emit('importDataProgress', {
      importDataId: importDataId,
      fileName: fileName,
      rowId: rowDoc._id.toString(),
      status: 'failure',
      error: error.message
    });
  }
}

async function processImportDataDocument(importDataDoc, dbClient, database, io) {
  const { _id, fileName, data, columnMapping, userId } = importDataDoc;
  const results = [];
  let processedCount = 0;
  let successCount = 0;
  let errorCount = 0;
  const totalItems = data?.length || 0;
  const recentErrors = [];

  console.log(`📦 Processing ImportData document ${_id} with ${totalItems} items`);
  console.log(`📋 ColumnMapping:`, JSON.stringify(columnMapping, null, 2));
  if (data && data.length > 0) {
    console.log(`📋 Sample data item (first row):`, JSON.stringify(data[0], null, 2));
  }


  for (let i = 0; i < data.length; i++) {
    const item = data[i];
    processedCount++;

    const rowResult = {
      index: i,
      poName: getMappedValue(item, 'POName', columnMapping) || getMappedValue(item, 'PONumber', columnMapping),
      sku: getMappedValue(item, 'SKU', columnMapping),
      shippingNumber: getMappedValue(item, 'shippingNumber', columnMapping),
      status: 'pending',
      message: ''
    };

    try {
      if (!rowResult.poName) {
        rowResult.status = 'error';
        rowResult.message = 'Missing required fields: POName';
        results.push(rowResult);
        errorCount++;

        const errorDetail = {
          index: i,
          row: rowResult,
          error: rowResult.message,
          timestamp: new Date().toISOString()
        };
        recentErrors.push(errorDetail);
        if (recentErrors.length > 10) recentErrors.shift();

        io.emit('importDataProgress', {
          importDataId: _id.toString(),
          fileName,
          processed: processedCount,
          total: totalItems,
          success: successCount,
          errors: errorCount,
          remaining: totalItems - processedCount,
          recentErrors: recentErrors.slice(-5)
        });
        continue;
      }
      if (!rowResult.sku) {
        rowResult.status = 'error';
        rowResult.message = 'Missing required fields: SKU';
        results.push(rowResult);
        errorCount++;

        const errorDetail = {
          index: i,
          row: rowResult,
          error: rowResult.message,
          timestamp: new Date().toISOString()
        };
        recentErrors.push(errorDetail);
        if (recentErrors.length > 10) recentErrors.shift();

        io.emit('importDataProgress', {
          importDataId: _id.toString(),
          fileName,
          processed: processedCount,
          total: totalItems,
          success: successCount,
          errors: errorCount,
          remaining: totalItems - processedCount,
          recentErrors: recentErrors.slice(-5)
        });
        continue;
      }

      if (!rowResult.shippingNumber) {
        rowResult.status = 'error';
        rowResult.message = 'Missing required fields: shippingNumber';
        results.push(rowResult);
        errorCount++;

        const errorDetail = {
          index: i,
          row: rowResult,
          error: rowResult.message,
          timestamp: new Date().toISOString()
        };
        recentErrors.push(errorDetail);
        if (recentErrors.length > 10) recentErrors.shift();

        io.emit('importDataProgress', {
          importDataId: _id.toString(),
          fileName,
          processed: processedCount,
          total: totalItems,
          success: successCount,
          errors: errorCount,
          remaining: totalItems - processedCount,
          recentErrors: recentErrors.slice(-5)
        });
        continue;
      }

      const poRequest = {
        entityType: 'PO',
        filter: { PONumber: rowResult.poName },
        pagination: { page: 1, pageSize: 1 }
      };

      const poData = await getAggregatedData(poRequest);

      if (!poData?.data?.length) {
        rowResult.status = 'error';
        rowResult.message = `PO not found: ${rowResult.poName}`;
        results.push(rowResult);
        errorCount++;

        const errorDetail = {
          index: i,
          row: rowResult,
          error: rowResult.message,
          timestamp: new Date().toISOString()
        };
        recentErrors.push(errorDetail);
        if (recentErrors.length > 10) recentErrors.shift();

        io.emit('importDataProgress', {
          importDataId: _id.toString(),
          fileName,
          processed: processedCount,
          total: totalItems,
          success: successCount,
          errors: errorCount,
          remaining: totalItems - processedCount,
          recentErrors: recentErrors.slice(-5)
        });
        continue;
      }
      const po = poData.data[0];

      const productRequest = {
        entityType: 'Product',
        filter: { sku: rowResult.sku },
        pagination: { page: 1, pageSize: 1 }
      };
      const productData = await getAggregatedData(productRequest);

      if (!productData?.data?.length) {
        rowResult.status = 'error';
        rowResult.message = `Product not found: ${rowResult.sku}`;
        results.push(rowResult);
        errorCount++;

        const errorDetail = {
          index: i,
          row: rowResult,
          error: rowResult.message,
          timestamp: new Date().toISOString()
        };
        recentErrors.push(errorDetail);
        if (recentErrors.length > 10) recentErrors.shift();

        io.emit('importDataProgress', {
          importDataId: _id.toString(),
          fileName,
          processed: processedCount,
          total: totalItems,
          success: successCount,
          errors: errorCount,
          remaining: totalItems - processedCount,
          recentErrors: recentErrors.slice(-5)
        });
        continue;
      }
      const product = productData.data[0];

      const lineItemsRequest = {
        entityType: 'lineItem',
        filter: {
          poId: { $eq: new ObjectId(po._id) },
          productId: { $eq: new ObjectId(product._id) }
        },
        pagination: { page: 1, pageSize: 1000 }
      };
      const lineItemsData = await getAggregatedData(lineItemsRequest);

      if (!lineItemsData?.data?.length) {
        rowResult.status = 'error';
        rowResult.message = 'No line items found';
        results.push(rowResult);
        errorCount++;

        const errorDetail = {
          index: i,
          row: rowResult,
          error: rowResult.message,
          timestamp: new Date().toISOString()
        };
        recentErrors.push(errorDetail);
        if (recentErrors.length > 10) recentErrors.shift();

        io.emit('importDataProgress', {
          importDataId: _id.toString(),
          fileName,
          processed: processedCount,
          total: totalItems,
          success: successCount,
          errors: errorCount,
          remaining: totalItems - processedCount,
          recentErrors: recentErrors.slice(-5)
        });
        continue;
      }
      const lineItems = lineItemsData.data;

      const shipmentRequest = {
        entityType: 'Shipment',
        filter: { shippingNumber: rowResult.shippingNumber },
        pagination: { page: 1, pageSize: 1 }
      };
      const shipmentData = await getAggregatedData(shipmentRequest);

      let shipmentId;
      if (shipmentData?.data?.length) {
        shipmentId = shipmentData.data[0]._id;
      } else {
        const shipmentCollection = database.collection('Shipment');
        const newShipment = {
          shippingNumber: rowResult.shippingNumber,
          shippingMode: getMappedValue(item, 'shippingMode', columnMapping) || null,
          arrivalDate: getMappedValue(item, 'arrivalDate', columnMapping) ? new Date(getMappedValue(item, 'arrivalDate', columnMapping)) : new Date(),
          createdAt: new Date(),
          updatedAt: new Date()
        };
        const shipmentResult = await shipmentCollection.insertOne(newShipment);
        shipmentId = shipmentResult.insertedId;


        try {
          const webhookResult = await callMakeWebhook('Shipment', 'POST', newShipment, { id: shipmentId }, shipmentId);
          console.log("webhookResult for Shipment POST (document):", webhookResult);
        } catch (webhookError) {
          console.error("Error calling webhook for Shipment (document):", webhookError);
        }

      }

      const lineItemCollection = database.collection('lineItem');
      const updateResult = await lineItemCollection.updateMany(
        { _id: { $in: lineItems.map(li => new ObjectId(li._id)) } },
        {
          $set: {
            shipmentId: new ObjectId(shipmentId),
            shipmentName: rowResult.shippingNumber,
            updatedAt: new Date()
          }
        }
      );

      if (updateResult.modifiedCount > 0) {
        rowResult.status = 'success';
        rowResult.message = `Successfully updated ${updateResult.modifiedCount} line item(s)`;
        successCount++;

        for (const lineItem of lineItems) {
          try {
            const webhookResult = await callMakeWebhook('lineItem', 'PUT', {
              shipmentId: new ObjectId(shipmentId),
              shipmentName: rowResult.shippingNumber
            }, { id: lineItem._id }, lineItem._id);
            console.log("webhookResult for lineItem PUT (document):", webhookResult);
          } catch (webhookError) {
            console.error("Error calling webhook for lineItem (document):", webhookError);
          }
        }

      } else {
        rowResult.status = 'error';
        rowResult.message = 'Failed to update line items';
        errorCount++;

        const errorDetail = {
          index: i,
          row: rowResult,
          error: rowResult.message,
          timestamp: new Date().toISOString()
        };
        recentErrors.push(errorDetail);
        if (recentErrors.length > 10) recentErrors.shift();
      }

      results.push(rowResult);

      io.emit('importDataProgress', {
        importDataId: _id.toString(),
        fileName,
        processed: processedCount,
        total: totalItems,
        success: successCount,
        errors: errorCount,
        remaining: totalItems - processedCount,
        recentErrors: recentErrors.slice(-5)
      });

    } catch (error) {
      rowResult.status = 'error';
      rowResult.message = `Processing error: ${error.message}`;
      results.push(rowResult);
      errorCount++;

      const errorDetail = {
        index: i,
        row: rowResult,
        error: error.message,
        stack: error.stack,
        timestamp: new Date().toISOString()
      };
      recentErrors.push(errorDetail);
      if (recentErrors.length > 10) recentErrors.shift();

      io.emit('importDataProgress', {
        importDataId: _id.toString(),
        fileName,
        processed: processedCount,
        total: totalItems,
        success: successCount,
        errors: errorCount,
        remaining: totalItems - processedCount,
        recentErrors: recentErrors.slice(-5)
      });
    }
  }

  const importDataCollection = database.collection('ImportData');
  await importDataCollection.updateOne(
    { _id: _id },
    {
      $set: {
        processingStatus: 'completed',
        processedAt: new Date(),
        processingResults: {
          total: totalItems,
          success: successCount,
          error: errorCount,
          results: results
        }
      }
    }
  );

  io.emit('importDataComplete', {
    importDataId: _id.toString(),
    fileName,
    summary: {
      total: totalItems,
      success: successCount,
      error: errorCount
    },
    results: results,
    recentErrors: recentErrors.slice(-10)
  });

  console.log(`✅ Completed processing ImportData document ${_id}: ${successCount} success, ${errorCount} errors`);

  return {
    total: totalItems,
    success: successCount,
    error: errorCount,
    results: results
  };
}

let isProcessing = false;

async function checkAndProcessImportData(io) {
  if (isProcessing) {
    console.log('⏸️ ImportData processing already in progress, skipping this execution');
    return;
  }

  isProcessing = true;
  const dbClient = new MongoClient(process.env.MONGODB_CONNECTION_STRING);

  try {
    await dbClient.connect();
    const database = dbClient.db(process.env.DB_NAME);
    const importDataCollection = database.collection('ImportData');
    const importDataRowsCollection = database.collection('ImportDataRows');

    const candidateRows = await importDataRowsCollection.find({
      status: 'pending'
    }).limit(100).toArray();

    if (candidateRows.length === 0) {
      console.log('⏰ No pending ImportDataRows to process');
      return;
    }

    const processingStartedAt = new Date();
    const candidateIds = candidateRows.map(row => row._id);

    const claimResult = await importDataRowsCollection.updateMany(
      {
        _id: { $in: candidateIds },
        status: 'pending'
      },
      {
        $set: {
          status: 'processing',
          processingStartedAt: processingStartedAt
        }
      }
    );

    if (claimResult.modifiedCount === 0) {
      console.log('⏰ No rows available for processing (may be claimed by another instance)');
      return;
    }

    const claimedRows = await importDataRowsCollection.find({
      _id: { $in: candidateIds },
      status: 'processing',
      processingStartedAt: processingStartedAt
    }).toArray();

    if (claimedRows.length === 0) {
      console.log('⏰ No rows successfully claimed for processing');
      return;
    }

    console.log(`📋 Successfully claimed ${claimedRows.length} ImportDataRows to process`);

    const rowsByImportData = {};
    for (const row of claimedRows) {
      if (!rowsByImportData[row.importDataId]) {
        rowsByImportData[row.importDataId] = [];
      }
      rowsByImportData[row.importDataId].push(row);
    }

    for (const [importDataId, rows] of Object.entries(rowsByImportData)) {
      const importDataDoc = await importDataCollection.findOne({ _id: new ObjectId(importDataId) });

      if (!importDataDoc) {
        console.error(`❌ ImportData document not found for ID: ${importDataId}`);
        await importDataRowsCollection.updateMany(
          { importDataId: new ObjectId(importDataId), status: 'pending' },
          {
            $set: {
              status: 'failure',
              error: 'ImportData document not found',
              processedAt: new Date()
            }
          }
        );
        continue;
      }

      const columnMapping = importDataDoc.columnMapping || {};
      const fileName = importDataDoc.fileName;

      console.log(`📦 Processing ${rows.length} rows for ImportData ${importDataId} (${fileName})`);

      const totalRows = await importDataRowsCollection.countDocuments({ importDataId: new ObjectId(importDataId) });
      const successCount = await importDataRowsCollection.countDocuments({ importDataId: new ObjectId(importDataId), status: 'success' });
      const failureCount = await importDataRowsCollection.countDocuments({ importDataId: new ObjectId(importDataId), status: 'failure' });
      const pendingCount = await importDataRowsCollection.countDocuments({ importDataId: new ObjectId(importDataId), status: 'pending' });
      const processingCount = await importDataRowsCollection.countDocuments({ importDataId: new ObjectId(importDataId), status: 'processing' });

      for (const rowDoc of rows) {
        try {
          await processImportDataRow(rowDoc, columnMapping, dbClient, database, io, importDataId.toString(), fileName);

          const currentSuccess = await importDataRowsCollection.countDocuments({ importDataId: new ObjectId(importDataId), status: 'success' });
          const currentFailure = await importDataRowsCollection.countDocuments({ importDataId: new ObjectId(importDataId), status: 'failure' });
          const currentPending = await importDataRowsCollection.countDocuments({ importDataId: new ObjectId(importDataId), status: 'pending' });
          const currentProcessing = await importDataRowsCollection.countDocuments({ importDataId: new ObjectId(importDataId), status: 'processing' });
          const processed = currentSuccess + currentFailure;

          io.emit('importDataProgress', {
            importDataId: importDataId.toString(),
            fileName: fileName,
            processed: processed,
            total: totalRows,
            success: currentSuccess,
            errors: currentFailure,
            remaining: currentPending + currentProcessing
          });

          if (currentPending === 0 && currentProcessing === 0) {
            const allRows = await importDataRowsCollection.find({ importDataId: new ObjectId(importDataId) }).toArray();

            await importDataCollection.updateOne(
              { _id: new ObjectId(importDataId) },
              {
                $set: {
                  processingStatus: 'completed',
                  processedAt: new Date(),
                  counts: {
                    total: totalRows,
                    success: currentSuccess,
                    failure: currentFailure,
                    pending: 0
                  }
                }
              }
            );

            io.emit('importDataComplete', {
              importDataId: importDataId.toString(),
              fileName: fileName,
              summary: {
                total: totalRows,
                success: currentSuccess,
                error: currentFailure
              },
              results: allRows.map(row => ({
                index: row.rowIndex,
                status: row.status,
                message: row.message || row.error,
                poName: row.data?.POName || row.data?.PONumber,
                sku: row.data?.SKU,
                shippingNumber: row.data?.shippingNumber
              }))
            });

            console.log(`✅ Completed processing ImportData ${importDataId}: ${currentSuccess} success, ${currentFailure} failures out of ${totalRows} total rows`);
          }
        } catch (error) {
          console.error(`❌ Error processing row ${rowDoc._id}:`, error);

          await importDataRowsCollection.updateOne(
            { _id: rowDoc._id },
            {
              $set: {
                status: 'failure',
                error: error.message,
                errorStack: error.stack,
                processedAt: new Date()
              }
            }
          );
        }
      }
    }

  } catch (error) {
    console.error('❌ Error in checkAndProcessImportData:', error);
  } finally {
    try {
      await dbClient.close();
    } catch (closeError) {
      console.error('❌ Error closing database connection:', closeError);
    }
    isProcessing = false;
  }
}

module.exports = {
  checkAndProcessImportData,
  processImportDataRow,
  processImportDataDocument
};

