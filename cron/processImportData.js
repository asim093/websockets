require("dotenv").config();
const { MongoClient, ObjectId } = require("mongodb");
const { getAggregatedData } = require("../EntityHandler/READ");
const updateEntity = require("../EntityHandler/UPDATE");
const createEntity = require("../EntityHandler/CREATE");
const { callMakeWebhook } = require("../utils/webhook");
const { getMappedValue } = require("../utils/columnMapping");
const { validateRequiredField, handleRowError } = require("../utils/importErrorhandling");

async function processImportDataRow(rowDoc, columnMapping, dbClient, database, io, importDataId, fileName) {
  const importDataRowsCollection = database.collection('ImportDataRows');
  const item = rowDoc.data;

  try {
    const poName = getMappedValue(item, 'POName', columnMapping) || getMappedValue(item, 'PONumber', columnMapping);
    const sku = getMappedValue(item, 'SKU', columnMapping);
    const shippingNumber = getMappedValue(item, 'shippingNumber', columnMapping);
    const quantity = getMappedValue(item, 'quantity', columnMapping) ? parseInt(getMappedValue(item, 'quantity', columnMapping)) : null;

    const requiredFields = [
      { value: poName, name: 'POName' },
      { value: shippingNumber, name: 'shippingNumber' },
      { value: sku, name: 'sku' }
    ];

    for (const field of requiredFields) {
      if (!(await validateRequiredField(field.value, field.name, importDataRowsCollection, rowDoc, io, importDataId, fileName))) {
        return;
      }
    }

    const poRequest = {
      entityType: 'PO',
      filter: { PONumber: poName },
      pagination: { page: 1, pageSize: 1 }
    };
    const poData = await getAggregatedData(poRequest);

    if (!poData?.data?.length) {
      await handleRowError(importDataRowsCollection, rowDoc, `PO not found: ${poName}`, io, importDataId, fileName);
      return;
    }
    const po = poData.data[0];
    console.log('po', po);

    const productRequest = {
      entityType: 'Product',
      filter: { sku: sku },
      pagination: { page: 1, pageSize: 10 }
    };
    const productData = await getAggregatedData(productRequest);

    if (!productData?.data?.length) {
      await handleRowError(importDataRowsCollection, rowDoc, `Product not found: ${sku}`, io, importDataId, fileName);
      return;
    }

    if (productData?.data?.length > 1) {
      await handleRowError(importDataRowsCollection, rowDoc, `Multiple products found with SKU: ${sku}`, io, importDataId, fileName);
      return;
    }

    const product = productData.data[0];

    console.log('product', product);
    const lineItemsRequest = {
      entityType: 'lineItem',
      filter: {
        poId: { $eq: new ObjectId(po._id) },
        productId: { $eq: new ObjectId(product._id) }
      },
      pagination: { page: 1, pageSize: 1000 }
    };
    console.log('lineItemsRequest', lineItemsRequest);
    const lineItemsData = await getAggregatedData(lineItemsRequest);
    console.log('lineItemsData', lineItemsData);

    if (!lineItemsData?.data?.length) {
      await handleRowError(importDataRowsCollection, rowDoc, 'No line items found', io, importDataId, fileName);
      return;
    }
    const lineItems = lineItemsData.data;

    if (lineItems.length > 1) {
      await handleRowError(importDataRowsCollection, rowDoc, `Multiple line items found for PO: ${poName} and SKU: ${sku}`, io, importDataId, fileName);
      return;
    }

    const lineItem = lineItems[0];
    const lineItemStatus = lineItem.status || '';

    if (lineItemStatus === 'Invoiced' || lineItemStatus === 'Delivered') {
      await handleRowError(importDataRowsCollection, rowDoc, `Line item has invalid status (${lineItemStatus}). Cannot update line items with status Invoiced or Delivered.`, io, importDataId, fileName);
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
      await handleRowError(importDataRowsCollection, rowDoc, `Multiple shipments found with shipping number: ${shippingNumber}`, io, importDataId, fileName);
      return;
    }

    const shippingMode = getMappedValue(item, 'shippingMode', columnMapping) || null;
    const shipDate = getMappedValue(item, 'shipDate', columnMapping) ? new Date(getMappedValue(item, 'shipDate', columnMapping)) : new Date();

    let eta = null;
    if (shipDate && shippingMode) {
      eta = new Date(shipDate);
      const shippingModeUpper = shippingMode.toUpperCase();
      if (shippingModeUpper === 'AIR') {
        eta.setDate(eta.getDate() + 14);
      } else if (shippingModeUpper === 'SEA' || shippingModeUpper === 'BOAT') {
        eta.setDate(eta.getDate() + 35);
      }
    }

    let dbShipmentId;
    if (shipmentData?.data?.length === 1) {
      dbShipmentId = shipmentData.data[0]._id;
      const shipmentUpdatePayload = {
        shippingNumber: shippingNumber,
        shippingMode: shippingMode,
        shipDate: shipDate,
        eta: eta
      };
      const updateRes = await updateEntity('Shipment', dbShipmentId, shipmentUpdatePayload);
      if (!updateRes?.success) {
        throw new Error(updateRes?.message || 'Failed to update shipment');
      }
      try {
        await callMakeWebhook('Shipment', 'PUT', shipmentUpdatePayload, { id: dbShipmentId }, dbShipmentId);
      } catch (webhookError) {
        console.error("Error calling webhook for Shipment (update):", webhookError);
      }
    } else if (shipmentData?.data?.length === 0) {
      const newShipment = {
        shippingNumber: shippingNumber,
        shippingMode: shippingMode,
        shipDate: shipDate,
        eta: eta
      };
      const createRes = await createEntity('Shipment', newShipment);
      if (!createRes?.success || !createRes?.id) {
        throw new Error(createRes?.message || 'Failed to create shipment');
      }
      dbShipmentId = createRes.id;
      try {
        await callMakeWebhook('Shipment', 'POST', newShipment, { id: dbShipmentId }, dbShipmentId);
      } catch (webhookError) {
        console.error("Error calling webhook for Shipment (create):", webhookError);
      }
    }

    let modifiedCount = 0;
    for (const li of validLineItems) {
      const existingShipments = Array.isArray(li.shipments) ? li.shipments : [];
      const existingShipmentIndex = existingShipments.findIndex(
        s => s.shipmentName === shippingNumber
      );

      let shipmentsArray = [...existingShipments];
      const lineItemSizeBreakdown = Array.isArray(li.sizeBreakdown) ? li.sizeBreakdown : [];
      const hasSizeBreakdown = lineItemSizeBreakdown.length > 0;

      if (existingShipmentIndex >= 0) {
        const existingShipment = shipmentsArray[existingShipmentIndex];
        
        if (hasSizeBreakdown) {
          const matchingSize = lineItemSizeBreakdown.find(
            size => size.csmSku && size.csmSku === sku
          );

          if (!matchingSize) {
            await handleRowError(importDataRowsCollection, rowDoc, `Size not found for SKU: ${sku} in line item sizeBreakdown`, io, importDataId, fileName);
            return;
          }

          const shipmentSizeBreakdown = Array.isArray(existingShipment.sizeBreakdown) ? existingShipment.sizeBreakdown : [];
          const sizeBreakdownIndex = shipmentSizeBreakdown.findIndex(
            sb => sb.sizeName === matchingSize.sizeName
          );

          const sizeQuantity = quantity || parseInt(matchingSize.quantity || 0) || 0;

          if (sizeBreakdownIndex >= 0) {
            shipmentSizeBreakdown[sizeBreakdownIndex].quantity = sizeQuantity;
          } else {
            shipmentSizeBreakdown.push({
              sizeName: matchingSize.sizeName,
              quantity: sizeQuantity
            });
          }

          const totalQuantity = shipmentSizeBreakdown.reduce(
            (sum, sb) => sum + (parseInt(sb.quantity || 0) || 0),
            0
          );

          shipmentsArray[existingShipmentIndex] = {
            ...existingShipment,
            quantity: totalQuantity,
            sizeBreakdown: shipmentSizeBreakdown
          };
        } else {
          const newQuantity = quantity || parseInt(existingShipment.quantity || 0) || 0;
          shipmentsArray[existingShipmentIndex] = {
            ...existingShipment,
            quantity: newQuantity
          };
        }
      } else {
        if (hasSizeBreakdown) {
          const matchingSize = lineItemSizeBreakdown.find(
            size => size.csmSku && size.csmSku === sku
          );

          if (!matchingSize) {
            await handleRowError(importDataRowsCollection, rowDoc, `Size not found for SKU: ${sku} in line item sizeBreakdown`, io, importDataId, fileName);
            return;
          }

          const sizeQuantity = quantity || parseInt(matchingSize.quantity || 0) || 0;
          shipmentsArray.push({
            shipmentId: dbShipmentId.toString(),
            shipmentName: shippingNumber,
            quantity: sizeQuantity,
            sizeBreakdown: [{
              sizeName: matchingSize.sizeName,
              quantity: sizeQuantity
            }]
          });
        } else {
          const defaultQuantity = quantity || parseInt(li.quantity || 0) || 0;
          shipmentsArray.push({
            shipmentId: dbShipmentId.toString(),
            shipmentName: shippingNumber,
            quantity: defaultQuantity
          });
        }
      }

      const updatePayload = {
        shipments: shipmentsArray
      };

      const updateRes = await updateEntity('lineItem', li._id, updatePayload);
      if (updateRes?.success) {
        modifiedCount += 1;
        try {
          await callMakeWebhook('lineItem', 'PUT', updatePayload, { id: li._id }, li._id);
        } catch (webhookError) {
          console.error("Error calling webhook for lineItem PUT:", webhookError);
        }
      } else {
        console.error(`Failed to update line item ${li._id}:`, updateRes?.message);
      }
    }

    if (modifiedCount > 0) {
      const message = `Successfully updated ${modifiedCount} line item(s)`;

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


      io.emit('importDataProgress', {
        importDataId: importDataId,
        fileName: fileName,
        rowId: rowDoc._id.toString(),
        status: 'success',
        message: message
      });
    } else {
      await handleRowError(importDataRowsCollection, rowDoc, 'Failed to update line items', io, importDataId, fileName);
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
        for (const rowDoc of rows) {
          await handleRowError(importDataRowsCollection, rowDoc, 'ImportData document not found', io, importDataId.toString(), '');
        }
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

          await handleRowError(importDataRowsCollection, rowDoc, error.message, io, importDataId.toString(), fileName);

          await importDataRowsCollection.updateOne(
            { _id: rowDoc._id },
            {
              $set: {
                errorStack: error.stack
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
};

