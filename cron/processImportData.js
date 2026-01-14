require("dotenv").config();
const { MongoClient, ObjectId } = require("mongodb");
const { getAggregatedData } = require("../EntityHandler/READ");
const updateEntity = require("../EntityHandler/UPDATE");
const createEntity = require("../EntityHandler/CREATE");
const { callMakeWebhook } = require("../utils/webhook");
const { getMappedValue } = require("../utils/columnMapping");
const { validateRequiredField, handleRowError } = require("../utils/importErrorhandling");

function formatShippingMode(mode) {
  if (!mode) return null;
  const modeUpper = mode.toUpperCase();
  if (modeUpper === 'AIR') {
    return 'Air';
  } else if (modeUpper === 'SEA' || modeUpper === 'BOAT') {
    return 'Sea';
  } else if (modeUpper === 'GROUND') {
    return 'Ground';
  }
  return mode;
}

function parseDateString(dateString) {
  if (!dateString) return null;
  try {
    if (dateString instanceof Date) {
      if (isNaN(dateString.getTime())) {
        console.error('Invalid Date object');
        return null;
      }
      return new Date(Date.UTC(
        dateString.getUTCFullYear(),
        dateString.getUTCMonth(),
        dateString.getUTCDate(),
        0, 0, 0, 0
      ));
    }

    const dateStr = String(dateString).trim();

    if (/^\d{1,2}-\d{1,2}-\d{4}$/.test(dateStr)) {
      const parts = dateStr.split("-");
      let month, day, year;

      if (parseInt(parts[0], 10) > 12) {
        day = parseInt(parts[0], 10);
        month = parseInt(parts[1], 10);
        year = parseInt(parts[2], 10);
      } else {
        month = parseInt(parts[0], 10);
        day = parseInt(parts[1], 10);
        year = parseInt(parts[2], 10);
      }

      if (month < 1 || month > 12 || day < 1 || day > 31) {
        console.error(`Invalid date format: ${dateStr} (month: ${month}, day: ${day})`);
        return null;
      }

      const utcDate = new Date(Date.UTC(year, month - 1, day, 0, 0, 0, 0));
      return utcDate;
    }

    if (/^\d{4}\/\d{1,2}\/\d{1,2}$/.test(dateStr)) {
      const parts = dateStr.split("/");
      const year = parseInt(parts[0], 10);
      const month = parseInt(parts[1], 10);
      const day = parseInt(parts[2], 10);

      if (month < 1 || month > 12 || day < 1 || day > 31) {
        console.error(`Invalid date format: ${dateStr} (month: ${month}, day: ${day})`);
        return null;
      }

      const utcDate = new Date(Date.UTC(year, month - 1, day, 0, 0, 0, 0));
      return utcDate;
    }

    if (/^\d{1,2}\/\d{1,2}\/\d{2}$/.test(dateStr)) {
      const parts = dateStr.split("/");
      const month = parseInt(parts[0], 10);
      const day = parseInt(parts[1], 10);
      let year = parseInt(parts[2], 10);

      if (year < 100) {
        year = 2000 + year;
      }

      if (month < 1 || month > 12 || day < 1 || day > 31) {
        console.error(`Invalid date format: ${dateStr} (month: ${month}, day: ${day})`);
        return null;
      }

      const utcDate = new Date(Date.UTC(year, month - 1, day, 0, 0, 0, 0));
      return utcDate;
    }

    if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(dateStr)) {
      const parts = dateStr.split("/");
      const month = parseInt(parts[0], 10);
      const day = parseInt(parts[1], 10);
      const year = parseInt(parts[2], 10);

      if (month < 1 || month > 12 || day < 1 || day > 31) {
        console.error(`Invalid date format: ${dateStr} (month: ${month}, day: ${day})`);
        return null;
      }

      const utcDate = new Date(Date.UTC(year, month - 1, day, 0, 0, 0, 0));
      return utcDate;
    }

    if (/^\d{4}-\d{1,2}-\d{1,2}$/.test(dateStr)) {
      const parts = dateStr.split("-");
      const year = parseInt(parts[0], 10);
      const month = parseInt(parts[1], 10);
      const day = parseInt(parts[2], 10);

      if (month < 1 || month > 12 || day < 1 || day > 31) {
        console.error(`Invalid date format: ${dateStr} (month: ${month}, day: ${day})`);
        return null;
      }

      const utcDate = new Date(Date.UTC(year, month - 1, day, 0, 0, 0, 0));
      return utcDate;
    }

    if (dateStr.includes('T') || dateStr.match(/^\d{4}-\d{2}-\d{2}/)) {
      const datePart = dateStr.split('T')[0];
      if (/^\d{4}-\d{2}-\d{2}$/.test(datePart)) {
        const parts = datePart.split('-');
        const year = parseInt(parts[0], 10);
        const month = parseInt(parts[1], 10);
        const day = parseInt(parts[2], 10);
        const utcDate = new Date(Date.UTC(year, month - 1, day, 0, 0, 0, 0));
        return utcDate;
      }
    }

    const parsedDate = new Date(dateStr);
    if (isNaN(parsedDate.getTime())) {
      console.error(`Failed to parse date: ${dateStr}`);
      return null;
    }

    const utcDate = new Date(Date.UTC(
      parsedDate.getUTCFullYear(),
      parsedDate.getUTCMonth(),
      parsedDate.getUTCDate(),
      0, 0, 0, 0
    ));
    return utcDate;
  } catch (error) {
    console.error('Error parsing date:', dateString, error);
    return null;
  }
}

async function processImportDataRow(rowDoc, columnMapping, dbClient, database, io, importDataId, fileName) {
  const importDataRowsCollection = database.collection('ImportDataRows');
  const item = rowDoc.data;

  const trackingData = {
    sizePricingLineItemIds: [],
    shipmentIds: []
  };

  try {
    const poName = getMappedValue(item, 'POName', columnMapping) || getMappedValue(item, 'PONumber', columnMapping);
    const sku = getMappedValue(item, 'SKU', columnMapping);
    const shippingNumber = getMappedValue(item, 'shippingNumber', columnMapping);
    const quantity = getMappedValue(item, 'quantity', columnMapping) ? parseInt(getMappedValue(item, 'quantity', columnMapping)) : null;

    const requiredFields = [
      { value: poName, name: 'POName' },
      { value: shippingNumber, name: 'shippingNumber' },
      { value: sku, name: 'sku' },
      { value: quantity, name: "quantity" }
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

    const productRequest = {
      entityType: 'Product',
      filter: { sku: sku },
      pagination: { page: 1, pageSize: 10 }
    };
    const productData = await getAggregatedData(productRequest);

    let product = null;
    let lineItem = null;
    let isSizePricingCase = false;

    if (productData?.data?.length > 0) {
      if (productData.data.length > 1) {
        await handleRowError(importDataRowsCollection, rowDoc, `Multiple products found with SKU: ${sku}`, io, importDataId, fileName);
        return;
      }

      product = productData.data[0];

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
        await handleRowError(importDataRowsCollection, rowDoc, 'No line items found', io, importDataId, fileName);
        return;
      }
      const lineItems = lineItemsData.data;

      if (lineItems.length > 1) {
        await handleRowError(importDataRowsCollection, rowDoc, `Multiple line items found for PO: ${poName} and SKU: ${sku}`, io, importDataId, fileName);
        return;
      }

      lineItem = lineItems[0];
      isSizePricingCase = false;

    } else {
      const lineItemsRequest = {
        entityType: 'lineItem',
        filter: {
          poId: { $eq: new ObjectId(po._id) },
          'sizeBreakdown.csmSku': sku
        },
        pagination: { page: 1, pageSize: 1000 }
      };
      const lineItemsData = await getAggregatedData(lineItemsRequest);

      if (!lineItemsData?.data?.length) {
        await handleRowError(importDataRowsCollection, rowDoc, `No line item found with matching csmSku ${sku} in sizeBreakdown`, io, importDataId, fileName);
        return;
      }
      const allMatchingLineItems = lineItemsData.data;

      let foundLineItem = null;
      let foundMatchingSize = null;

      for (const li of allMatchingLineItems) {
        const lineItemSizeBreakdown = Array.isArray(li.sizeBreakdown) ? li.sizeBreakdown : [];
        const matchingSize = lineItemSizeBreakdown.find(
          size => size.csmSku && size.csmSku === sku
        );

        if (matchingSize) {
          if (quantity) {
            const importQty = parseInt(quantity);
            if (parseInt(matchingSize.quantity || 0) === importQty) {
              foundLineItem = li;
              foundMatchingSize = matchingSize;
              break;
            }
          } else {
            if (!foundLineItem) {
              foundLineItem = li;
              foundMatchingSize = matchingSize;
            }
          }
        }
      }

      if (!foundLineItem && allMatchingLineItems.length > 0) {
        const firstLi = allMatchingLineItems[0];
        const lineItemSizeBreakdown = Array.isArray(firstLi.sizeBreakdown) ? firstLi.sizeBreakdown : [];
        const matchingSize = lineItemSizeBreakdown.find(
          size => size.csmSku && size.csmSku === sku
        );
        if (matchingSize) {
          foundLineItem = firstLi;
          foundMatchingSize = matchingSize;
        }
      }

      if (!foundLineItem) {
        const existingRowCheck = await importDataRowsCollection.findOne({ _id: rowDoc._id });
        if (existingRowCheck?.status !== 'pending' && existingRowCheck?.status !== 'success') {
          await handleRowError(importDataRowsCollection, rowDoc, `Product not found: ${sku} and no matching size found in line items`, io, importDataId, fileName);
        }
        return;
      }

      lineItem = foundLineItem;
      isSizePricingCase = true;
    }
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

    const shippingModeRaw = getMappedValue(item, 'shippingMode', columnMapping) || null;
    const shippingMode = shippingModeRaw ? formatShippingMode(shippingModeRaw) : null;
    const shipDateValue = getMappedValue(item, 'shipDate', columnMapping);
    let shipDate = shipDateValue ? parseDateString(shipDateValue) : null;
    if (!shipDate) {
      const now = new Date();
      shipDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0, 0));
    }
    
    let eta = null;
    if (shipDate && shippingMode) {
      eta = new Date(shipDate);
      const shippingModeUpper = shippingMode.toUpperCase();
      if (shippingModeUpper === 'AIR') {
        eta.setDate(eta.getDate() + 14);
      } else if (shippingModeUpper === 'SEA' || shippingModeUpper === 'BOAT') {
        eta.setDate(eta.getDate() + 35);
      } else if (shippingModeUpper === 'GROUND') {
        eta.setDate(eta.getDate() + 3);
      }
    }

    let dbShipmentId;
    if (shipmentData?.data?.length === 1) {
      dbShipmentId = shipmentData.data[0]._id;
      trackingData.shipmentIds.push(dbShipmentId.toString());
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
      trackingData.shipmentIds.push(dbShipmentId.toString());
      try {
        await callMakeWebhook('Shipment', 'POST', newShipment, { id: dbShipmentId }, dbShipmentId);
      } catch (webhookError) {
        console.error("Error calling webhook for Shipment (create):", webhookError);
      }
    }

    let modifiedCount = 0;
    let errorReason = null;
    let sizePricingAddedToSuspected = false;
    let sizePricingProcessed = false;
    let normalPricingAddedToSuspected = false;
    for (const li of validLineItems) {
      const existingShipments = Array.isArray(li.shipments) ? li.shipments : [];
      let shipmentsArray = [...existingShipments];
      const lineItemSizeBreakdown = Array.isArray(li.sizeBreakdown) ? li.sizeBreakdown : [];
      const hasSizeBreakdown = lineItemSizeBreakdown.length > 0;

      if (isSizePricingCase) {
        trackingData.sizePricingLineItemIds.push(li._id.toString());

        const matchingSizes = lineItemSizeBreakdown.filter(
          size => size.csmSku && size.csmSku === sku
        );

        if (matchingSizes.length === 0) {
          continue;
        }

        if (matchingSizes.length > 1) {
          const sizeNames = matchingSizes.map(s => s.sizeName).filter(Boolean).join(', ');
          await handleRowError(importDataRowsCollection, rowDoc, `Multiple sizes found with same SKU ${sku} in sizeBreakdown: ${sizeNames}. Please specify size or quantity to match.`, io, importDataId, fileName);
          return { trackingData };
        }

        let currentSuspectedProducts = [];
        if (dbShipmentId) {
          const shipmentEntityRequest = {
            entityType: 'Shipment',
            filter: { _id: new ObjectId(dbShipmentId) },
            pagination: { page: 1, pageSize: 1 }
          };
          const shipmentEntityData = await getAggregatedData(shipmentEntityRequest);
          if (shipmentEntityData?.data?.length > 0) {
            currentSuspectedProducts = Array.isArray(shipmentEntityData.data[0].suspectedProducts)
              ? [...shipmentEntityData.data[0].suspectedProducts]
              : [];
          }
        }

        let matchingSize = matchingSizes.find(size => {
          return !currentSuspectedProducts.some(
            sp => sp.sku === size.csmSku && sp.sizeName === size.sizeName
          );
        });

        if (!matchingSize) {
          matchingSize = matchingSizes[0];
        }

        const finalQuantity = quantity ? parseInt(quantity) : parseInt(matchingSize.quantity || 0) || 0;

        if (dbShipmentId) {
          const shipmentEntityRequest = {
            entityType: 'Shipment',
            filter: { _id: new ObjectId(dbShipmentId) },
            pagination: { page: 1, pageSize: 1 }
          };
          const shipmentEntityData = await getAggregatedData(shipmentEntityRequest);

          if (shipmentEntityData?.data?.length > 0) {
            const currentShipment = shipmentEntityData.data[0];
            const suspectedProducts = Array.isArray(currentShipment.suspectedProducts) ? [...currentShipment.suspectedProducts] : [];

            const existingSuspectedIndex = suspectedProducts.findIndex(
              sp => sp.sku === matchingSize.csmSku && 
                    sp.sizeName === matchingSize.sizeName &&
                    parseInt(sp.quantity || 0) === finalQuantity
            );

            const suspectedProductData = {
              sizeName: matchingSize.sizeName,
              sku: matchingSize.csmSku,
              quantity: finalQuantity,
            };

            if (existingSuspectedIndex >= 0) {
              suspectedProducts[existingSuspectedIndex] = suspectedProductData;
            } else {
              suspectedProducts.push(suspectedProductData);
            }

            const shipmentUpdatePayload = {
              suspectedProducts: suspectedProducts
            };

            const updateRes = await updateEntity('Shipment', dbShipmentId, shipmentUpdatePayload);
            if (updateRes?.success) {
              sizePricingAddedToSuspected = true;
              sizePricingProcessed = true;
              try {
                await callMakeWebhook('Shipment', 'PUT', shipmentUpdatePayload, { id: dbShipmentId }, dbShipmentId);
              } catch (webhookError) {
                console.error("Error calling webhook for Shipment (suspectedProducts update):", webhookError);
              }
            }
          }
        }

        if (sizePricingProcessed) {
          const message = `SizePricing line item - added to suspectedProducts. Will be processed in reconciliation.`;

          await importDataRowsCollection.updateOne(
            { _id: rowDoc._id },
            {
              $set: {
                status: 'pending_reconciliation',
                message: message,
                processedAt: new Date()
              }
            }
          );

          io.emit('importDataProgress', {
            importDataId: importDataId,
            fileName: fileName,
            rowId: rowDoc._id.toString(),
            status: 'pending_reconciliation',
            message: message
          });
        }

        continue;

      } else {
        const importQty = quantity || parseInt(li.quantity || 0) || 0;
        let perfectMatchFound = false;
        let quantityFoundInAnyShipment = false;

        for (let i = 0; i < shipmentsArray.length; i++) {
          const shipment = shipmentsArray[i];

          if (!shipment.sizeBreakdown || (Array.isArray(shipment.sizeBreakdown) && shipment.sizeBreakdown.length === 0)) {
            const shipmentQuantity = parseInt(shipment.quantity || 0) || 0;
            const quantityMatches = shipmentQuantity === importQty;
            const formattedShippingMode = formatShippingMode(shippingMode);
            const shipmentShippingMode = shipment.shippingMode || null;
            const modeMatches = formattedShippingMode && shipmentShippingMode &&
              formattedShippingMode.toLowerCase() === shipmentShippingMode.toLowerCase();

            if (quantityMatches && modeMatches) {
              shipmentsArray[i] = {
                ...shipment,
                shipmentId: dbShipmentId.toString(),
                shipmentName: shippingNumber,
                shipdate: shipDate ? (() => {
                  const utcDate = new Date(Date.UTC(
                    shipDate.getUTCFullYear(),
                    shipDate.getUTCMonth(),
                    shipDate.getUTCDate(),
                    0, 0, 0, 0
                  ));
                  return utcDate.toISOString().split('T')[0];
                })() : null
              };
              perfectMatchFound = true;
              break;
            }

            if (shipmentQuantity > 0) {
              quantityFoundInAnyShipment = true;
            }
          }
        }

        if (!perfectMatchFound && dbShipmentId) {
          const shipmentEntityRequest = {
            entityType: 'Shipment',
            filter: { _id: new ObjectId(dbShipmentId) },
            pagination: { page: 1, pageSize: 1 }
          };
          const shipmentEntityData = await getAggregatedData(shipmentEntityRequest);

          if (shipmentEntityData?.data?.length > 0) {
            const currentShipment = shipmentEntityData.data[0];
            const suspectedProducts = Array.isArray(currentShipment.suspectedProducts) ? [...currentShipment.suspectedProducts] : [];

            const existingSuspectedIndex = suspectedProducts.findIndex(
              sp => sp.sku === product.sku && !sp.sizeName
            );

            const suspectedProductData = {
              sku: product.sku,
              quantity: importQty,
            };

            if (existingSuspectedIndex >= 0) {
              suspectedProducts[existingSuspectedIndex] = suspectedProductData;
            } else {
              suspectedProducts.push(suspectedProductData);
            }

            const shipmentUpdatePayload = {
              suspectedProducts: suspectedProducts
            };

            const updateRes = await updateEntity('Shipment', dbShipmentId, shipmentUpdatePayload);
            if (updateRes?.success) {
              normalPricingAddedToSuspected = true;
              try {
                await callMakeWebhook('Shipment', 'PUT', shipmentUpdatePayload, { id: dbShipmentId }, dbShipmentId);
              } catch (webhookError) {
                console.error("Error calling webhook for Shipment (suspectedProducts update):", webhookError);
              }
            }
          }

          errorReason = quantityFoundInAnyShipment
            ? `Quantity (${importQty}) or shipping mode (${shippingMode || 'N/A'}) does not match any existing shipment. Added to suspectedProducts.`
            : `No shipment found with quantity in line item shipments. Added to suspectedProducts.`;
          continue;
        }
      }

      if (shipmentsArray.length > existingShipments.length) {
        console.error(`❌ ERROR: shipmentsArray length increased from ${existingShipments.length} to ${shipmentsArray.length}. New objects should never be created!`);
        throw new Error(`Invalid state: shipmentsArray length increased. This should never happen. Original: ${existingShipments.length}, New: ${shipmentsArray.length}`);
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
        const errorMsg = updateRes?.message || 'Unknown error';
        errorReason = `Failed to update line item: ${errorMsg}`;
        console.error(`Failed to update line item ${li._id}:`, errorMsg);
      }
    }

    if (modifiedCount > 0) {
      const message = `Successfully processed ${modifiedCount} line item(s)`;

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
    } else if (sizePricingAddedToSuspected && !sizePricingProcessed) {
      const message = `SizePricing line item - added to suspectedProducts. Will be processed in reconciliation.`;

      await importDataRowsCollection.updateOne(
        { _id: rowDoc._id },
        {
          $set: {
            status: 'pending_reconciliation',
            message: message,
            processedAt: new Date()
          }
        }
      );

      io.emit('importDataProgress', {
        importDataId: importDataId,
        fileName: fileName,
        rowId: rowDoc._id.toString(),
        status: 'pending_reconciliation',
        message: message
      });
    } else if (normalPricingAddedToSuspected) {
      const message = errorReason || `Added to suspectedProducts. No perfect match found for quantity and shipping mode.`;

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
      const existingRowCheck = await importDataRowsCollection.findOne({ _id: rowDoc._id });
      if (existingRowCheck?.status === 'pending_reconciliation' || existingRowCheck?.status === 'pending' || existingRowCheck?.status === 'success') {
        return { trackingData };
      }

      if (sizePricingProcessed || sizePricingAddedToSuspected) {
        return { trackingData };
      }

      let errorMessage = 'Failed to update line items';
      if (errorReason) {
        errorMessage = errorReason;
      } else {
        const hasSizeBreakdown = lineItem?.sizeBreakdown && Array.isArray(lineItem.sizeBreakdown) && lineItem.sizeBreakdown.length > 0;
        if (hasSizeBreakdown) {
          errorMessage = `No perfect match found for SKU: ${sku} in line item shipments. Quantity or shipping mode mismatch. Added to suspectedProducts.`;
        } else {
          errorMessage = `No perfect match found for quantity: ${quantity || 'N/A'} and shipping mode: ${shippingMode || 'N/A'} in line item shipments. Added to suspectedProducts.`;
        }
      }
      await handleRowError(importDataRowsCollection, rowDoc, errorMessage, io, importDataId, fileName);
    }

    return { trackingData };

  } catch (error) {
    console.error(`❌ Error processing row ${rowDoc._id}:`, error);

    const existingRow = await importDataRowsCollection.findOne({ _id: rowDoc._id });

    if (existingRow?.status === 'pending_reconciliation') {
      return { trackingData };
    }

    if (existingRow?.status === 'pending' || existingRow?.status === 'success') {
      return { trackingData };
    }

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

    return { trackingData };
  }

  return { trackingData };
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

      const allTrackingData = {
        sizePricingLineItemIds: [],
        shipmentIds: []
      };

      for (const rowDoc of rows) {
        try {
          const result = await processImportDataRow(rowDoc, columnMapping, dbClient, database, io, importDataId.toString(), fileName);

          if (result?.trackingData) {
            allTrackingData.sizePricingLineItemIds.push(...result.trackingData.sizePricingLineItemIds);
            allTrackingData.shipmentIds.push(...result.trackingData.shipmentIds);
          }

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
            const uniqueLineItemIds = [...new Set(allTrackingData.sizePricingLineItemIds)];
            const uniqueShipmentIds = [...new Set(allTrackingData.shipmentIds)];

            if (uniqueLineItemIds.length > 0 && uniqueShipmentIds.length > 0) {
              await reconcileSizePricingLineItems(database, io, importDataId.toString(), fileName, uniqueLineItemIds, uniqueShipmentIds);
            }

            const finalSuccessCount = await importDataRowsCollection.countDocuments({ importDataId: new ObjectId(importDataId), status: 'success' });
            const finalFailureCount = await importDataRowsCollection.countDocuments({ importDataId: new ObjectId(importDataId), status: 'failure' });
            const finalPendingReconciliationCount = await importDataRowsCollection.countDocuments({ importDataId: new ObjectId(importDataId), status: 'pending_reconciliation' });

            const allRows = await importDataRowsCollection.find({ importDataId: new ObjectId(importDataId) }).toArray();

            await importDataCollection.updateOne(
              { _id: new ObjectId(importDataId) },
              {
                $set: {
                  processingStatus: 'completed',
                  processedAt: new Date(),
                  counts: {
                    total: totalRows,
                    success: finalSuccessCount,
                    failure: finalFailureCount,
                    pending: finalPendingReconciliationCount
                  }
                }
              }
            );

            io.emit('importDataComplete', {
              importDataId: importDataId.toString(),
              fileName: fileName,
              summary: {
                total: totalRows,
                success: finalSuccessCount,
                error: finalFailureCount
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

            console.log(`✅ Completed processing ImportData ${importDataId}: ${finalSuccessCount} success, ${finalFailureCount} failures out of ${totalRows} total rows`);
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

async function reconcileSizePricingLineItems(database, io, importDataId, fileName, trackedLineItemIds = [], trackedShipmentIds = []) {
  try {
    console.log('🔄 Starting reconciliation for sizePricing line items...');

    if (trackedLineItemIds.length === 0 || trackedShipmentIds.length === 0) {
      console.log('✅ No tracked line items or shipments for reconciliation');
      return;
    }

    const shipmentsCollection = database.collection('Shipment');
    const lineItemsCollection = database.collection('lineItem');
    const importDataRowsCollection = database.collection('ImportDataRows');

    const allTrackedShipments = await shipmentsCollection.find({
      _id: { $in: trackedShipmentIds.map(id => new ObjectId(id)) }
    }).toArray();

    if (allTrackedShipments.length === 0) {
      console.log('✅ No tracked shipments found for reconciliation');
      return;
    }

    const shipmentsWithSuspectedProducts = allTrackedShipments.filter(
      s => Array.isArray(s.suspectedProducts) && s.suspectedProducts.length > 0
    );

    let reconciledCount = 0;

    const lineItems = await lineItemsCollection.find({
      _id: { $in: trackedLineItemIds.map(id => new ObjectId(id)) },
      shipments: { $exists: true, $ne: [] },
      sizeBreakdown: { $exists: true, $ne: [] }
    }).toArray();

    for (const lineItem of lineItems) {
      const lineItemSizeBreakdown = Array.isArray(lineItem.sizeBreakdown) ? lineItem.sizeBreakdown : [];
      if (lineItemSizeBreakdown.length === 0) continue;

      const shipmentsArray = Array.isArray(lineItem.shipments) ? lineItem.shipments : [];

      for (let i = 0; i < shipmentsArray.length; i++) {
        const shipmentObj = shipmentsArray[i];
        const shipmentSizeBreakdown = Array.isArray(shipmentObj.sizeBreakdown) ? shipmentObj.sizeBreakdown : [];

        if (shipmentSizeBreakdown.length === 0) continue;

        let matchedShipment = null;
        let matchedSuspectedProducts = [];

        const matchingShipments = [];

        for (const trackedShipment of allTrackedShipments) {
          const trackedSuspectedProducts = Array.isArray(trackedShipment.suspectedProducts)
            ? [...trackedShipment.suspectedProducts]
            : [];

          if (trackedSuspectedProducts.length === 0) continue;

          let allSizesMatch = true;
          const tempMatchedSuspected = [];
          const tempRemainingSuspected = [...trackedSuspectedProducts];

          for (const size of shipmentSizeBreakdown) {
            if (!size.sizeName) {
              allSizesMatch = false;
              break;
            }

            const lineItemSize = lineItemSizeBreakdown.find(
              liSize => liSize.sizeName === size.sizeName && liSize.csmSku
            );

            if (!lineItemSize || !lineItemSize.csmSku) {
              allSizesMatch = false;
              break;
            }

            const sizeCsmSku = lineItemSize.csmSku;

            const matchingIndex = tempRemainingSuspected.findIndex(
              sp => sp.sizeName === size.sizeName &&
                sp.sku === sizeCsmSku &&
                parseInt(sp.quantity || 0) === parseInt(size.quantity || 0)
            );

            if (matchingIndex === -1) {
              allSizesMatch = false;
              break;
            }

            tempMatchedSuspected.push(tempRemainingSuspected[matchingIndex]);
            tempRemainingSuspected.splice(matchingIndex, 1);
          }

          if (allSizesMatch && tempMatchedSuspected.length === shipmentSizeBreakdown.length) {
            const trackedShipmentShippingMode = trackedShipment.shippingMode || null;
            const objectShippingMode = shipmentObj.shippingMode || null;

            const modeMatches = !objectShippingMode ||
              (trackedShipmentShippingMode &&
                trackedShipmentShippingMode.toLowerCase() === objectShippingMode.toLowerCase());

            if (modeMatches) {
              matchingShipments.push({
                shipment: trackedShipment,
                matchedSuspectedProducts: tempMatchedSuspected
              });
            }
          }
        }

        if (matchingShipments.length > 0) {
          if (matchingShipments.length === 1) {
            matchedShipment = matchingShipments[0].shipment;
            matchedSuspectedProducts = matchingShipments[0].matchedSuspectedProducts;
          } else {
            let earliestDate = null;
            let selectedMatch = null;

            for (const match of matchingShipments) {
              const exfactoryDate = match.shipment.exfactoryDate || match.shipment.shipDate;
              
              if (exfactoryDate) {
                const exfactoryDateObj = exfactoryDate instanceof Date ? exfactoryDate : new Date(exfactoryDate);
                
                if (!earliestDate || exfactoryDateObj < earliestDate) {
                  earliestDate = exfactoryDateObj;
                  selectedMatch = match;
                }
              } else if (!selectedMatch) {
                selectedMatch = match;
              }
            }

            if (selectedMatch) {
              matchedShipment = selectedMatch.shipment;
              matchedSuspectedProducts = selectedMatch.matchedSuspectedProducts;
            } else {
              matchedShipment = matchingShipments[0].shipment;
              matchedSuspectedProducts = matchingShipments[0].matchedSuspectedProducts;
            }
          }
        }

        if (!matchedShipment) {
          for (const trackedShipment of allTrackedShipments) {
            const trackedSuspectedProducts = Array.isArray(trackedShipment.suspectedProducts)
              ? [...trackedShipment.suspectedProducts]
              : [];

            if (trackedSuspectedProducts.length === 0) continue;

            let allSizesMatch = true;
            const tempMatchedSuspected = [];
            const tempRemainingSuspected = [...trackedSuspectedProducts];

            for (const size of shipmentSizeBreakdown) {
              if (!size.sizeName) {
                allSizesMatch = false;
                break;
              }

              const lineItemSize = lineItemSizeBreakdown.find(
                liSize => liSize.sizeName === size.sizeName && liSize.csmSku
              );

              if (!lineItemSize || !lineItemSize.csmSku) {
                allSizesMatch = false;
                break;
              }

              const sizeCsmSku = lineItemSize.csmSku;

              const matchingIndex = tempRemainingSuspected.findIndex(
                sp => sp.sizeName === size.sizeName &&
                  sp.sku === sizeCsmSku &&
                  parseInt(sp.quantity || 0) === parseInt(size.quantity || 0)
              );

              if (matchingIndex === -1) {
                allSizesMatch = false;
                break;
              }

              tempMatchedSuspected.push(tempRemainingSuspected[matchingIndex]);
              tempRemainingSuspected.splice(matchingIndex, 1);
            }

            if (allSizesMatch && tempMatchedSuspected.length === shipmentSizeBreakdown.length) {
              const trackedShipmentShippingMode = trackedShipment.shippingMode || null;
              const objectShippingMode = shipmentObj.shippingMode || null;

              const modeMatches = !objectShippingMode ||
                (trackedShipmentShippingMode &&
                  trackedShipmentShippingMode.toLowerCase() === objectShippingMode.toLowerCase());

              if (modeMatches) {
                matchedShipment = trackedShipment;
                matchedSuspectedProducts = tempMatchedSuspected;
                break;
              }
            }
          }
        }

        if (matchedShipment) {
          shipmentsArray[i] = {
            ...shipmentObj,
            shipmentId: matchedShipment._id.toString(),
            shipmentName: matchedShipment.shippingNumber || matchedShipment.name || '',
            shipdate: matchedShipment.shipDate ? (() => {
              const utcDate = new Date(Date.UTC(
                matchedShipment.shipDate.getUTCFullYear(),
                matchedShipment.shipDate.getUTCMonth(),
                matchedShipment.shipDate.getUTCDate(),
                0, 0, 0, 0
              ));
              return utcDate.toISOString().split('T')[0];
            })() : null,
            shippingMode: matchedShipment.shippingMode || shipmentObj.shippingMode || 'Air'
          };

          const updatePayload = { shipments: shipmentsArray };
          const updateRes = await updateEntity('lineItem', lineItem._id, updatePayload);

          if (updateRes?.success) {
            reconciledCount++;

            const matchedShipmentSuspectedProducts = Array.isArray(matchedShipment.suspectedProducts)
              ? [...matchedShipment.suspectedProducts]
              : [];

            const updatedSuspectedProducts = matchedShipmentSuspectedProducts.filter(sp => {
              return !matchedSuspectedProducts.some(msp =>
                msp.sizeName === sp.sizeName &&
                msp.sku === sp.sku &&
                parseInt(msp.quantity || 0) === parseInt(sp.quantity || 0)
              );
            });

            const shipmentUpdatePayload = {
              suspectedProducts: updatedSuspectedProducts
            };

            await updateEntity('Shipment', matchedShipment._id, shipmentUpdatePayload);

            const updatedShipment = await shipmentsCollection.findOne({ _id: matchedShipment._id });
            if (updatedShipment) {
              const shipmentIndex = allTrackedShipments.findIndex(s => s._id.toString() === matchedShipment._id.toString());
              if (shipmentIndex !== -1) {
                allTrackedShipments[shipmentIndex] = updatedShipment;
              }
            }

            try {
              await callMakeWebhook('lineItem', 'PUT', updatePayload, { id: lineItem._id }, lineItem._id);
              await callMakeWebhook('Shipment', 'PUT', shipmentUpdatePayload, { id: matchedShipment._id }, matchedShipment._id);
            } catch (webhookError) {
              console.error("Error calling webhook during reconciliation:", webhookError);
            }

            const relatedCsmSkus = matchedSuspectedProducts.map(msp => msp.sku).filter(Boolean);

            if (relatedCsmSkus.length > 0) {
              const successRows = await importDataRowsCollection.updateMany(
                {
                  importDataId: new ObjectId(importDataId),
                  'data.SKU': { $in: relatedCsmSkus },
                  status: 'pending_reconciliation'
                },
                {
                  $set: {
                    status: 'success',
                    message: `Successfully reconciled and associated with shipment ${matchedShipment.shippingNumber || matchedShipment.name || matchedShipment._id.toString()}`,
                    processedAt: new Date()
                  }
                }
              );

              if (successRows.modifiedCount > 0) {
                const updatedRows = await importDataRowsCollection.find({
                  importDataId: new ObjectId(importDataId),
                  'data.SKU': { $in: relatedCsmSkus },
                  status: 'success'
                }).toArray();

                for (const row of updatedRows) {
                  io.emit('importDataProgress', {
                    importDataId: importDataId,
                    fileName: fileName,
                    rowId: row._id.toString(),
                    status: 'success',
                    message: row.message
                  });
                }
              }
            }
          }
        } else {
          const sizeNames = shipmentSizeBreakdown.map(s => s.sizeName).filter(Boolean);
          const sizeQuantities = shipmentSizeBreakdown.map(s => `${s.sizeName}(${s.quantity || 0})`).join(', ');

          const relatedCsmSkus = lineItemSizeBreakdown
            .filter(s => sizeNames.includes(s.sizeName))
            .map(s => s.csmSku)
            .filter(Boolean);

          if (relatedCsmSkus.length > 0) {
            const errorRows = await importDataRowsCollection.find({
              importDataId: new ObjectId(importDataId),
              'data.SKU': { $in: relatedCsmSkus },
              status: 'pending_reconciliation'
            }).toArray();

            for (const errorRow of errorRows) {
              const existingRow = await importDataRowsCollection.findOne({ _id: errorRow._id });

              if (existingRow?.status === 'success' || existingRow?.status === 'pending_reconciliation') {
                if (existingRow?.message?.includes('Successfully reconciled') || existingRow?.message?.includes('added to suspectedProducts')) {
                  continue;
                }
              }

              await handleRowError(
                importDataRowsCollection,
                errorRow,
                `No Suspected Products Found: After checking all tracked shipments, no matching suspectedProducts found for sizes ${sizeQuantities}`,
                io,
                importDataId,
                fileName
              );
            }
          }
        }
      }
    }


    const remainingPendingRows = await importDataRowsCollection.find({
      importDataId: new ObjectId(importDataId),
      status: 'pending_reconciliation'
    }).toArray();

    if (remainingPendingRows.length > 0) {
      for (const pendingRow of remainingPendingRows) {
        const existingRow = await importDataRowsCollection.findOne({ _id: pendingRow._id });
        
        if (existingRow?.status === 'pending_reconciliation' && existingRow?.message?.includes('added to suspectedProducts')) {
          await handleRowError(
            importDataRowsCollection,
            pendingRow,
            'No suspectedProducts found: After reconciliation process completed, no matching suspectedProducts found.',
            io,
            importDataId,
            fileName
          );
        }
      }
    }

    console.log(`✅ Reconciliation completed: ${reconciledCount} sizePricing line items associated`);
    
  } catch (error) {
    console.error('❌ Error in reconcileSizePricingLineItems:', error);
  }
}

module.exports = {
  checkAndProcessImportData,
  processImportDataRow,
  reconcileSizePricingLineItems,
};

