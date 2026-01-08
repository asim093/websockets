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
    let errorReason = null;
    for (const li of validLineItems) {
      const existingShipments = Array.isArray(li.shipments) ? li.shipments : [];
      let shipmentsArray = [...existingShipments];
      const lineItemSizeBreakdown = Array.isArray(li.sizeBreakdown) ? li.sizeBreakdown : [];
      const hasSizeBreakdown = lineItemSizeBreakdown.length > 0;

      if (hasSizeBreakdown) {

        const matchingSize = lineItemSizeBreakdown.find(
          size => size.csmSku && size.csmSku === sku
        );

        if (!matchingSize) {
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
                sp => sp.sku === sku
              );

              const suspectedProductData = {
                sku: sku,
                quantity: quantity || 0,
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
                try {
                  await callMakeWebhook('Shipment', 'PUT', shipmentUpdatePayload, { id: dbShipmentId }, dbShipmentId);
                } catch (webhookError) {
                  console.error("Error calling webhook for Shipment (suspectedProducts update):", webhookError);
                }
              }
            }
          }
          await handleRowError(importDataRowsCollection, rowDoc, `Size not found for SKU: ${sku} in line item sizeBreakdown`, io, importDataId, fileName);
          return;
        }

        const importQuantity = quantity || parseInt(matchingSize.quantity || 0) || 0;
        let perfectMatchFound = false;
        let suspectedProductsAdded = false;
        let noMatchReason = null;

        for (let i = 0; i < shipmentsArray.length; i++) {
          const shipment = shipmentsArray[i];
          const shipmentSizeBreakdown = Array.isArray(shipment.sizeBreakdown) ? shipment.sizeBreakdown : [];

          if (shipmentSizeBreakdown.length > 0) {
            const matchingShipmentSize = shipmentSizeBreakdown.find(
              sb => sb.sizeName && sb.sizeName === matchingSize.sizeName
            );

            if (matchingShipmentSize) {
              const shipmentSizeQuantity = parseInt(matchingShipmentSize.quantity || 0) || 0;
              const isSingleSizeShipment = shipmentSizeBreakdown.length === 1;
              const quantityMatches = isSingleSizeShipment
                ? (shipmentSizeQuantity === importQuantity && parseInt(shipment.quantity || 0) === importQuantity)
                : (shipmentSizeQuantity === importQuantity);

              const modeMatches = !shippingMode || !shipment.shippingMode || (shippingMode && shipment.shippingMode && shippingMode.toLowerCase() === shipment.shippingMode.toLowerCase());


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
                  })() : null,
                  shippingMode: formatShippingMode(shippingMode) || shipment.shippingMode
                };
                perfectMatchFound = true;
                break;
              }
            }
          }
        }

        if (!perfectMatchFound && dbShipmentId) {
          let skuFoundInShipment = false;
          let quantityMismatch = false;
          let modeMismatch = false;

          for (let j = 0; j < shipmentsArray.length; j++) {
            const shipment = shipmentsArray[j];
            const shipmentSizeBreakdown = Array.isArray(shipment.sizeBreakdown) ? shipment.sizeBreakdown : [];
            if (shipmentSizeBreakdown.length > 0) {
              const matchingShipmentSize = shipmentSizeBreakdown.find(
                sb => sb.sizeName && sb.sizeName === matchingSize.sizeName
              );
              if (matchingShipmentSize) {
                skuFoundInShipment = true;
                const shipmentSizeQuantity = parseInt(matchingShipmentSize.quantity || 0) || 0;
                const isSingleSizeShipment = shipmentSizeBreakdown.length === 1;
                if (isSingleSizeShipment) {
                  if (shipmentSizeQuantity !== importQuantity || parseInt(shipment.quantity || 0) !== importQuantity) {
                    quantityMismatch = true;
                  }
                } else {
                  if (shipmentSizeQuantity !== importQuantity) {
                    quantityMismatch = true;
                  }
                }
                if (shippingMode && shipment.shippingMode && shippingMode.toLowerCase() !== shipment.shippingMode.toLowerCase()) {
                  modeMismatch = true;
                }
                break;
              }
            }
          }

          if (!skuFoundInShipment) {
            noMatchReason = `SKU: ${matchingSize.csmSku} not found in any existing shipment's sizeBreakdown`;
          } else if (quantityMismatch && modeMismatch) {
            noMatchReason = `SKU: ${matchingSize.csmSku} found but quantity (${importQuantity}) and shipping mode (${shippingMode || 'N/A'}) do not match`;
          } else if (quantityMismatch) {
            noMatchReason = `SKU: ${matchingSize.csmSku} found but quantity (${importQuantity}) does not match`;
          } else if (modeMismatch) {
            noMatchReason = `SKU: ${matchingSize.csmSku} found but shipping mode (${shippingMode || 'N/A'}) does not match`;
          } else {
            noMatchReason = `SKU: ${matchingSize.csmSku} found but no perfect match in existing shipments`;
          }

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
              sp => sp.sku === matchingSize.csmSku && sp.sizeName === matchingSize.sizeName
            );

            const suspectedProductData = {
              sizeName: matchingSize.sizeName,
              sku: matchingSize.csmSku,
              quantity: importQuantity,
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
              suspectedProductsAdded = true;
              try {
                await callMakeWebhook('Shipment', 'PUT', shipmentUpdatePayload, { id: dbShipmentId }, dbShipmentId);
              } catch (webhookError) {
                console.error("Error calling webhook for Shipment (suspectedProducts update):", webhookError);
              }
            }
          }
        }

        if (suspectedProductsAdded) {
          errorReason = noMatchReason || `No perfect match found for SKU: ${matchingSize.csmSku} in line item shipments. Added to suspectedProducts.`;
          continue;
        }

      } else {
        const importQty = quantity || parseInt(li.quantity || 0) || 0;
        let perfectMatchFound = false;
        let suspectedProductsAdded = false;
        let noMatchReason = null;

        for (let i = 0; i < shipmentsArray.length; i++) {
          const shipment = shipmentsArray[i];

          if (!shipment.sizeBreakdown || (Array.isArray(shipment.sizeBreakdown) && shipment.sizeBreakdown.length === 0)) {
            const shipmentQuantity = parseInt(shipment.quantity || 0) || 0;
            const quantityMatches = shipmentQuantity === importQty;
            const modeMatches = !shippingMode || !shipment.shippingMode || (shippingMode && shipment.shippingMode && shippingMode.toLowerCase() === shipment.shippingMode.toLowerCase());

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
                })() : null,
                shippingMode: formatShippingMode(shippingMode) || shipment.shippingMode
              };
              perfectMatchFound = true;
              break;
            }
          }
        }

        if (!perfectMatchFound && dbShipmentId) {
          let quantityFound = false;
          let quantityMismatch = false;
          let modeMismatch = false;

          for (let j = 0; j < shipmentsArray.length; j++) {
            const shipment = shipmentsArray[j];
            if (!shipment.sizeBreakdown || (Array.isArray(shipment.sizeBreakdown) && shipment.sizeBreakdown.length === 0)) {
              const shipmentQuantity = parseInt(shipment.quantity || 0) || 0;
              if (shipmentQuantity > 0) {
                quantityFound = true;
                if (shipmentQuantity !== importQty) {
                  quantityMismatch = true;
                }
                if (shippingMode && shipment.shippingMode && shippingMode.toLowerCase() !== shipment.shippingMode.toLowerCase()) {
                  modeMismatch = true;
                }
              }
            }
          }

          if (quantityFound) {
            if (quantityMismatch && modeMismatch) {
              noMatchReason = `Quantity (${importQty}) and shipping mode (${shippingMode || 'N/A'}) do not match any existing shipment`;
            } else if (quantityMismatch) {
              noMatchReason = `Quantity (${importQty}) does not match any existing shipment`;
            } else if (modeMismatch) {
              noMatchReason = `Shipping mode (${shippingMode || 'N/A'}) does not match any existing shipment`;
            } else {
              noMatchReason = `No perfect match found for quantity: ${importQty} and shipping mode: ${shippingMode || 'N/A'}`;
            }

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
                sp => sp.sku === product.sku
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
                suspectedProductsAdded = true;
                try {
                  await callMakeWebhook('Shipment', 'PUT', shipmentUpdatePayload, { id: dbShipmentId }, dbShipmentId);
                } catch (webhookError) {
                  console.error("Error calling webhook for Shipment (suspectedProducts update):", webhookError);
                }
              }
            }
          } else {
            // Line item has no shipments, so don't add to suspectedProducts
            noMatchReason = `No shipment found with quantity in line item shipments`;
          }
        }

        if (suspectedProductsAdded) {
          errorReason = noMatchReason || `No perfect match found for quantity: ${importQty} and shipping mode: ${shippingMode || 'N/A'} in line item shipments. Added to suspectedProducts.`;
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

      io.emit('importDataProgress', {
        importDataId: importDataId,
        fileName: fileName,
        rowId: rowDoc._id.toString(),
        status: 'success',
        message: message
      });
    } else {
      let errorMessage = 'Failed to update line items';
      if (errorReason) {
        errorMessage = errorReason;
      } else {
        const hasSizeBreakdown = lineItems[0]?.sizeBreakdown && Array.isArray(lineItems[0].sizeBreakdown) && lineItems[0].sizeBreakdown.length > 0;
        if (hasSizeBreakdown) {
          errorMessage = `No perfect match found for SKU: ${sku} in line item shipments. Quantity or shipping mode mismatch. Added to suspectedProducts.`;
        } else {
          errorMessage = `No perfect match found for quantity: ${quantity || 'N/A'} and shipping mode: ${shippingMode || 'N/A'} in line item shipments. Added to suspectedProducts.`;
        }
      }
      await handleRowError(importDataRowsCollection, rowDoc, errorMessage, io, importDataId, fileName);
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

