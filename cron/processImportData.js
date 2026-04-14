require("dotenv").config();
const { MongoClient, ObjectId } = require("mongodb");
const { getAggregatedData } = require("../EntityHandler/READ");
const updateEntity = require("../EntityHandler/UPDATE");
const createEntity = require("../EntityHandler/CREATE");
const { callMakeWebhook } = require("../utils/webhook");
const { getMappedValue } = require("../utils/columnMapping");
const { validateRequiredField, handleRowError } = require("../utils/importErrorhandling");

// Normalizes shipping mode values into canonical labels.
function formatShippingMode(mode) {
  if (!mode) return null;
  const modeUpper = mode.toUpperCase();
  if (modeUpper === "AIR") return "Air";
  if (modeUpper === "SEA" || modeUpper === "BOAT") return "Sea";
  if (modeUpper === "GROUND") return "Ground";
  return mode;
}

// Checks shipping mode compatibility for normal-pricing shipment slots.
function normalPricingShippingModesCompatible(importModeFormatted, slotModeRaw) {
  const slotFmt = slotModeRaw != null && String(slotModeRaw).trim() !== "" ? formatShippingMode(slotModeRaw) : null;
  const imp = importModeFormatted || null;
  if (!imp && !slotFmt) return true;
  if (imp && slotFmt) return imp.toLowerCase() === slotFmt.toLowerCase();
  return true;
}

// Returns true when import mode conflicts with an existing shipment slot mode.
function importModeConflictsWithSlot(importModeFormatted, slotShippingMode) {
  const imp = importModeFormatted != null && String(importModeFormatted).trim() !== ""
    ? formatShippingMode(importModeFormatted)
    : null;
  const slot = slotShippingMode != null && String(slotShippingMode).trim() !== ""
    ? formatShippingMode(slotShippingMode)
    : null;
  if (!imp || !slot) return false;
  return imp.toLowerCase() !== slot.toLowerCase();
}

// Forces associated shipment entries to "Shipped" when linked shipment info exists.
function forceShippedStatusForAssociatedShipments(shipments = []) {
  return shipments.map((s) => {
    const hasShipmentId = s?.shipmentId != null && String(s.shipmentId).trim() !== "";
    const hasShipmentName = s?.shipmentName != null && String(s.shipmentName).trim() !== "";
    if (hasShipmentId && hasShipmentName) return { ...s, status: "Shipped" };
    return s;
  });
}

// Detects whether a line item is locked due to invoice or delivered state.
function isLineItemInvoiceLocked(lineItem = {}) {
  const hasTopLevelInvoiceId = lineItem?.invoiceId != null && String(lineItem.invoiceId).trim() !== "";
  const hasInvoicesArray = Array.isArray(lineItem?.Invoices) && lineItem.Invoices.length > 0;
  const shipments = Array.isArray(lineItem?.shipments) ? lineItem.shipments : [];
  const hasLockedShipment = shipments.some((s) => {
    const status = String(s?.status || "").trim().toLowerCase();
    const hasShipmentInvoiceId = s?.invoiceId != null && String(s.invoiceId).trim() !== "";
    return hasShipmentInvoiceId || status === "invoiced" || status === "delivered";
  });
  return hasTopLevelInvoiceId || hasInvoicesArray || hasLockedShipment;
}

// Parses import quantity and validates numeric input.
function parseImportQuantity(rawValue) {
  if (rawValue == null) return { value: null, valid: false };
  const txt = String(rawValue).trim();
  if (txt === "") return { value: null, valid: false };
  const parsed = Number.parseInt(txt, 10);
  if (!Number.isFinite(parsed)) return { value: null, valid: false };
  return { value: parsed, valid: true };
}

// Trims and normalizes imported SKU values to a comparable string.
function normalizeImportSku(raw) {
  if (raw == null) return "";
  return String(raw).trim();
}

// Collects comparable SKU values from a size row.
function sizeRowSkuValues(size) {
  if (!size || typeof size !== "object") return [];
  const vals = [
    size.csmSku,
  ];
  return [...new Set(vals.map((v) => normalizeImportSku(v)).filter(Boolean))];
}

// Compares a size row against an imported SKU.
function sizeRowMatchesImportSku(size, importSkuNorm) {
  const norm = normalizeImportSku(importSkuNorm);
  if (!norm) return false;
  return sizeRowSkuValues(size).some((v) => v === norm);
}

// Resolves the canonical SKU used for shipment reconciliation from a size row.
function canonicalSizeSkuForShipment(size) {
  if (!size || typeof size !== "object") return "";
  const csm =
    normalizeImportSku(size.csmSku) ||
    normalizeImportSku(size.CsmSku) ||
    normalizeImportSku(size.CSM_SKU);
  if (csm) return csm;
  const sku = normalizeImportSku(size.sku) || normalizeImportSku(size.Sku) || normalizeImportSku(size.SKU);
  if (sku) return sku;
  return (
    normalizeImportSku(size.clientSku) ||
    normalizeImportSku(size.ClientSku) ||
    normalizeImportSku(size.client_sku) ||
    ""
  );
}

// Finds whether any shipment slot matches a specific size name and quantity.
function lineItemShipmentSlotMatchesSizeQty(lineItem, sizeName, importQty) {
  const q = parseInt(importQty, 10);
  if (!Number.isFinite(q)) return false;
  const nameNorm = String(sizeName || "").trim();
  if (!nameNorm) return false;
  const shipments = Array.isArray(lineItem?.shipments) ? lineItem.shipments : [];
  for (const sh of shipments) {
    const sb = Array.isArray(sh.sizeBreakdown) ? sh.sizeBreakdown : [];
    for (const row of sb) {
      if (String(row?.sizeName || "").trim() === nameNorm && parseInt(row?.quantity || 0, 10) === q) {
        return true;
      }
    }
  }
  return false;
}

// Returns the first shipment slot index matching size name and quantity.
function findFirstShipmentSlotIndexForSizeAndQty(lineItem, sizeName, importQty) {
  const q = parseInt(importQty, 10);
  if (!Number.isFinite(q)) return -1;
  const nameNorm = String(sizeName || "").trim();
  if (!nameNorm) return -1;
  const shipments = Array.isArray(lineItem?.shipments) ? lineItem.shipments : [];
  for (let i = 0; i < shipments.length; i++) {
    const sb = Array.isArray(shipments[i].sizeBreakdown) ? shipments[i].sizeBreakdown : [];
    for (const row of sb) {
      if (String(row?.sizeName || "").trim() === nameNorm && parseInt(row?.quantity || 0, 10) === q) {
        return i;
      }
    }
  }
  return -1;
}

// Returns the first shipment slot index matching size name only.
function findFirstShipmentSlotIndexForSizeName(lineItem, sizeName) {
  const nameNorm = String(sizeName || "").trim();
  if (!nameNorm) return -1;
  const shipments = Array.isArray(lineItem?.shipments) ? lineItem.shipments : [];
  for (let i = 0; i < shipments.length; i++) {
    const sb = Array.isArray(shipments[i].sizeBreakdown) ? shipments[i].sizeBreakdown : [];
    for (const row of sb) {
      if (String(row?.sizeName || "").trim() === nameNorm) {
        return i;
      }
    }
  }
  return -1;
}

// Builds a lightweight shipment summary for import logging.
function summarizeLineItemShipmentsForLog(shipments = []) {
  return (Array.isArray(shipments) ? shipments : []).map((s) => ({
    id: s?.id,
    quantity: s?.quantity,
    status: s?.status,
    shipmentId: s?.shipmentId,
    shipmentName: s?.shipmentName,
    shippingMode: s?.shippingMode,
    sizeBreakdownLen: Array.isArray(s?.sizeBreakdown) ? s.sizeBreakdown.length : 0,
  }));
}

// Removes stale normal-pricing suspected products from a shipment.
async function removeNormalPricingSuspectedProduct(database, shipmentId, sku) {
  try {
    if (!shipmentId || !sku) return;
    const shipmentsCollection = database.collection("Shipment");
    const existing = await shipmentsCollection.findOne({ _id: new ObjectId(shipmentId) });
    if (!existing) return;
    const current = Array.isArray(existing.suspectedProducts) ? existing.suspectedProducts : [];
    if (current.length === 0) return;
    const next = current.filter((sp) => !(sp && sp.sku === sku && !sp.sizeName));
    if (next.length === current.length) return;
    const updateRes = await updateEntity("Shipment", shipmentId, { suspectedProducts: next });
    if (updateRes?.success) {
      callMakeWebhook("Shipment", "PUT", { suspectedProducts: next }, { id: shipmentId }, shipmentId)
        .catch(e => console.error("Webhook Shipment suspectedProducts cleanup:", e));
    }
  } catch (e) {
    console.error("Failed to remove normalPricing suspectedProducts:", e);
  }
}

// Parses multiple date formats and returns a normalized UTC date.
function parseDateString(dateString) {
  if (!dateString) return null;
  try {
    if (dateString instanceof Date) {
      if (isNaN(dateString.getTime())) { console.error("Invalid Date object"); return null; }
      return new Date(Date.UTC(dateString.getUTCFullYear(), dateString.getUTCMonth(), dateString.getUTCDate(), 0, 0, 0, 0));
    }
    const dateStr = String(dateString).trim();
    const tryParse = (y, m, d) => {
      if (m < 1 || m > 12 || d < 1 || d > 31) { console.error(`Invalid date: ${dateStr}`); return null; }
      return new Date(Date.UTC(y, m - 1, d, 0, 0, 0, 0));
    };
    if (/^\d{1,2}-\d{1,2}-\d{4}$/.test(dateStr)) {
      const [p0, p1, p2] = dateStr.split("-").map(Number);
      return p0 > 12 ? tryParse(p2, p1, p0) : tryParse(p2, p0, p1);
    }
    if (/^\d{4}\/\d{1,2}\/\d{1,2}$/.test(dateStr)) {
      const [y, m, d] = dateStr.split("/").map(Number);
      return tryParse(y, m, d);
    }
    if (/^\d{1,2}\/\d{1,2}\/\d{2}$/.test(dateStr)) {
      const [m, d, yy] = dateStr.split("/").map(Number);
      return tryParse(2000 + yy, m, d);
    }
    if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(dateStr)) {
      const [m, d, y] = dateStr.split("/").map(Number);
      return tryParse(y, m, d);
    }
    if (/^\d{4}-\d{1,2}-\d{1,2}$/.test(dateStr)) {
      const [y, m, d] = dateStr.split("-").map(Number);
      return tryParse(y, m, d);
    }
    if (dateStr.includes("T") || /^\d{4}-\d{2}-\d{2}/.test(dateStr)) {
      const datePart = dateStr.split("T")[0];
      if (/^\d{4}-\d{2}-\d{2}$/.test(datePart)) {
        const [y, m, d] = datePart.split("-").map(Number);
        return new Date(Date.UTC(y, m - 1, d, 0, 0, 0, 0));
      }
    }
    const parsedDate = new Date(dateStr);
    if (isNaN(parsedDate.getTime())) { console.error(`Failed to parse date: ${dateStr}`); return null; }
    return new Date(Date.UTC(parsedDate.getUTCFullYear(), parsedDate.getUTCMonth(), parsedDate.getUTCDate(), 0, 0, 0, 0));
  } catch (error) {
    console.error("Error parsing date:", dateString, error);
    return null;
  }
}

// Aggregates ImportDataRows status counts for progress reporting.
async function getStatusCounts(importDataRowsCollection, importDataId) {
  const result = await importDataRowsCollection.aggregate([
    { $match: { importDataId: new ObjectId(importDataId) } },
    { $group: { _id: "$status", count: { $sum: 1 } } }
  ]).toArray();

  const counts = { total: 0, success: 0, failure: 0, pending: 0, processing: 0, pending_reconciliation: 0 };
  for (const r of result) {
    if (r._id && counts.hasOwnProperty(r._id)) counts[r._id] = r.count;
    counts.total += r.count;
  }
  return counts;
}

// Processes one shipment import row and updates matching shipment/line item data.
async function processImportDataRow(rowDoc, columnMapping, dbClient, database, io, importDataId, fileName) {
  const importDataRowsCollection = database.collection("ImportDataRows");
  const item = rowDoc.data;
  const trackingData = { sizePricingLineItemIds: [], shipmentIds: [] };

  try {
    const poName = getMappedValue(item, "POName", columnMapping) || getMappedValue(item, "PONumber", columnMapping);
    const sku = getMappedValue(item, "SKU", columnMapping);
    const skuNorm = normalizeImportSku(sku);
    const shippingNumber = getMappedValue(item, "shippingNumber", columnMapping);
    const shipDateValue = getMappedValue(item, "shipDate", columnMapping);
    const quantityRaw = getMappedValue(item, "quantity", columnMapping);
    const parsedQuantity = parseImportQuantity(quantityRaw);
    const quantity = parsedQuantity.valid ? parsedQuantity.value : null;

    const requiredFields = [
      { value: poName, name: "POName" },
      { value: shippingNumber, name: "shippingNumber" },
      { value: sku, name: "sku" },
      { value: quantityRaw, name: "quantity" },
      { value: shipDateValue, name: "shipDate" }
    ];
    for (const field of requiredFields) {
      if (!(await validateRequiredField(field.value, field.name, importDataRowsCollection, rowDoc, io, importDataId, fileName))) return;
    }
    if (!parsedQuantity.valid) {
      await handleRowError(importDataRowsCollection, rowDoc, `Invalid quantity: ${quantityRaw}`, io, importDataId, fileName);
      return;
    }

    const poData = await getAggregatedData({ entityType: "PO", filter: { PONumber: poName }, pagination: { page: 1, pageSize: 1 } });
    if (!poData?.data?.length) {
      await handleRowError(importDataRowsCollection, rowDoc, `PO not found: ${poName}`, io, importDataId, fileName);
      return;
    }
    const po = poData.data[0];

    const productData = await getAggregatedData({ entityType: "Product", filter: { sku }, pagination: { page: 1, pageSize: 10 } });
    let product = null, lineItem = null, isSizePricingCase = false;

    if (productData?.data?.length > 0) {
      if (productData.data.length > 1) {
        await handleRowError(importDataRowsCollection, rowDoc, `Multiple products found with SKU: ${sku}`, io, importDataId, fileName);
        return;
      }
      product = productData.data[0];
      const lineItemsData = await getAggregatedData({
        entityType: "lineItem",
        filter: { poId: { $eq: new ObjectId(po._id) }, productId: { $eq: new ObjectId(product._id) } },
        pagination: { page: 1, pageSize: 1000 }
      });
      if (!lineItemsData?.data?.length) {
        await handleRowError(importDataRowsCollection, rowDoc, "No line items found", io, importDataId, fileName);
        return;
      }
      if (lineItemsData.data.length > 1) {
        await handleRowError(importDataRowsCollection, rowDoc, `Multiple line items found for PO: ${poName} and SKU: ${sku}`, io, importDataId, fileName);
        return;
      }
      lineItem = lineItemsData.data[0];
      isSizePricingCase = false;
    } else {
      const poOid = new ObjectId(String(po._id));
      const lineItemsCollection = database.collection("lineItem");
      const poLineItems = await lineItemsCollection.find({ poId: poOid }).limit(2000).toArray();
      const allMatchingLineItems = poLineItems.filter((li) => {
        const sizeBreakdown = Array.isArray(li.sizeBreakdown) ? li.sizeBreakdown : [];
        return sizeBreakdown.some((row) => sizeRowMatchesImportSku(row, skuNorm));
      });
      if (allMatchingLineItems.length === 0) {
        await handleRowError(
          importDataRowsCollection,
          rowDoc,
          `On ${poName}, no product line Includes this size SKU "${sku}". Check that the SKU matches a size on that order.`,
          io,
          importDataId,
          fileName
        );
        return;
      }
      let foundLineItem = null;
      for (const li of allMatchingLineItems) {
        const sizeBreakdown = Array.isArray(li.sizeBreakdown) ? li.sizeBreakdown : [];
        const matchingSize = sizeBreakdown.find((row) => sizeRowMatchesImportSku(row, skuNorm));
        if (!matchingSize) continue;
        if (quantity !== null) {
          if (lineItemShipmentSlotMatchesSizeQty(li, matchingSize.sizeName, quantity)) {
            foundLineItem = li;
            break;
          }
        } else if (!foundLineItem) {
          foundLineItem = li;
        }
      }
      if (!foundLineItem && quantity === null && allMatchingLineItems.length > 0) {
        const firstLi = allMatchingLineItems[0];
        const sizeBreakdown = Array.isArray(firstLi.sizeBreakdown) ? firstLi.sizeBreakdown : [];
        const matchingSize = sizeBreakdown.find((row) => sizeRowMatchesImportSku(row, skuNorm));
        if (matchingSize) foundLineItem = firstLi;
      }
      if (!foundLineItem) {
        const existingRowCheck = await importDataRowsCollection.findOne({ _id: rowDoc._id });
        if (existingRowCheck?.status !== "pending" && existingRowCheck?.status !== "success") {
          const qtyHint = quantity != null ? String(quantity) : "not given";
          await handleRowError(
            importDataRowsCollection,
            rowDoc,
            `On ${poName}, SKU "${sku}" is on the order, but no shipment line for that product lists this size with quantity ${qtyHint}. On each shipment row, the size and quantity must match what you put in the import file.`,
            io,
            importDataId,
            fileName
          );
        }
        return;
      }
      lineItem = foundLineItem;
      isSizePricingCase = true;
    }

    if (isLineItemInvoiceLocked(lineItem)) {
      await handleRowError(
        importDataRowsCollection,
        rowDoc,
        "This order line is already invoiced or delivered. It cannot be changed.",
        io,
        importDataId,
        fileName
      );
      return { trackingData };
    }

    const shipmentData = await getAggregatedData({ entityType: "Shipment", filter: { shippingNumber }, pagination: { page: 1, pageSize: 10 } });
    if (shipmentData?.data?.length > 1) {
      await handleRowError(importDataRowsCollection, rowDoc, `Multiple shipments found with shipping number: ${shippingNumber}`, io, importDataId, fileName);
      return;
    }

    const shippingModeRaw = getMappedValue(item, "shippingMode", columnMapping) || null;
    const shippingMode = shippingModeRaw ? formatShippingMode(shippingModeRaw) : null;
    let shipDate = parseDateString(shipDateValue);
    if (!shipDate) {
      await handleRowError(importDataRowsCollection, rowDoc, `Invalid shipDate: ${shipDateValue}`, io, importDataId, fileName);
      return;
    }

    const existingShipment = shipmentData?.data?.length === 1 ? shipmentData.data[0] : null;
    const existingShipmentMode = existingShipment?.shippingMode ? formatShippingMode(existingShipment.shippingMode) : null;
    let effectiveShippingMode = shippingMode;
    if (!effectiveShippingMode) {
      effectiveShippingMode = existingShipment ? existingShipmentMode : "Air";
    }

    let eta = null;
    if (shipDate && effectiveShippingMode) {
      eta = new Date(shipDate);
      const up = effectiveShippingMode.toUpperCase();
      if (up === "AIR") eta.setDate(eta.getDate() + 14);
      else if (up === "SEA" || up === "BOAT") eta.setDate(eta.getDate() + 35);
      else if (up === "GROUND") eta.setDate(eta.getDate() + 3);
    }

    let dbShipmentId;
    if (shipmentData?.data?.length === 1) {
      dbShipmentId = shipmentData.data[0]._id;
      trackingData.shipmentIds.push(dbShipmentId.toString());
      const shipmentUpdatePayload = { shippingNumber, shippingMode: effectiveShippingMode, shipDate, eta };
      const updateRes = await updateEntity("Shipment", dbShipmentId, shipmentUpdatePayload);
      if (!updateRes?.success) throw new Error(updateRes?.message || "Failed to update shipment");
      callMakeWebhook("Shipment", "PUT", shipmentUpdatePayload, { id: dbShipmentId }, dbShipmentId).catch(e => console.error("Webhook Shipment PUT:", e));
    } else if (shipmentData?.data?.length === 0) {
      const newShipment = { shippingNumber, shippingMode: effectiveShippingMode, shipDate, eta };
      const createRes = await createEntity("Shipment", newShipment);
      if (!createRes?.success || !createRes?.id) throw new Error(createRes?.message || "Failed to create shipment");
      dbShipmentId = createRes.id;
      trackingData.shipmentIds.push(dbShipmentId.toString());
      callMakeWebhook("Shipment", "POST", newShipment, { id: dbShipmentId }, dbShipmentId).catch(e => console.error("Webhook Shipment POST:", e));
    }

    let modifiedCount = 0, errorReason = null;

    for (const li of [lineItem]) {
      if (isLineItemInvoiceLocked(li)) {
        await handleRowError(
          importDataRowsCollection,
          rowDoc,
          "This Order Line is already invoiced or delivered. It cannot be changed.",
          io,
          importDataId,
          fileName
        );
        return { trackingData };
      }

      const existingShipments = Array.isArray(li.shipments) ? li.shipments : [];
      let shipmentsArray = [...existingShipments];
      const lineItemSizeBreakdown = Array.isArray(li.sizeBreakdown) ? li.sizeBreakdown : [];

      if (isSizePricingCase) {
        const matchingSizes = lineItemSizeBreakdown.filter((size) => sizeRowMatchesImportSku(size, skuNorm));
        if (matchingSizes.length === 0) {
          await handleRowError(
            importDataRowsCollection,
            rowDoc,
            `This product line does not include SKU "${sku}".`,
            io,
            importDataId,
            fileName
          );
          return { trackingData };
        }
        if (matchingSizes.length > 1) {
          const sizeNames = matchingSizes.map(s => s.sizeName).filter(Boolean).join(", ");
          await handleRowError(importDataRowsCollection, rowDoc, `More than one size on this line uses SKU "${sku}" (${sizeNames}). Enter quantity in your file so we can pick the correct shipment line.`, io, importDataId, fileName);
          return { trackingData };
        }

        const matchingSize = matchingSizes[0];
        const slotIndex = quantity !== null
          ? findFirstShipmentSlotIndexForSizeAndQty(li, matchingSize.sizeName, quantity)
          : findFirstShipmentSlotIndexForSizeName(li, matchingSize.sizeName);

        if (slotIndex < 0) {
          const noSlotMsg = quantity !== null
            ? `For this size, quantity ${quantity} does not match any shipment line on the product. Each shipment line must show this size with the same quantity as in your file.`
            : "No shipment line on this product lists this size. Add the size to a shipment row on the order first.";
          await handleRowError(importDataRowsCollection, rowDoc, noSlotMsg, io, importDataId, fileName);
          return { trackingData };
        }

        const slot = shipmentsArray[slotIndex];
        const shipmentStatus = slot.status || "";
        if (slot.invoiceId || shipmentStatus === "Invoiced" || shipmentStatus === "Delivered") {
          await handleRowError(
            importDataRowsCollection,
            rowDoc,
            `This shipment line is already invoiced or delivered (${shipmentStatus}). It cannot be updated.`,
            io,
            importDataId,
            fileName
          );
          return { trackingData };
        }

        if (importModeConflictsWithSlot(effectiveShippingMode, slot.shippingMode)) {
          await handleRowError(
            importDataRowsCollection,
            rowDoc,
            "The shipping mode in your file does not match the shipping mode on the shipment line for this size and quantity. .",
            io,
            importDataId,
            fileName
          );
          return { trackingData };
        }

        if (!dbShipmentId) {
          await handleRowError(
            importDataRowsCollection,
            rowDoc,
            "We could not create or find the shipment for this shipping number, so the order line was not updated.",
            io,
            importDataId,
            fileName
          );
          return { trackingData };
        }

        shipmentsArray[slotIndex] = {
          ...slot,
          shipmentId: dbShipmentId.toString(),
          shipmentName: shippingNumber,
          shipdate: shipDate ? new Date(Date.UTC(shipDate.getUTCFullYear(), shipDate.getUTCMonth(), shipDate.getUTCDate(), 0, 0, 0, 0)).toISOString().split("T")[0] : null,
          shippingMode: effectiveShippingMode || slot.shippingMode || null,
          status: "Shipped"
        };

      } else {
        const importQty = quantity !== null ? quantity : (parseInt(li.quantity || 0, 10) || 0);
        let perfectMatchFound = false, quantityFoundInAnyShipment = false, qtyMatchButModeConflict = false;

        for (let i = 0; i < shipmentsArray.length; i++) {
          const shipment = shipmentsArray[i];
          const shipmentStatus = shipment.status || "";
          if (shipment.invoiceId || shipmentStatus === "Invoiced" || shipmentStatus === "Delivered") {
            await handleRowError(importDataRowsCollection, rowDoc, `This shipment line is already invoiced or delivered (${shipmentStatus}). It cannot be updated.`, io, importDataId, fileName);
            return;
          }
          if (!shipment.sizeBreakdown || (Array.isArray(shipment.sizeBreakdown) && shipment.sizeBreakdown.length === 0)) {
            const shipmentQuantity = parseInt(shipment.quantity || 0) || 0;
            const modeMatches = normalPricingShippingModesCompatible(effectiveShippingMode, shipment.shippingMode);
            if (shipmentQuantity === importQty && modeMatches) {
              shipmentsArray[i] = {
                ...shipment,
                shipmentId: dbShipmentId ? dbShipmentId.toString() : null,
                shipmentName: shippingNumber,
                shipdate: shipDate ? new Date(Date.UTC(shipDate.getUTCFullYear(), shipDate.getUTCMonth(), shipDate.getUTCDate(), 0, 0, 0, 0)).toISOString().split("T")[0] : null,
                shippingMode: effectiveShippingMode || shipment.shippingMode || null,
                status: "Shipped"
              };
              perfectMatchFound = true;
              break;
            }
            if (shipmentQuantity === importQty && importModeConflictsWithSlot(effectiveShippingMode, shipment.shippingMode)) {
              qtyMatchButModeConflict = true;
            }
            if (shipmentQuantity > 0) quantityFoundInAnyShipment = true;
          }
        }

        if (!perfectMatchFound) {
          if (!dbShipmentId) {
            await handleRowError(
              importDataRowsCollection,
              rowDoc,
              "We could not find the shipment for this shipping number, so the order line was not updated.",
              io,
              importDataId,
              fileName
            );
            return { trackingData };
          }
          if (qtyMatchButModeConflict) {
            await handleRowError(
              importDataRowsCollection,
              rowDoc,
              "The shipping mode in your file does not match the shipment line for this quantity. The quantity is right, but both sides list a mode and they do not match.",
              io,
              importDataId,
              fileName
            );
            return { trackingData };
          }
          if (quantityFoundInAnyShipment) {
            await handleRowError(
              importDataRowsCollection,
              rowDoc,
              "No open shipment line on this product has exactly this quantity with a matching shipping mode. Check each shipment row on the order.",
              io,
              importDataId,
              fileName
            );
            return { trackingData };
          }
          await handleRowError(
            importDataRowsCollection,
            rowDoc,
            "For this product, the import only updates shipment lines that use a single total quantity (not a size-by-size list on the row). None of those lines matched your quantity.",
            io,
            importDataId,
            fileName
          );
          return { trackingData };
        }
      }

      if (shipmentsArray.length > existingShipments.length) throw new Error("Invalid state: shipmentsArray length increased.");

      const updatePayload = { shipments: forceShippedStatusForAssociatedShipments(shipmentsArray) };
      console.log("[import][lineItem PUT]", {
        lineItemId: String(li._id),
        rowId: String(rowDoc?._id || ""),
        importDataId: String(importDataId || ""),
        shipments: summarizeLineItemShipmentsForLog(updatePayload.shipments),
      });
      const updateRes = await updateEntity("lineItem", li._id, updatePayload);
      if (updateRes?.success) {
        modifiedCount += 1;
        callMakeWebhook("lineItem", "PUT", updatePayload, { id: li._id }, li._id).catch(e => console.error("Webhook lineItem PUT:", e));
        if (!isSizePricingCase && dbShipmentId && product?.sku) {
          await removeNormalPricingSuspectedProduct(database, dbShipmentId, product.sku);
        }
      } else {
        errorReason = `Failed to update line item: ${updateRes?.message || "Unknown error"}`;
      }
    }

    if (modifiedCount > 0) {
      await importDataRowsCollection.updateOne({ _id: rowDoc._id }, { $set: { status: "success", message: `Successfully processed ${modifiedCount} line item(s)`, processedAt: new Date() } });
    } else {
      const existingRowCheck = await importDataRowsCollection.findOne({ _id: rowDoc._id });
      if (existingRowCheck?.status === "pending_reconciliation" || existingRowCheck?.status === "pending" || existingRowCheck?.status === "success") return { trackingData };
      const errorMessage = errorReason || (lineItem?.sizeBreakdown?.length > 0
        ? `We could not update a shipment line for SKU "${sku}". Check quantity and shipping mode on each open shipment on the order.`
        : `We could not update a shipment line for quantity ${quantity !== null ? quantity : "N/A"} and shipping mode ${effectiveShippingMode || "N/A"}.`);
      await handleRowError(importDataRowsCollection, rowDoc, errorMessage, io, importDataId, fileName);
    }

    return { trackingData };
  } catch (error) {
    console.error(`Error processing row ${rowDoc._id}:`, error);
    const existingRow = await importDataRowsCollection.findOne({ _id: rowDoc._id });
    if (existingRow?.status === "pending_reconciliation" || existingRow?.status === "pending" || existingRow?.status === "success") return { trackingData };
    await importDataRowsCollection.updateOne({ _id: rowDoc._id }, { $set: { status: "failure", error: error.message, errorStack: error.stack, processedAt: new Date() } });
    io.emit("importDataProgress", { importDataId, fileName, rowId: rowDoc._id.toString(), status: "failure", error: error.message });
    return { trackingData };
  }
}

// Processes one Bulk Update import row against the configured schema mapping.
async function processBulkUpdateRow(rowDoc, importDataDoc, schemaDoc, database, io, importDataId, fileName) {
  const importDataRowsCollection = database.collection("ImportDataRows");
  const item = rowDoc.data || {};
  const columnMapping = importDataDoc.columnMapping || {};
  const schemaName = schemaDoc.entity || importDataDoc.schemaName;

  if (!schemaName) {
    await handleRowError(importDataRowsCollection, rowDoc, "Schema name is missing for Bulk Update import", io, importDataId, fileName);
    return {};
  }

  const mappedPayload = {};
  for (const [excelCol, fieldKey] of Object.entries(columnMapping)) {
    if (!fieldKey || fieldKey === "REMOVE_MAPPING") continue;
    const rawValue = item[excelCol];
    if (rawValue === null || rawValue === undefined) continue;
    const stringVal = typeof rawValue === "string" ? rawValue.trim() : String(rawValue).trim();
    if (stringVal === "") continue;
    if (fieldKey === "_id") mappedPayload._id = stringVal;
    else mappedPayload[fieldKey] = rawValue;
  }

  if (Object.keys(mappedPayload).length === 0) {
    await handleRowError(importDataRowsCollection, rowDoc, "No mapped data found for this row", io, importDataId, fileName);
    return {};
  }

  try {
    const arrayFields = { ...(schemaDoc.basicFields || {}), ...(schemaDoc.customFields || {}) };
    for (const [fieldKey, expectedType] of Object.entries(arrayFields)) {
      if (expectedType !== "array") continue;
      const currentVal = mappedPayload[fieldKey];
      if (typeof currentVal === "string") {
        const trimmed = currentVal.trim();
        if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
          try {
            const parsed = JSON.parse(trimmed);
            if (Array.isArray(parsed)) mappedPayload[fieldKey] = parsed;
            else { await handleRowError(importDataRowsCollection, rowDoc, `Field ${fieldKey} should be an array, but parsed JSON is ${typeof parsed}`, io, importDataId, fileName); return {}; }
          } catch (e) { await handleRowError(importDataRowsCollection, rowDoc, `Field ${fieldKey} contains invalid JSON array: ${e.message}`, io, importDataId, fileName); return {}; }
        }
      }
    }
  } catch (e) {
    await handleRowError(importDataRowsCollection, rowDoc, `Failed to normalize array fields: ${e.message}`, io, importDataId, fileName);
    return {};
  }

  const requiredFields = Array.isArray(schemaDoc.requiredFields) ? schemaDoc.requiredFields : [];
  for (const fieldPath of requiredFields) {
    const value = mappedPayload[fieldPath];
    if (value === undefined || value === null || (typeof value === "string" && value.trim() === "") || (Array.isArray(value) && value.length === 0)) {
      await handleRowError(importDataRowsCollection, rowDoc, `Missing required field: ${fieldPath}`, io, importDataId, fileName);
      return {};
    }
  }

  const exportFields = Array.isArray(schemaDoc.exportConfig?.fields) ? schemaDoc.exportConfig.fields : [];
  const escapeRegExp = (str) => str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

  for (const [fieldKey, currentValue] of Object.entries(mappedPayload)) {
    const fieldCfg = exportFields.find(f => f && f.key === fieldKey);
    if (!fieldCfg || !fieldCfg.entity || !fieldCfg.lookupDisplayField) continue;
    const collection = database.collection(fieldCfg.entity);
    const displayField = fieldCfg.displayField;
    const isArrayField = fieldCfg.isArray || (typeof fieldCfg.type === "string" && fieldCfg.type.endsWith("[]"));

    if (!isArrayField) {
      const raw = Array.isArray(currentValue) ? currentValue[0] : currentValue;
      const searchValue = String(raw).trim();
      if (!searchValue) continue;
      const doc = await collection.findOne({ [fieldCfg.lookupDisplayField]: { $regex: new RegExp(`^${escapeRegExp(searchValue)}$`, "i") } });
      if (!doc) { await handleRowError(importDataRowsCollection, rowDoc, `${fieldCfg.label || fieldKey} "${searchValue}" not found`, io, importDataId, fileName); return {}; }
      mappedPayload[fieldKey] = doc._id;
      if (displayField) mappedPayload[displayField] = doc[fieldCfg.lookupDisplayField] ?? doc[displayField] ?? searchValue;
    } else {
      const valuesArray = Array.isArray(currentValue) ? currentValue : String(currentValue).split(",").map(v => v.trim()).filter(v => v);
      if (!valuesArray.length) continue;
      const foundIds = [], foundNames = [], missing = [];
      for (const name of valuesArray) {
        const doc = await collection.findOne({ [fieldCfg.lookupDisplayField]: { $regex: new RegExp(`^${escapeRegExp(name)}$`, "i") } });
        if (!doc) { missing.push(name); continue; }
        foundIds.push(doc._id);
        foundNames.push(doc[fieldCfg.lookupDisplayField] ?? doc[displayField] ?? name);
      }
      if (missing.length > 0) { await handleRowError(importDataRowsCollection, rowDoc, `${fieldCfg.label || fieldKey} values not found: ${missing.join(", ")}`, io, importDataId, fileName); return {}; }
      mappedPayload[fieldKey] = foundIds;
      if (displayField) mappedPayload[displayField] = foundNames.join(fieldCfg.joinWith || ", ");
    }
  }

  try {
    let message, operation, entityId, webhookPayload;

    if (mappedPayload._id) {
      const { _id, ...updatePayload } = mappedPayload;
      const updateRes = await updateEntity(schemaName, _id, updatePayload);
      if (!updateRes || updateRes.success === false) {
        const notFound = typeof updateRes?.message === "string" && updateRes.message.toLowerCase().includes("no entity found with the provided id");
        if (notFound) {
          const createRes = await createEntity(schemaName, { ...updatePayload });
          if (!createRes || createRes.success === false) {
            let msg = createRes?.message || "Failed to create record";
            if (Array.isArray(createRes?.errors) && createRes.errors.length > 0) msg = `Validation failed: ${createRes.errors.join(" | ")}`;
            throw new Error(msg);
          }
          message = `Created new ${schemaName} record (because _id did not match any document)`;
          operation = "POST"; entityId = createRes.id; webhookPayload = { ...updatePayload };
        } else {
          let msg = updateRes?.message || "Failed to update record";
          if (Array.isArray(updateRes?.errors) && updateRes.errors.length > 0) msg = `Validation failed: ${updateRes.errors.join(" | ")}`;
          throw new Error(msg);
        }
      } else {
        message = `Updated existing ${schemaName} record`;
        operation = "PUT"; entityId = updateRes.id || _id; webhookPayload = updatePayload;
      }
    } else {
      const createRes = await createEntity(schemaName, mappedPayload);
      if (!createRes || createRes.success === false) {
        let msg = createRes?.message || "Failed to create record";
        if (Array.isArray(createRes?.errors) && createRes.errors.length > 0) msg = `Validation failed: ${createRes.errors.join(" | ")}`;
        throw new Error(msg);
      }
      message = `Created new ${schemaName} record`;
      operation = "POST"; entityId = createRes.id; webhookPayload = mappedPayload;
    }

    callMakeWebhook(schemaName, operation, webhookPayload, { id: entityId }, entityId)
      .catch(e => console.error(`Webhook Bulk Update ${schemaName} (${operation}):`, e));

    await importDataRowsCollection.updateOne({ _id: rowDoc._id }, { $set: { status: "success", message, processedAt: new Date() } });
    return {};
  } catch (error) {
    await handleRowError(importDataRowsCollection, rowDoc, error.message || "Bulk update failed", io, importDataId, fileName);
    return {};
  }
}

let isProcessingShipment = false;
let isProcessingBulkUpdate = false;
const STUCK_PROCESSING_MINUTES = Number.parseInt(process.env.IMPORT_STUCK_PROCESSING_MINUTES || "15", 10);

// Claims and processes pending shipment import rows in safe batches.
async function checkAndProcessImportData(io) {
  if (isProcessingShipment) { console.log("⏸️ ImportData processing already in progress, skipping"); return; }
  isProcessingShipment = true;
  const dbClient = new MongoClient(process.env.MONGODB_CONNECTION_STRING);

  try {
    await dbClient.connect();
    const database = dbClient.db(process.env.DB_NAME);
    const importDataCollection = database.collection("ImportData");
    const importDataRowsCollection = database.collection("ImportDataRows");

    const stuckBefore = new Date(Date.now() - STUCK_PROCESSING_MINUTES * 60 * 1000);
    const recovered = await importDataRowsCollection.updateMany(
      {
        status: "processing",
        processingStartedAt: { $lt: stuckBefore },
      },
      {
        $set: { status: "pending" },
        $unset: { processingStartedAt: "" },
      }
    );
    if (recovered.modifiedCount > 0) {
      console.log(`♻️ Recovered ${recovered.modifiedCount} stuck processing rows`);
    }

    const candidateRows = await importDataRowsCollection.find({ status: "pending" }).limit(100).toArray();
    if (candidateRows.length === 0) { console.log("⏰ No pending ImportDataRows to process"); return; }

    const processingStartedAt = new Date();
    const candidateIds = candidateRows.map(row => row._id);

    const claimResult = await importDataRowsCollection.updateMany(
      { _id: { $in: candidateIds }, status: "pending" },
      { $set: { status: "processing", processingStartedAt } }
    );
    if (claimResult.modifiedCount === 0) { console.log("⏰ No rows available (claimed by another instance)"); return; }

    const claimedRows = await importDataRowsCollection.find({ _id: { $in: candidateIds }, status: "processing", processingStartedAt }).toArray();
    if (claimedRows.length === 0) { console.log("⏰ No rows successfully claimed"); return; }

    console.log(`📋 Claimed ${claimedRows.length} ImportDataRows`);

    const rowsByImportData = {};
    for (const row of claimedRows) {
      if (!rowsByImportData[row.importDataId]) rowsByImportData[row.importDataId] = [];
      rowsByImportData[row.importDataId].push(row);
    }

    for (const [importDataId, rows] of Object.entries(rowsByImportData)) {
      const importDataDoc = await importDataCollection.findOne({ _id: new ObjectId(importDataId) });
      if (!importDataDoc) {
        for (const rowDoc of rows) await handleRowError(importDataRowsCollection, rowDoc, "ImportData document not found", io, importDataId.toString(), "");
        continue;
      }

      if (importDataDoc.type === "Bulk Update" || importDataDoc.schemaName) {
        console.log(`⏭️ Skipping Bulk Update ${importDataId} in Shipment processor`);
        await importDataRowsCollection.updateMany(
          { _id: { $in: rows.map(r => r._id) } },
          { $set: { status: "pending" }, $unset: { processingStartedAt: "" } }
        );
        continue;
      }

      const columnMapping = importDataDoc.columnMapping || {};
      const fileName = importDataDoc.fileName;
      console.log(` Processing ${rows.length} rows for ${importDataId} (${fileName})`);

      const initialCounts = await getStatusCounts(importDataRowsCollection, importDataId);
      const totalRows = initialCounts.total;
      const progressCounts = {
        total: initialCounts.total,
        success: initialCounts.success,
        failure: initialCounts.failure,
        pending: initialCounts.pending,
        processing: initialCounts.processing,
        pending_reconciliation: initialCounts.pending_reconciliation,
      };

      const allTrackingData = { sizePricingLineItemIds: [], shipmentIds: [] };

      for (const rowDoc of rows) {
        try {
          const result = await processImportDataRow(rowDoc, columnMapping, dbClient, database, io, importDataId.toString(), fileName);
          if (result?.trackingData) {
            allTrackingData.sizePricingLineItemIds.push(...result.trackingData.sizePricingLineItemIds);
            allTrackingData.shipmentIds.push(...result.trackingData.shipmentIds);
          }

          const latestRow = await importDataRowsCollection.findOne({ _id: rowDoc._id });
          if (progressCounts.processing > 0) progressCounts.processing -= 1;
          const latestStatus = latestRow?.status;
          if (latestStatus && Object.prototype.hasOwnProperty.call(progressCounts, latestStatus)) {
            progressCounts[latestStatus] += 1;
          }

          io.emit("importDataProgress", {
            importDataId: importDataId.toString(), fileName,
            processed: progressCounts.success + progressCounts.failure,
            total: totalRows,
            success: progressCounts.success,
            errors: progressCounts.failure,
            remaining: progressCounts.pending + progressCounts.processing,
            rowId: rowDoc._id.toString(),
            status: latestRow?.status,
            error: latestRow?.error
          });

          if (progressCounts.pending === 0 && progressCounts.processing === 0) {
            const uniqueLineItemIds = [...new Set(allTrackingData.sizePricingLineItemIds)];
            const uniqueShipmentIds = [...new Set(allTrackingData.shipmentIds)];
            if (uniqueLineItemIds.length > 0 && uniqueShipmentIds.length > 0) {
              await reconcileSizePricingLineItems(database, io, importDataId.toString(), fileName, uniqueLineItemIds, uniqueShipmentIds);
            }

            const finalCounts = await getStatusCounts(importDataRowsCollection, importDataId);
            const allRows = await importDataRowsCollection.find(
              { importDataId: new ObjectId(importDataId) },
              { projection: { rowIndex: 1, status: 1, message: 1, error: 1, "data.POName": 1, "data.PONumber": 1, "data.SKU": 1, "data.shippingNumber": 1 } }
            ).limit(500).toArray();

            await importDataCollection.updateOne(
              { _id: new ObjectId(importDataId) },
              { $set: { processingStatus: "completed", processedAt: new Date(), counts: { total: totalRows, success: finalCounts.success, failure: finalCounts.failure, pending: finalCounts.pending_reconciliation } } }
            );

            io.emit("importDataComplete", {
              importDataId: importDataId.toString(), fileName,
              summary: { total: totalRows, success: finalCounts.success, error: finalCounts.failure },
              results: allRows.map(row => ({ index: row.rowIndex, status: row.status, message: row.message || row.error, poName: row.data?.POName || row.data?.PONumber, sku: row.data?.SKU, shippingNumber: row.data?.shippingNumber }))
            });
            console.log(`✅ Completed ${importDataId}: ${finalCounts.success} success, ${finalCounts.failure} failures / ${totalRows} total`);
          }
        } catch (error) {
          console.error(`Error processing row ${rowDoc._id}:`, error);
          await handleRowError(importDataRowsCollection, rowDoc, error.message, io, importDataId.toString(), fileName);
          await importDataRowsCollection.updateOne({ _id: rowDoc._id }, { $set: { errorStack: error.stack } });
        }
      }
    }
  } catch (error) {
    console.error("Error in checkAndProcessImportData:", error);
  } finally {
    try { await dbClient.close(); } catch (e) { console.error("Error closing DB:", e); }
    isProcessingShipment = false;
  }
}

// Claims and processes pending Bulk Update rows in safe batches.
async function checkAndProcessBulkUpdateImportData(io) {
  if (isProcessingBulkUpdate) { console.log("⏸️ Bulk Update processing already in progress, skipping"); return; }
  isProcessingBulkUpdate = true;
  const dbClient = new MongoClient(process.env.MONGODB_CONNECTION_STRING);

  try {
    await dbClient.connect();
    const database = dbClient.db(process.env.DB_NAME);
    const importDataCollection = database.collection("ImportData");
    const schemaCollection = database.collection("Schema");
    const importDataRowsCollection = database.collection("ImportDataRows");

    const stuckBefore = new Date(Date.now() - STUCK_PROCESSING_MINUTES * 60 * 1000);
    const recovered = await importDataRowsCollection.updateMany(
      {
        status: "processing",
        processingStartedAt: { $lt: stuckBefore },
      },
      {
        $set: { status: "pending" },
        $unset: { processingStartedAt: "" },
      }
    );
    if (recovered.modifiedCount > 0) {
      console.log(`♻️ Recovered ${recovered.modifiedCount} stuck bulk-update rows`);
    }

    const candidateRows = await importDataRowsCollection.find({ status: "pending" }).limit(100).toArray();
    if (candidateRows.length === 0) { console.log("⏰ No pending Bulk Update rows"); return; }

    const processingStartedAt = new Date();
    const candidateIds = candidateRows.map(row => row._id);

    const claimResult = await importDataRowsCollection.updateMany(
      { _id: { $in: candidateIds }, status: "pending" },
      { $set: { status: "processing", processingStartedAt } }
    );
    if (claimResult.modifiedCount === 0) { console.log("⏰ No Bulk Update rows available"); return; }

    const claimedRows = await importDataRowsCollection.find({ _id: { $in: candidateIds }, status: "processing", processingStartedAt }).toArray();
    if (claimedRows.length === 0) { console.log("⏰ No Bulk Update rows claimed"); return; }

    console.log(`📋 Claimed ${claimedRows.length} Bulk Update rows`);

    const rowsByImportData = {};
    for (const row of claimedRows) {
      if (!rowsByImportData[row.importDataId]) rowsByImportData[row.importDataId] = [];
      rowsByImportData[row.importDataId].push(row);
    }

    for (const [importDataId, rows] of Object.entries(rowsByImportData)) {
      const importDataDoc = await importDataCollection.findOne({ _id: new ObjectId(importDataId) });
      if (!importDataDoc) {
        for (const rowDoc of rows) await handleRowError(importDataRowsCollection, rowDoc, "ImportData document not found", io, importDataId.toString(), "");
        continue;
      }

      if (importDataDoc.type !== "Bulk Update" && !importDataDoc.schemaName) {
        await importDataRowsCollection.updateMany(
          { _id: { $in: rows.map(r => r._id) } },
          { $set: { status: "pending" }, $unset: { processingStartedAt: "" } }
        );
        continue;
      }

      const fileName = importDataDoc.fileName;
      const schemaName = importDataDoc.schemaName;
      const schemaQuery = [];
      if (schemaName) {
        schemaQuery.push({ entity: schemaName }, { name: schemaName });
        if (ObjectId.isValid(schemaName)) schemaQuery.push({ _id: new ObjectId(schemaName) });
      }
      const schemaDoc = await schemaCollection.findOne(schemaQuery.length ? { $or: schemaQuery } : { entity: schemaName });

      if (!schemaDoc) {
        console.error(`Schema not found: ${schemaName}`);
        for (const rowDoc of rows) await handleRowError(importDataRowsCollection, rowDoc, "Schema not found for Bulk Update import", io, importDataId.toString(), fileName);
        continue;
      }

      console.log(` Processing ${rows.length} Bulk Update rows for ${importDataId} (${fileName})`);

      const initialCounts = await getStatusCounts(importDataRowsCollection, importDataId);
      const totalRows = initialCounts.total;
      const progressCounts = {
        total: initialCounts.total,
        success: initialCounts.success,
        failure: initialCounts.failure,
        pending: initialCounts.pending,
        processing: initialCounts.processing,
        pending_reconciliation: initialCounts.pending_reconciliation,
      };

      for (const rowDoc of rows) {
        try {
          await processBulkUpdateRow(rowDoc, importDataDoc, schemaDoc, database, io, importDataId.toString(), fileName);

          const latestRow = await importDataRowsCollection.findOne({ _id: rowDoc._id });
          if (progressCounts.processing > 0) progressCounts.processing -= 1;
          const latestStatus = latestRow?.status;
          if (latestStatus && Object.prototype.hasOwnProperty.call(progressCounts, latestStatus)) {
            progressCounts[latestStatus] += 1;
          }

          io.emit("importDataProgress", {
            importDataId: importDataId.toString(), fileName,
            processed: progressCounts.success + progressCounts.failure,
            total: totalRows,
            success: progressCounts.success,
            errors: progressCounts.failure,
            remaining: progressCounts.pending + progressCounts.processing,
            rowId: rowDoc._id.toString(),
            status: latestRow?.status,
            error: latestRow?.error
          });

          if (progressCounts.pending === 0 && progressCounts.processing === 0) {
            const finalCounts = await getStatusCounts(importDataRowsCollection, importDataId);
            const allRows = await importDataRowsCollection.find(
              { importDataId: new ObjectId(importDataId) },
              { projection: { rowIndex: 1, status: 1, message: 1, error: 1 } }
            ).limit(500).toArray();

            await importDataCollection.updateOne(
              { _id: new ObjectId(importDataId) },
              { $set: { processingStatus: "completed", processedAt: new Date(), counts: { total: totalRows, success: finalCounts.success, failure: finalCounts.failure, pending: finalCounts.pending_reconciliation } } }
            );

            io.emit("importDataComplete", {
              importDataId: importDataId.toString(), fileName,
              summary: { total: totalRows, success: finalCounts.success, error: finalCounts.failure },
              results: allRows.map(row => ({ index: row.rowIndex, status: row.status, message: row.message || row.error }))
            });
            console.log(`✅ Completed Bulk Update ${importDataId}: ${finalCounts.success} success, ${finalCounts.failure} failures / ${totalRows} total`);
          }
        } catch (error) {
          console.error(`Error processing Bulk Update row ${rowDoc._id}:`, error);
          await handleRowError(importDataRowsCollection, rowDoc, error.message, io, importDataId.toString(), fileName);
          await importDataRowsCollection.updateOne({ _id: rowDoc._id }, { $set: { errorStack: error.stack } });
        }
      }
    }
  } catch (error) {
    console.error("Error in checkAndProcessBulkUpdateImportData:", error);
  } finally {
    try { await dbClient.close(); } catch (e) { console.error("Error closing DB (Bulk Update):", e); }
    isProcessingBulkUpdate = false;
  }
}

// Reconciles size-pricing line items with tracked shipments after import processing.
async function reconcileSizePricingLineItems(database, io, importDataId, fileName, trackedLineItemIds = [], trackedShipmentIds = []) {
  try {
    if (trackedLineItemIds.length === 0 || trackedShipmentIds.length === 0) { console.log("No tracked items for reconciliation"); return; }

    const shipmentsCollection = database.collection("Shipment");
    const lineItemsCollection = database.collection("lineItem");
    const importDataRowsCollection = database.collection("ImportDataRows");

    const allTrackedShipments = await shipmentsCollection.find({ _id: { $in: trackedShipmentIds.map(id => new ObjectId(id)) } }).toArray();
    if (allTrackedShipments.length === 0) { console.log("No tracked shipments for reconciliation"); return; }

    let reconciledCount = 0;
    const touchedRowIds = new Set();
    const pendingRows = await importDataRowsCollection.find({
      importDataId: new ObjectId(importDataId),
      status: "pending_reconciliation",
    }).toArray();
    const pendingRowsBySku = new Map();
    for (const row of pendingRows) {
      const rowSku = String(row?.data?.SKU || "").trim();
      if (!rowSku) continue;
      if (!pendingRowsBySku.has(rowSku)) pendingRowsBySku.set(rowSku, []);
      pendingRowsBySku.get(rowSku).push(row);
    }

    const markRowsFailureBySkus = async (skus = [], message) => {
      const uniqueSkus = [...new Set((Array.isArray(skus) ? skus : []).map((s) => String(s || "").trim()).filter(Boolean))];
      for (const sku of uniqueSkus) {
        const rows = pendingRowsBySku.get(sku) || [];
        for (const row of rows) {
          const rowId = String(row._id);
          if (touchedRowIds.has(rowId)) continue;
          await handleRowError(importDataRowsCollection, row, message, io, importDataId, fileName);
          touchedRowIds.add(rowId);
        }
      }
    };

    const markRowsSuccessBySkus = async (skus = [], message) => {
      const uniqueSkus = [...new Set((Array.isArray(skus) ? skus : []).map((s) => String(s || "").trim()).filter(Boolean))];
      for (const sku of uniqueSkus) {
        const rows = pendingRowsBySku.get(sku) || [];
        for (const row of rows) {
          const rowId = String(row._id);
          if (touchedRowIds.has(rowId)) continue;
          await importDataRowsCollection.updateOne(
            { _id: row._id, status: "pending_reconciliation" },
            { $set: { status: "success", message, processedAt: new Date() } }
          );
          io.emit("importDataProgress", { importDataId, fileName, rowId, status: "success", message });
          touchedRowIds.add(rowId);
        }
      }
    };

    const normalizeQty = (v) => {
      const n = Number.parseInt(v, 10);
      return Number.isFinite(n) ? n : 0;
    };

    const toIsoDate = (raw) => {
      if (!raw) return null;
      const d = raw instanceof Date ? raw : new Date(raw);
      if (Number.isNaN(d.getTime())) return null;
      return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), 0, 0, 0, 0)).toISOString().split("T")[0];
    };

    const lineItems = await lineItemsCollection.find({
      _id: { $in: trackedLineItemIds.map(id => new ObjectId(id)) },
      shipments: { $exists: true, $ne: [] },
      sizeBreakdown: { $exists: true, $ne: [] }
    }).toArray();

    for (const lineItem of lineItems) {
      if (isLineItemInvoiceLocked(lineItem)) {
        const blockedSkus = (Array.isArray(lineItem.sizeBreakdown) ? lineItem.sizeBreakdown : [])
          .map((s) => canonicalSizeSkuForShipment(s))
          .filter(Boolean);
        await markRowsFailureBySkus(blockedSkus, "This order line is already invoiced or delivered, so it cannot be matched automatically.");
        continue;
      }

      const lineItemSizeBreakdown = Array.isArray(lineItem.sizeBreakdown) ? lineItem.sizeBreakdown : [];
      if (lineItemSizeBreakdown.length === 0) continue;
      const shipmentsArray = Array.isArray(lineItem.shipments) ? [...lineItem.shipments] : [];

      for (let i = 0; i < shipmentsArray.length; i++) {
        const shipmentObj = shipmentsArray[i];
        const shipmentSizeBreakdown = Array.isArray(shipmentObj.sizeBreakdown) ? shipmentObj.sizeBreakdown : [];

        const shipmentStatus = shipmentObj.status || "";
        if (shipmentObj.invoiceId ||shipmentStatus === "Invoiced" || shipmentStatus === "Delivered") {
          const sizeNamesBlocked = shipmentSizeBreakdown.map(s => s.sizeName).filter(Boolean);
          const blockedSkus = lineItemSizeBreakdown
            .filter(s => sizeNamesBlocked.includes(s.sizeName))
            .map(s => canonicalSizeSkuForShipment(s))
            .filter(Boolean);
          await markRowsFailureBySkus(
            blockedSkus,
            `Line Item shipment has invalid status (${shipmentStatus}). Cannot reconcile while Invoiced or Delivered.`
          );
          continue;
        }
        if (shipmentSizeBreakdown.length === 0) continue;
        if (shipmentSizeBreakdown.some((s) => !String(s?.sizeName || "").trim())) {
          const relatedSkus = lineItemSizeBreakdown.map((s) => canonicalSizeSkuForShipment(s)).filter(Boolean);
          await markRowsFailureBySkus(
            relatedSkus,
            "Some size names are missing on a shipment line, so automatic matching was skipped."
          );
          continue;
        }

        const tryMatchShipments = (shipmentsList) => {
          const matchingShipments = [];
          for (const trackedShipment of shipmentsList) {
            const trackedSuspected = Array.isArray(trackedShipment.suspectedProducts) ? [...trackedShipment.suspectedProducts] : [];
            if (trackedSuspected.length === 0) continue;
            let allSizesMatch = true;
            const tempMatched = [], tempRemaining = [...trackedSuspected];
            for (const size of shipmentSizeBreakdown) {
              if (!size.sizeName) { allSizesMatch = false; break; }
              const lineItemSize = lineItemSizeBreakdown.find(
                (ls) =>
                  String(ls?.sizeName || "").trim() === String(size?.sizeName || "").trim() &&
                  canonicalSizeSkuForShipment(ls) !== ""
              );
              if (!lineItemSize) { allSizesMatch = false; break; }
              const lineSku = canonicalSizeSkuForShipment(lineItemSize);
              const idx = tempRemaining.findIndex(
                (sp) =>
                  String(sp?.sizeName || "").trim() === String(size?.sizeName || "").trim() &&
                  normalizeImportSku(sp?.sku) === lineSku &&
                  normalizeQty(sp?.quantity) === normalizeQty(size?.quantity)
              );
              if (idx === -1) { allSizesMatch = false; break; }
              tempMatched.push(tempRemaining[idx]);
              tempRemaining.splice(idx, 1);
            }
            if (allSizesMatch && tempMatched.length === shipmentSizeBreakdown.length) {
              const trackedMode = trackedShipment?.shippingMode ? formatShippingMode(trackedShipment.shippingMode) : null;
              const slotMode = shipmentObj?.shippingMode ? formatShippingMode(shipmentObj.shippingMode) : null;
              const modeMatches = !slotMode || !trackedMode || trackedMode.toLowerCase() === slotMode.toLowerCase();
              if (modeMatches) matchingShipments.push({ shipment: trackedShipment, matchedSuspectedProducts: tempMatched });
            }
          }
          return matchingShipments;
        };

        const matchingShipments = tryMatchShipments(allTrackedShipments);
        let matchedShipment = null, matchedSuspectedProducts = [];

        if (matchingShipments.length === 1) {
          matchedShipment = matchingShipments[0].shipment;
          matchedSuspectedProducts = matchingShipments[0].matchedSuspectedProducts;
        } else if (matchingShipments.length > 1) {
          let earliestDate = null, selectedMatch = null;
          for (const match of matchingShipments) {
            const d = match.shipment.exfactoryDate || match.shipment.shipDate;
            if (d) {
              const dateObj = d instanceof Date ? d : new Date(d);
              if (!Number.isNaN(dateObj.getTime()) && (!earliestDate || dateObj < earliestDate)) {
                earliestDate = dateObj;
                selectedMatch = match;
              }
            } else if (!selectedMatch) selectedMatch = match;
          }
          const chosen = selectedMatch || matchingShipments[0];
          matchedShipment = chosen.shipment; matchedSuspectedProducts = chosen.matchedSuspectedProducts;
        }

        if (matchedShipment) {
          shipmentsArray[i] = {
            ...shipmentObj,
            shipmentId: matchedShipment._id.toString(),
            shipmentName: matchedShipment.shippingNumber || matchedShipment.name || "",
            shipdate: toIsoDate(matchedShipment.shipDate),
            shippingMode: formatShippingMode(matchedShipment.shippingMode) || formatShippingMode(shipmentObj.shippingMode) || "Air",
            status: "Shipped"
          };

          const updatePayload = { shipments: forceShippedStatusForAssociatedShipments(shipmentsArray) };
          console.log("[reconcile][lineItem PUT]", {
            lineItemId: String(lineItem._id),
            importDataId: String(importDataId || ""),
            matchedShipmentId: String(matchedShipment?._id || ""),
            shipments: summarizeLineItemShipmentsForLog(updatePayload.shipments),
          });
          const updateRes = await updateEntity("lineItem", lineItem._id, updatePayload);
          if (updateRes?.success) {
            reconciledCount++;
            const updatedSuspectedProducts = (Array.isArray(matchedShipment.suspectedProducts) ? matchedShipment.suspectedProducts : []).filter(sp =>
              !matchedSuspectedProducts.some(msp =>
                String(msp?.sizeName || "").trim() === String(sp?.sizeName || "").trim()
                && String(msp?.sku || "").trim() === String(sp?.sku || "").trim()
                && normalizeQty(msp?.quantity) === normalizeQty(sp?.quantity)
              )
            );
            await updateEntity("Shipment", matchedShipment._id, { suspectedProducts: updatedSuspectedProducts });

            const updatedShipment = await shipmentsCollection.findOne({ _id: matchedShipment._id });
            if (updatedShipment) {
              const idx = allTrackedShipments.findIndex(s => s._id.toString() === matchedShipment._id.toString());
              if (idx !== -1) allTrackedShipments[idx] = updatedShipment;
            }

            callMakeWebhook("lineItem", "PUT", updatePayload, { id: lineItem._id }, lineItem._id).catch(e => console.error("Webhook reconcile lineItem:", e));
            callMakeWebhook("Shipment", "PUT", { suspectedProducts: updatedSuspectedProducts }, { id: matchedShipment._id }, matchedShipment._id).catch(e => console.error("Webhook reconcile Shipment:", e));

            const relatedCsmSkus = matchedSuspectedProducts.map(msp => msp.sku).filter(Boolean);
            if (relatedCsmSkus.length > 0) {
              await markRowsSuccessBySkus(
                relatedCsmSkus,
                `Successfully reconciled and associated with shipment ${matchedShipment.shippingNumber || matchedShipment._id.toString()}`
              );
            }
          }
        } else {
          const sizeNames = shipmentSizeBreakdown.map(s => s.sizeName).filter(Boolean);
          const sizeQuantities = shipmentSizeBreakdown.map(s => `${s.sizeName}(${s.quantity || 0})`).join(", ");
          const relatedCsmSkus = lineItemSizeBreakdown
            .filter(s => sizeNames.includes(s.sizeName))
            .map(s => canonicalSizeSkuForShipment(s))
            .filter(Boolean);

          if (relatedCsmSkus.length > 0) {
            await markRowsFailureBySkus(
              relatedCsmSkus,
              `No shipment on file matched this order for sizes: ${sizeQuantities}. Check the shipment and the items flagged for review.`
            );
          }
        }
      }
    }

    const remainingPendingRows = await importDataRowsCollection.find({
      importDataId: new ObjectId(importDataId),
      status: "pending_reconciliation",
    }).toArray();
    for (const pendingRow of remainingPendingRows) {
      const rowId = String(pendingRow._id);
      if (touchedRowIds.has(rowId)) continue;
      await handleRowError(
        importDataRowsCollection,
        pendingRow,
        "After automatic review, no shipment could be linked to this row. Please check the order and shipment details manually.",
        io,
        importDataId,
        fileName
      );
      touchedRowIds.add(rowId);
    }

    console.log(`✅ Reconciliation completed: ${reconciledCount} sizePricing line items associated`);
  } catch (error) {
    console.error("Error in reconcileSizePricingLineItems:", error);
  }
}

module.exports = {
  checkAndProcessImportData,
  processImportDataRow,
  reconcileSizePricingLineItems,
  checkAndProcessBulkUpdateImportData,
  processBulkUpdateRow,
};