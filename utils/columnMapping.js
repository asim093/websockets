
function getExcelColumnForSchemaField(columnMapping, schemaField) {
  if (!columnMapping) return null;
  for (const [excelCol, mappedField] of Object.entries(columnMapping)) {
    if (mappedField === schemaField) {
      return excelCol;
    }
  }
  return null;
}

function getMappedValue(item, schemaField, columnMapping) {
  const excelColumn = getExcelColumnForSchemaField(columnMapping, schemaField);
  if (excelColumn && item[excelColumn] !== undefined) {
    return item[excelColumn];
  }
  if (item[schemaField] !== undefined) {
    return item[schemaField];
  }
  const variations = {
    'POName': ['PONumber', 'PO', 'Client Po', 'poName'],
    'SKU': ['sku', 'Sku'],
    'shippingNumber': ['Shipment', 'ShipmentNo', 'shippingNumber', 'ShippingNumber']
  };
  if (variations[schemaField]) {
    for (const variant of variations[schemaField]) {
      if (item[variant] !== undefined) {
        return item[variant];
      }
    }
  }
  return null;
}

module.exports = {
  getExcelColumnForSchemaField,
  getMappedValue
};

