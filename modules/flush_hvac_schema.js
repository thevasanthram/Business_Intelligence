async function flush_hvac_schema(sql_request) {
  try {
    const flushing_query = `
    DROP TABLE gross_profit
    DROP TABLE purchase_order
    DROP TABLE cogs_material
    DROP TABLE cogs_labor
    DROP TABLE technician
    DROP TABLE cogs_equipment
    DROP TABLE cogs_service
    DROP TABLE sku_details
    DROP TABLE vendor
    DROP TABLE invoice
    DROP TABLE job_details
    DROP TABLE location
    DROP TABLE customer_details
    DROP TABLE business_unit
    DROP TABLE legal_entity`;

    const createTable = await sql_request.query(flushing_query);

    // console.log(fomatted_table_name + " created");
    console.log("HVAC Tables deleted");
  } catch (error) {
    console.log("HVAC Tables deletion failed. Trying Again!", error);
    // create_flat_tables(sql_request, flattenedObj, table_name);
  }
}

module.exports = flush_hvac_schema;