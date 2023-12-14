async function flush_hvac_data(sql_request) {
  try {
    const flushing_query = `
    DELETE FROM gross_profit
    DELETE FROM purchase_order
    DELETE FROM cogs_material
    DELETE FROM cogs_labor
    DELETE FROM non_job_appointments
    DELETE FROM appointment_assignments
    DELETE FROM technician
    DELETE FROM cogs_equipment
    DELETE FROM cogs_service
    DELETE FROM sku_details
    DELETE FROM vendor
    DELETE FROM invoice
    DELETE FROM sales_details
    DELETE FROM appointments
    DELETE FROM job_details
    DELETE FROM call_details
    DELETE FROM projects
    DELETE FROM location
    DELETE FROM customer_details
    DELETE FROM bookings
    DELETE FROM campaigns
    DELETE FROM business_unit
    DELETE FROM us_cities
    DELETE FROM legal_entity`;

    const createTable = await sql_request.query(flushing_query);

    // console.log(fomatted_table_name + " created");
    console.log("HVAC Tables deleted");
  } catch (error) {
    console.log("HVAC Tables deletion failed. Trying Again!", error);
    // create_flat_tables(sql_request, flattenedObj, table_name);
  }
}

module.exports = flush_hvac_data;
