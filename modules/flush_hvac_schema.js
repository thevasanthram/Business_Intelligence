async function flush_hvac_schema(sql_request, is_initial_execute) {
  try {
    const flushing_query = `
    DROP TABLE gross_profit
    DROP TABLE purchase_order
    DROP TABLE cogs_material
    DROP TABLE cogs_labor
    DROP TABLE non_job_appointments
    DROP TABLE appointment_assignments
    DROP TABLE technician
    DROP TABLE cogs_equipment
    DROP TABLE cogs_service
    DROP TABLE sku_details
    DROP TABLE vendor
    DROP TABLE invoice
    DROP TABLE appointments
    DROP TABLE sales_details
    DROP TABLE job_details
    DROP TABLE call_details
    DROP TABLE gross_pay_items
    DROP TABLE payrolls
    DROP TABLE project_managers
    DROP TABLE projects
    DROP TABLE job_types
    DROP TABLE location
    DROP TABLE customer_details
    DROP TABLE bookings
    DROP TABLE campaigns
    DROP TABLE employees
    DROP TABLE business_unit
    DROP TABLE us_cities
    DROP TABLE legal_entity`;

    const createTable = await sql_request.query(flushing_query);

    // console.log(fomatted_table_name + " created");
    console.log("HVAC Tables deleted");
  } catch (error) {
    if (!is_initial_execute) {
      console.log("HVAC Tables deletion failed. Trying Again!", error);
    }
    // create_flat_tables(sql_request, flattenedObj, table_name);
  }
}

module.exports = flush_hvac_schema;
