const fs = require("fs");
const path = require("path");
const flattenObject = require("./new_flatten_object");
const extractMatchingValues = require("./extract_matching_values");
const mssql = require("mssql");
const { json } = require("express");

async function prepare_table_rows(data_pool, header_data, table) {
  // Loop through the data and add rows to the table
  await Promise.all(
    data_pool.map(async (currentObj, index) => {
      const flattenedObj = flattenObject(currentObj);
      const filteredObj = extractMatchingValues(header_data, flattenedObj);

      table.rows.add(...Object.values(filteredObj)); // Spread the elements of the row array as arguments
    })
  );
}

async function flat_data_bulk_insertion(
  sql_request,
  data_pool,
  header_data,
  table_name
) {
  const formatted_table_name =
    table_name.replace(/-/g, "_").replace(/\//g, "_") + "_table";

  try {
    // Create a table object with create option set to false
    const table = new mssql.Table(formatted_table_name);
    table.create = false;

    // Define the columns based on the header_data
    Object.keys(header_data).map((column) => {
      table.columns.add(column, mssql.NVarChar(mssql.MAX)); // Adjust the data type as needed
    });

    await prepare_table_rows(data_pool, header_data, table);

    // Bulk insert
    const bulkResult = await sql_request.bulk(table);

    // Check if the bulk insert was successful
    if (bulkResult.rowsAffected && bulkResult.rowsAffected[0] > 0) {
      console.log(`${formatted_table_name} - bulk insertion successful`);
    } else {
      console.error(
        `Bulk insert did not affect any rows in ${formatted_table_name}`
      );
    }
  } catch (err) {
    console.error("Bulk insert error:", err);

    // Log the data that caused the error
    // console.error("Data that caused the error:", data_pool[index]);

    // Optionally, log the specific SQL query executed by the bulk insert
    console.error("SQL Query:", sql_request.commandText);
  }

  console.log(`${formatted_table_name} data insertion completed`);
}

module.exports = flat_data_bulk_insertion;
