const fs = require("fs");
const path = require("path");
const flattenObject = require("./new_flatten_object");
const extractMatchingValues = require("./extract_matching_values");
const mssql = require("mssql");

async function flat_data_bulk_insertion(
  sql_pool,
  data_pool,
  header_data,
  table_name
) {
  const formatted_table_name =
    table_name.replace(/-/g, "_").replace(/\//g, "_") + "_table";

  try {
    // Create a table object with create option set to false
    const table = new mssql.Table(formatted_table_name);
    table.create = true; // Create the table if it doesn't exist

    // Define the columns based on the header_data
    Object.keys(header_data).map((column) => {
      const columnName = `[${column}]`; // Enclose the column name in square brackets
      table.columns.add(column, mssql.NVarChar(mssql.MAX)); // Adjust the data type as needed
    });

    const batch = 1000;

    console.log("Total inserting records: ", data_pool.length);
    for (let i = 0; i < data_pool.length; i = i + batch) {
      data_pool.slice(i, i + batch).map(async (currentObj, index) => {
        const flattenedObj = flattenObject(currentObj);
        const filteredObj = extractMatchingValues(header_data, flattenedObj);

        table.rows.add(
          ...Object.values(filteredObj).map((value) => {
            return String(value).includes(`'`)
              ? `'${value.replace(/'/g, `''`)}'`
              : `'${value}'`;
          })
        ); // Spread the elements of the row array as arguments
      });
    }

    // console.log("schema: ", table.schema);
    // console.log("columns: ", table.columns);
    // console.log("rows: ", table.rows);

    // Bulk insert
    const bulkResult = await sql_pool.bulk(table);

    // Log the queries

    console.log(
      `${formatted_table_name} data insertion completed. Affected no of rows: `,
      bulkResult.rowsAffected
    );
  } catch (err) {
    console.error(
      formatted_table_name,
      "Bulk insert error: trying again..",
      err
    );
    flat_data_bulk_insertion(sql_pool, data_pool, header_data, table_name);
  }
}

module.exports = flat_data_bulk_insertion;
