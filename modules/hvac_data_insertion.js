const fs = require("fs");
const path = require("path");
const flattenObject = require("./new_flatten_object");
const extractMatchingValues = require("./extract_matching_values");
const mssql = require("mssql");
const { type } = require("os");

async function hvac_data_insertion(
  sql_pool,
  data_pool,
  header_data,
  table_name
) {
  try {
    // Create a table object with create option set to false
    console.log("table_name: ", table_name);
    const table = new mssql.Table(table_name);
    table.create = false; // Create the table if it doesn't exist

    // Define the columns based on the header_data
    console.log("header_data: ", header_data);
    Object.keys(header_data).map((column) => {
      const data_type = header_data[column];
      console.log("data_type: ", data_type);
      if (data_type === "NVARCHAR") {
        table.columns.add(column, mssql.NVarChar(mssql.MAX));
      } else if (data_type === "INT") {
        table.columns.add(column, mssql.Int);
      }
    });

    // table.rows.add(1, "Expert Heating and Cooling");

    await Promise.all(
      data_pool.map(async (currentObj, index) => {
        table.rows.add(
          ...Object.values(currentObj).map((value) => {
            if (typeof value == "string") {
              console.log("value: ", value);
              if (value.includes(`'`)) {
                console.log("============");
              }
              return value.includes(`'`)
                ? `"${value.replace(/'/g, `''`)}"`
                : `'${value}'`;
            } else {
              return value;
            }
          })
        ); // Spread the elements of the row array as arguments
      })
    );

    // console.log("schema: ", table.schema);
    console.log("columns: ", table.columns);
    console.log("rows: ", table.rows);

    // Bulk insert
    const bulkResult = await sql_pool.bulk(table);

    // Log the queries

    console.log(
      `${table_name} data insertion completed. Affected no of rows: `,
      bulkResult.rowsAffected
    );
  } catch (err) {
    console.error(table_name, "Bulk insert error: trying again..", err);
    hvac_data_insertion(sql_pool, data_pool, header_data, table_name);
  }
}

module.exports = hvac_data_insertion;