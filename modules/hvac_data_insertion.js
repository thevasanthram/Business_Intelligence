const fs = require("fs");
const path = require("path");
const mssql = require("mssql");

async function hvac_data_insertion(
  sql_pool,
  data_pool,
  header_data,
  table_name,
  mode
) {
  let status = "failure";
  try {
    // clearing old records if exists

    if (mode == "FLASHING") {
      const delete_table_records = `DELETE FROM ${table_name}`;
      await sql_pool.query(delete_table_records);
    }

    // Create a table object with create option set to false
    const table = new mssql.Table(table_name);
    table.create = false; // Create the table if it doesn't exist

    // Define the columns based on the header_data
    // console.log("table_name: ", table_name);
    // console.log("header_data: ", header_data);

    Object.keys(header_data).map((column) => {
      const data_type = header_data[column]["data_type"];
      const constraint = header_data[column]["constraint"];

      if (data_type === "NVARCHAR") {
        table.columns.add(
          column,
          mssql.NVarChar(mssql.MAX),
          constraint ? constraint : {}
        );
      } else if (data_type === "NVARCHAR20") {
        table.columns.add(
          column,
          mssql.NVarChar(20),
          constraint ? constraint : {}
        );
      } else if (data_type === "INT") {
        table.columns.add(column, mssql.Int, constraint ? constraint : {});
      } else if (data_type === "TINYINT") {
        table.columns.add(column, mssql.TinyInt, constraint ? constraint : {});
      } else if (data_type === "DATETIME2") {
        table.columns.add(
          column,
          mssql.DateTime2,
          constraint ? constraint : {}
        );
      } else if (data_type === "TIME") {
        table.columns.add(column, mssql.Time, constraint ? constraint : {});
      } else if (data_type === "DECIMAL") {
        table.columns.add(
          column,
          mssql.Decimal(18, 8),
          constraint ? constraint : {}
        );
      } else if (data_type === "DECIMAL96") {
        table.columns.add(
          column,
          mssql.Decimal(9, 6),
          constraint ? constraint : {}
        );
      } else if (data_type === "BIGINT") {
        table.columns.add(column, mssql.BigInt, constraint ? constraint : {});
      }
    });

    await Promise.all(
      data_pool.map(async (currentObj, index) => {
        table.rows.add(
          ...Object.values(currentObj).map((value) => {
            if (typeof value == "string") {
              return value.includes(`'`)
                ? `${value.replace(/'/g, `''`)}`
                : `${value}`;
            } else {
              return value;
            }
          })
        ); // Spread the elements of the row array as arguments
      })
    );

    // console.log("schema: ", table.schema);
    // console.log("columns: ", table.columns);
    // console.log("rows: ", table.rows[0]);
    // console.log("rows: ", table.rows[1]);
    // console.log("rows: ", table.rows[2]);
    // console.log("rows: ", table.rows[3]);

    // Bulk insert
    const bulkResult = await sql_pool.bulk(table);

    // Log the queries

    console.log(
      `${table_name} data insertion completed. Affected no of rows: `,
      bulkResult.rowsAffected
    );

    status = "success";
  } catch (err) {
    console.error(table_name, "Bulk insert error: trying again..", err);

    status = "failure";
  }

  return status;
}

module.exports = hvac_data_insertion;
