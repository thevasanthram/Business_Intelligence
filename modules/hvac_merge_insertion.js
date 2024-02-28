const fs = require("fs");
const path = require("path");
const mssql = require("mssql");

async function hvac_merge_insertion(
  sql_pool,
  data_pool,
  header_data,
  table_name
) {
  let status = "failure";
  // Generate a temporary table name
  const tempTableName = `Temp_${table_name}_${Date.now()}`;
  try {
    // Create a table object with create option set to false
    const table = new mssql.Table(tempTableName);
    table.create = true; // Create the table if it doesn't exist

    // // update query
    // const updation_query = Object.keys(header_data)
    //   .map((column) => `Target.${column} = Source.${column}`)
    //   .join(",\n");

    // Update query
    const updation_query = Object.keys(header_data)
      .map((column) => `Target.[${column}] = Source.[${column}]`)
      .join(",\n");

    // Define the columns based on the header_data
    Object.keys(header_data).map((column) => {
      const data_type = header_data[column]["data_type"];
      const constraint = header_data[column]["constraint"];

      if (data_type === "NVARCHAR") {
        table.columns.add(
          column,
          mssql.NVarChar(mssql.MAX),
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

    // Add the data to the table
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
        );
      })
    );

    // Bulk insert into the temporary table
    await sql_pool.bulk(table);

    // Use MERGE to update or insert into the target table
    // Example of how to use it in the MERGE statement
    const mergeQuery = `
        MERGE INTO ${table_name} AS Target
        USING ${tempTableName} AS Source
        ON Target.id = Source.id
        WHEN MATCHED THEN
        UPDATE SET
            ${updation_query}
        WHEN NOT MATCHED THEN
        INSERT (${Object.keys(header_data)
          .map((column) => `[${column}]`)
          .join(", ")}
        )
        VALUES (${Object.keys(header_data)
          .map((column) => `[Source].[${column}]`)
          .join(", ")}
        );
    `;

    // console.log("mergeQuery: ", mergeQuery);

    // Execute the MERGE query
    const mergeResult = await sql_pool.query(mergeQuery);

    // Drop the temporary table
    await sql_pool.query(`DROP TABLE ${tempTableName}`);

    console.log(
      `${table_name} data insertion completed. Affected no of rows: `,
      mergeResult.rowsAffected
    );

    status = "success";
  } catch (err) {
    console.error(table_name, "Bulk insert error: trying again..", err);
    status = "failure";

    await sql_pool.query(`DROP TABLE ${tempTableName}`);

    await swl;
  }

  return status;
}

module.exports = hvac_merge_insertion;
