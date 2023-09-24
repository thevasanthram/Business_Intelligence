const mssql = require("mssql");

async function insertData() {
  try {
    const pool = await new mssql.ConnectionPool({
      user: "pinnacleadmin",
      password: "PiTestBi01",
      server: "pinnaclemep.database.windows.net",
      database: "hvac_db",
      requestTimeout: 60000 * 60 * 24, // Set the request timeout to 60 seconds (adjust as needed)
    }).connect();

    const table = new mssql.Table("legal_entity");
    table.create = true; // Do not create the table if it doesn't exist

    // Define the columns
    table.columns.add("id", mssql.Int ,{}); // Specify this column as the primary key
    table.columns.add("legal_name", mssql.NVarChar(mssql.MAX));

    // Add data rows
    table.rows.add(1, "Expert Heating and Cooling");
    table.rows.add(2, "Parket-Arntz Plumbing and Heating");
    table.rows.add(3, "Family Heating and Cooling");

    console.log("table: ", table.columns);

    const request = new mssql.Request(pool);

    const result = await request.bulk(table);
    console.log(`Bulk insert completed. Rows affected: ${result.rowsAffected}`);
  } catch (err) {
    console.error("Error:", err);
  }
}

insertData();
