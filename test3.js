const sql = require("mssql");

const config = {
  user: "pinnacleadmin",
  password: "PiTestBi01",
  server: "pinnaclemep.database.windows.net",
  database: "hvac_data_pool",
};

const pool = new sql.ConnectionPool(config);
const request = new sql.Request(pool);

(async () => {
  try {
    await pool.connect();

    // Replace 'YourTableName' with your actual table name
    const tableName = "sample_table";

    const timeValues = [
      "00:00:20",
      "01:30:45",
      "03:15:00",
      // Add more time values as needed
    ];

    // Create a table to hold the time values as strings
    const table = new sql.Table(tableName);
    table.create = true; // Do not create a new table

    // Define the schema for the table
    table.columns.add("duration", sql.Time);

    // Populate the table with data
    timeValues.forEach((time) => {
      table.rows.add(time);
    });

    // Perform the bulk insert
    const bulkInsertResult = await request.bulk(table);

    console.log("Bulk insert completed successfully.");
  } catch (err) {
    console.error("Error:", err);
  } finally {
    await pool.close();
  }
})();
