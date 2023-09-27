const sql = require("mssql");

// Connection configuration
const config = {
  user: "pinnacleadmin",
  password: "PiTestBi01",
  server: "pinnaclemep.database.windows.net",
  database: "hvac_db",
  options: {
    encrypt: true, // Use encryption
  },
};

// Sample data (replace with your data)
const dataToInsert = [
  { id: 1, name: "Item 1" },
  { id: 2, name: "Item 2" },
  // Add more rows as needed
];

async function bulkInsert() {
  try {
    // Connect to the database
    await sql.connect(config);

    // Create a table object for the target table
    const table = new sql.Table("sample_table");

    // Define the schema of the table (columns)
    table.create = true; // Create the table if it doesn't exist
    table.columns.add("id", sql.Int, { primary: true }); // Add primary key column
    table.columns.add("name", sql.NVarChar(255));

    // Add data to the table
    dataToInsert.forEach((row) => {
      table.rows.add(row.id, row.name);
    });

    // Create a request to perform the bulk insert
    const request = new sql.Request();

    // Perform the bulk insert
    await request.bulk(table);

    console.log("Bulk insert completed successfully.");
  } catch (err) {
    console.error("Error:", err.message);
  } finally {
    // Close the connection
    sql.close();
  }
}

// Call the bulkInsert function to perform the bulk insert
bulkInsert();
