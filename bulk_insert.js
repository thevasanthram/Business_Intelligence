const fs = require("fs");
const sql = require("mssql");

// database configuration
const config = {
  user: "deevia",
  password: "kiran@123",
  server: "deevia-trial.database.windows.net",
  database: "crop_db",
  options: {
    encrypt: true, // Use this option for SSL encryption
  },
};

// CSV file path
const csvFilePath = "./bulk_insert.csv";

// Define a function to perform bulk insert
async function importCSVData() {
  // Connect to the Azure SQL Database
  await sql.connect(config);

  // Define the bulk insert query
  const bulkInsertQuery = `BULK INSERT bulk_insert_check
  FROM '${csvFilePath}'  
  WITH (
    FIELDTERMINATOR = ',',     
    ROWTERMINATOR = '
  ',
    FIRSTROW = 1
  )`;

  

  console.log("bulkInsertQuery: ", bulkInsertQuery);

  // Execute the bulk insert query
  await sql.query(bulkInsertQuery);

  console.log("CSV data import completed.");
  try {
  } catch (err) {
    console.error("Error:", err.message);
  } finally {
    // Close the database connection
    await sql.close();
  }
}

// Run the importCSVData function
importCSVData();
