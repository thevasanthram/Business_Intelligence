const sql = require("mssql");
const ExcelJS = require("exceljs");
const fs = require("fs");

// Database configuration
const config = {
  user: "pinnacleadmin",
  password: "PiTestBi01",
  server: "pinnaclemep.database.windows.net",
  database: "main_hvac_db",
};

// Function to export data from a table to Excel in batches
async function exportTableToExcel(connection, tableName, batchSize) {
  try {
    const query = `SELECT * FROM ${tableName}`;
    console.log("Executing query:", query);

    const request = connection.request();
    request.stream = true; // Enable streaming

    const workbook = new ExcelJS.Workbook();
    let worksheet = workbook.addWorksheet(tableName);

    // Define a promise to handle stream completion
    const streamPromise = new Promise((resolve, reject) => {
      let isFirstRow = true; // To add column headers only once

      request.on("row", (row) => {
        if (isFirstRow) {
          // Add column headers to the worksheet (only for the first batch)
          const columnHeaders = Object.keys(row);
          worksheet.addRow(columnHeaders);
          isFirstRow = false;
        }
        worksheet.addRow(Object.values(row));

        // If you've written batchSize rows, resolve the promise to process the next batch
        if (worksheet.rowCount >= batchSize) {
          // Save the workbook to a separate file
          const filename = `${tableName}.xlsx`;
          const writer = fs.createWriteStream(filename);
          workbook.xlsx.write(writer).then(() => {
            writer.end();
            console.log(`Data from table ${tableName} exported to ${filename}`);
          });

          // Reset the worksheet for the next batch
          workbook.removeWorksheet(tableName);
          const newWorksheet = workbook.addWorksheet(tableName);
          worksheet = newWorksheet;
          resolve();
        }
      });

      request.on("error", (error) => {
        reject(error);
      });

      request.on("done", () => {
        // Resolve the promise when the stream is done
        resolve();
      });
    });

    // Execute the query with streaming
    await request.query(query);

    // Wait for the promise to resolve to ensure the batch is complete
    await streamPromise;
  } catch (error) {
    console.error(`Error exporting table ${tableName}:`, error);
  }
}

// Function to fetch all table names from the database
async function getAllTableNames() {
  const connection = await sql.connect(config);
  const result = await connection.request().query(`
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
  `);

  const tableNames = result.recordset.map((row) => row.TABLE_NAME);
  await connection.close();

  return tableNames;
}

// Export data from all tables to separate Excel files
(async () => {
  const tableNames = await getAllTableNames();

  const connection = await sql.connect(config);

  // Batch size for writing rows
  const batchSize = 1000; // You can adjust this value based on performance

  for (const tableName of tableNames) {
    console.log("tableName: ", tableName);
    await exportTableToExcel(connection, tableName, batchSize);
  }

  await connection.close();

  console.log(`Data from all tables exported to separate Excel files.`);
})().catch((err) => {
  console.error("Error:", err);
});
