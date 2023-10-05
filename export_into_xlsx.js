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

// Create an Excel workbook
const workbook = new ExcelJS.Workbook();
const filename = "all_tables.xlsx";

// Function to export data from a table to Excel
async function exportTableToExcel(connection, tableName, worksheet) {
  try {
    const query = `SELECT * FROM ${tableName}`;
    console.log("Executing query:", query);

    const result = await connection.request().query(query);
    console.log(`Query returned ${result.recordset.length} rows`);

    if (result.recordset.length > 0) {
      // Add column headers to the worksheet (only for the first batch)
      if (!worksheet.getRow(1).values.length) {
        const columnHeaders = Object.keys(result.recordset[0]);
        worksheet.addRow(columnHeaders);
      }

      // Add data rows to the worksheet
      for (const row of result.recordset) {
        worksheet.addRow(Object.values(row));
      }
    } else {
      console.log(`Table ${tableName} is empty.`);
    }
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

// Export data from all tables to Excel
(async () => {
  const tableNames = await getAllTableNames();

  const connection = await sql.connect(config);

  for (const tableName of tableNames) {
    console.log("tableName: ", tableName);
    const worksheet = workbook.addWorksheet(tableName);
    await exportTableToExcel(connection, tableName, worksheet);
  }

  await connection.close();

  // Save the Excel file in chunks to avoid memory issues
  const writer = fs.createWriteStream(filename);
  await workbook.xlsx.write(writer);
  writer.end();

  console.log(`Data from all tables exported to ${filename}`);
})().catch((err) => {
  console.error("Error:", err);
});
