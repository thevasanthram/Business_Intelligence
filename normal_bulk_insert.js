const sql = require('mssql');

// Configure the database connection
const config = {
  user: 'pinnacleadmin',
  password: 'PiTestBi01',
  server: 'pinnaclemep.database.windows.net',
  database: 'hvac_db',
  options: {
    encrypt: true, // Use this option if your Azure SQL Database requires encryption
  },
};

// Sample data to be inserted
const dataToInsert = [
  { id: 3, name: 'John' , foreign_id: 3},
  { id: 4, name: 'Jane', foreign_id: 2 },
  // Add more records here
];

// Create a connection pool
const pool = new sql.ConnectionPool(config);

// Function to bulk insert data
async function bulkInsert() {
  try {
    await pool.connect();

    // Create a table object representing your database table
    const table = new sql.Table('foreign_table'); // Replace 'YourTableName' with your actual table name
    table.create = true; // Create the table if it doesn't exist

    // Define the schema of your table (make sure it matches your database schema)
    table.columns.add('id', sql.Int, { primary: true, nullable: false });
    table.columns.add('name', sql.NVarChar(sql.MAX));
    table.columns.add('foreign_id', sql.Int, {nullable: false});

    // Add data rows to the table
    dataToInsert.forEach((row) => {
      table.rows.add(row.id, row.name, row.foreign_id);
    });

    // Perform the bulk insert
    const request = new sql.Request(pool);
    await request.bulk(table);

    console.log('Bulk insert completed successfully.');
  } catch (error) {
    console.error('Error:', error);
  } finally {
    // Close the connection pool when done
    pool.close();
  }
}

// Call the bulkInsert function
bulkInsert();
