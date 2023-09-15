const sql = require("mssql");

async function create_sql_connection() {
  // database configuration
  const config = {
    user: "deevia",
    password: "kiran@123",
    server: "deevia-trial.database.windows.net",
    database: "hvac_db",
    options: {
      encrypt: true, // Use this option for SSL encryption
      requestTimeout: 48 * 60 * 60 * 1000, // 60 seconds (adjust as needed)
    },
  };

  await sql.connect(config);

  // Create a request object
  const request = new sql.Request();

  return request; // Return both the pool and request objects
}

module.exports = create_sql_connection;
