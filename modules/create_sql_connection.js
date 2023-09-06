const sql = require("mssql");

async function create_sql_connection() {
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

  await sql.connect(config); // Connect to the database
  const request = new sql.Request();

  return request;
}

module.exports = create_sql_connection;
