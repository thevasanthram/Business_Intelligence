const mssql = require("mssql");

async function create_sql_connection_pool() {
  // Create a connection pool
  // const pool = await new mssql.ConnectionPool({
  //   user: "deevia",
  //   password: "kiran@123",
  //   server: "deevia-trial.database.windows.net",
  //   database: "hvac_db",
  //   requestTimeout: 60000 * 60 * 24, // Set the request timeout to 60 seconds (adjust as needed)
  // }).connect();

  let request;

  try {
    const pool = await new mssql.ConnectionPool({
      user: "pinnacleadmin",
      password: "PiTestBi01",
      server: "pinnaclemep.database.windows.net",
      database: "bi_play_ground",
      requestTimeout: 60000 * 60 * 24, // Set the request timeout to 60 seconds (adjust as needed)
    }).connect();

    request = new mssql.Request(pool);
  } catch (err) {
    console.log("Error while creating sql pool connection. Trying Again!", err);
    request = await create_sql_connection_pool();
  }

  return request; // Return both the pool and request objects
}

module.exports = create_sql_connection_pool;
