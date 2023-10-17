const sql = require("mssql");
const fs = require("fs");
const auto_flashing = require("./hvac_auto_flashing");

// modules
const create_sql_connection = require("./modules/create_sql_connection");
const create_sql_pool = require("./modules/create_sql_pool");

async function status_viewer() {
  // creating a client for azure sql database operations
  const sql_request = await create_sql_connection();
  const sql_pool = await create_sql_pool();

  try {
    const auto_update_query = `SELECT TOP 1 * FROM auto_update ORDER BY id DESC;`;

    // Execute the INSERT query and retrieve the ID
    const result = await sql_request.query(auto_update_query);

    if (result.rowsAffected > 0) {
      auto_flashing();
    } else {
      auto_flashing();
    }

    console.log("result: ", result);
  } catch (err) {
    console.log("Error while checking auto_update", err);
    auto_flashing();
  }
}

status_viewer();
