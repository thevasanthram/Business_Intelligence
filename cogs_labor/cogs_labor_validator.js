const sql = require("mssql");
const fs = require("fs");

async function create_sql_connection(db_name) {
  const config = {
    user: "pinnacleadmin",
    password: "PiTestBi01",
    server: "pinnaclemep.database.windows.net",
    database: db_name,
    options: {
      encrypt: true, // Use this option for SSL encryption
      requestTimeout: 48 * 60 * 60 * 1000, // 48 hours
    },
  };

  try {
    const pool = new sql.ConnectionPool(config);
    await pool.connect();
    return pool.request();
  } catch (err) {
    console.log("Error while creating request object, Trying Again!", err);
    return false;
  }
}

async function validator() {
  let main_db_client = "";
  do {
    main_db_client = await create_sql_connection("main_hvac_db");
  } while (!main_db_client);

  let hvac_db_client = "";
  do {
    hvac_db_client = await create_sql_connection("hvac_data_pool");
  } while (!hvac_db_client);

  try {
    const main_db_response = await main_db_client.query(
      "SELECT * from cogs_labor"
    );
    const main_db_result = main_db_response.recordset;

    const hvac_db_response = await hvac_db_client.query(
      "SELECT * from cogs_labor"
    );
    const hvac_db_result = hvac_db_response.recordset;

    // fs.writeFile("./main_db_result.js", JSON.stringify(main_db_result), () =>
    //   console.log("main_db_result done")
    // );

    // fs.writeFile("./hvac_db_result.js", JSON.stringify(hvac_db_result), () =>
    //   console.log("hvac_db_result done")
    // );

    console.log("main_db_result: ", main_db_result.length);
    console.log("hvac_db_result: ", hvac_db_result.length);

    let count = 0;

    // Find records in main_db_result that are not in hvac_db_result
    const unique_main_db_result = hvac_db_result.filter((main_record) => {
      count = count + 1;
      console.log("count: ", count);
      return !main_db_result.some((hvac_record) => {
        return (
          hvac_record["paid_duration"] == main_record["paid_duration"] &&
          hvac_record["labor_cost"] == main_record["labor_cost"] &&
          hvac_record["activity"] == main_record["activity"] &&
          hvac_record["paid_time_type"] == main_record["paid_time_type"] &&
          hvac_record["date"].getTime() == main_record["date"].getTime() &&
          hvac_record["startedOn"].getTime() ==
            main_record["startedOn"].getTime() &&
          hvac_record["endedOn"].getTime() ==
            main_record["endedOn"].getTime() &&
          hvac_record["isPrevailingWageJob"] ==
            main_record["isPrevailingWageJob"] &&
          hvac_record["job_details_id"] == main_record["job_details_id"] &&
          hvac_record["invoice_id"] == main_record["invoice_id"] &&
          hvac_record["project_id"] == main_record["project_id"] &&
          hvac_record["payrollId"] == main_record["payrollId"] &&
          hvac_record["technician_id"] == main_record["technician_id"]
        );
      });
    });

    console.log(
      "Unique records in main_db_result: ",
      unique_main_db_result.length
    );

    fs.writeFile(
      "./cogs_labor_result.js",
      JSON.stringify(unique_main_db_result),
      () => console.log("cogs_labor_result done")
    );
  } catch (err) {
    console.log("Error executing query: ", err);
  } finally {
    sql.close(); // Close the SQL connection pool
  }
}

validator();
