const fs = require("fs");
const path = require("path");

// modules
const create_sql_connection = require("./modules/create_sql_connection");
const getAccessToken = require("./modules/get_access_token");
const getAPIData = require("./modules/get_api_data");
const create_flat_tables = require("./modules/create_flat_tables");
const flat_data_insertion = require("./modules/flat_data_insertion");
const create_star_tables = require("./modules/create_star_tables");
const star_schema_data_insertion = require("./modules/star_schema_data_insertion");
const json_to_text_convertor = require("./modules/json_to_text_convertor");

// Service Titan's API parameters
const params_header = {
  createdOnOrAfter: "2023-08-01T00:00:00.00Z", // yyyy-mm-ddThh:mm:ss.mmZ
  includeTotal: true,
  pageSize: 2000,
};
const api_group = "payroll";
const api_name = "jobs/timesheets";
const inserting_batch_limit = 300;

async function flow_handler() {
  const sql_client = await create_sql_connection();

  // signing a new access token in Service Titan's API
  const access_token = await getAccessToken();

  // continuously fetching whole api data
  const { data_pool, flattenedSampleObj } = await getAPIData(
    access_token,
    api_group,
    api_name,
    params_header
  );

  // json_to_text_convertor
  // json_to_text_convertor(data_pool, api_group, api_name);

  // create flat tables
  await create_flat_tables(sql_client, flattenedSampleObj, api_group, api_name);

  // create star schema tables
  // await create_star_tables(sql_client, flattenedSampleObj, api_name);

  await flat_data_insertion(
    sql_client,
    data_pool,
    flattenedSampleObj,
    api_group,
    api_name,
    inserting_batch_limit
  );
}

flow_handler();
