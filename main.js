const fs = require("fs");
const path = require("path");

// modules
const create_sql_connection = require("./modules/create_sql_connection");
const getAccessToken = require("./modules/get_access_token");
const getAPIData = require("./modules/get_api_data");
const getAPIDataItem = require("./modules/get_api_data_item");
const create_flat_tables = require("./modules/create_flat_tables");
const flat_data_insertion = require("./modules/flat_data_insertion");
const create_star_tables = require("./modules/create_star_tables");
const star_schema_data_insertion = require("./modules/star_schema_data_insertion");
const json_to_text_convertor = require("./modules/json_to_text_convertor");
const response_format_csv_generator = require("./modules/response_format_csv_generator");
const csv_generator = require("./modules/csv_generator");

// Service Titan's API parameters
const api_collection = [
  {
    api_group: "accounting",
    api_name: "invoices",
  },
  {
    api_group: "accounting",
    api_name: "inventory-bills",
  },
  {
    api_group: "accounting",
    api_name: "payments",
  },
  {
    api_group: "crm",
    api_name: "customers",
  },
  {
    api_group: "crm",
    api_name: "bookings",
  },
  {
    api_group: "crm",
    api_name: "locations",
  },
  {
    api_group: "dispatch",
    api_name: "appointment-assignments",
  },
  {
    api_group: "dispatch",
    api_name: "zones",
  },
  {
    api_group: "equipmentsystems",
    api_name: "installed-equipment",
  },
  {
    api_group: "inventory",
    api_name: "adjustments",
  },
  {
    api_group: "inventory",
    api_name: "purchase-orders",
  },
  {
    api_group: "inventory",
    api_name: "receipts",
  },
  {
    api_group: "inventory",
    api_name: "returns",
  },
  {
    api_group: "inventory",
    api_name: "trucks",
  },
  {
    api_group: "inventory",
    api_name: "vendors",
  },
  {
    api_group: "inventory",
    api_name: "warehouses",
  },
  {
    api_group: "jpm",
    api_name: "appointments", // end keyword is used in this api response
  },
  {
    api_group: "jpm",
    api_name: "job-types",
  },
  {
    api_group: "jpm",
    api_name: "jobs",
  },
  {
    api_group: "marketing",
    api_name: "campaigns",
  },
  {
    api_group: "memberships",
    api_name: "memberships",
  },
  {
    api_group: "memberships",
    api_name: "recurring-services",
  },
  {
    api_group: "memberships",
    api_name: "recurring-service-events",
  },
  {
    api_group: "memberships",
    api_name: "recurring-service-types",
  },
  {
    api_group: "payroll",
    api_name: "gross-pay-items",
  },
  {
    api_group: "payroll",
    api_name: "jobs/timesheets",
  },
  {
    api_group: "payroll",
    api_name: "timesheet-codes",
  },
];

const params_header = {
  createdOnOrAfter: "2023-08-01T00:00:00.00Z", // 2022-09-01T00:00:00.00Z
  includeTotal: true,
  pageSize: 2000,
};

const api_group = "inventory";
const api_name = "transfers";
const inserting_batch_limit = 300;

async function flow_handler() {
  // const sql_client = await create_sql_connection();

  // signing a new access token in Service Titan's API
  const access_token = await getAccessToken();

  // continuously fetching whole api data
  const { data_pool, flattenedSampleObj } = await getAPIData(
    access_token,
    api_group,
    api_name,
    params_header
  );

  // continuously fetching whole api data for to crete item
  // const { data_pool, flattenedSampleObj } = await getAPIDataItem(
  //   access_token,
  //   api_group,
  //   api_name,
  //   params_header
  // );

  // generating csv
  csv_generator(data_pool, flattenedSampleObj, api_group + "_" + api_name);

  // json_to_text_convertor
  // json_to_text_convertor(data_pool, api_group, api_name);

  // api response sample csv generator
  // response_format_csv_generator(flattenedSampleObj, api_group + "_" + api_name);

  // create flat tables
  // await create_flat_tables(sql_client, flattenedSampleObj, api_group, api_name);

  // create star schema tables
  // await create_star_tables(sql_client, flattenedSampleObj, api_name);

  // await flat_data_insertion(
  //   sql_client,
  //   data_pool,
  //   flattenedSampleObj,
  //   api_group,
  //   api_name,
  //   inserting_batch_limit
  // );
}

flow_handler();

// for automatic ETL
// for (let i = 0; i < api_collection.length; i++) {
//   const api_group_temp = api_collection[i]["api_group"];
//   const api_name_temp = api_collection[i]["api_name"];

//   console.log(api_group_temp, api_name_temp);
//   flow_handler(api_group_temp, api_name_temp);
// }

// console.log("api_collection: ", api_collection.length);
