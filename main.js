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
    api_group: "jpm",
    api_name: "projects",
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
    api_group: "memberships",
    api_name: "membership-types",
  },
  {
    api_group: "payroll",
    api_name: "payrolls",
  },
  {
    api_group: "payroll",
    api_name: "payroll-adjustments",
  },
  {
    api_group: "payroll",
    api_name: "gross-pay-items",
  },
  {
    api_group: "payroll",
    api_name: "jobs/splits",
  },
  {
    api_group: "payroll",
    api_name: "jobs/timesheets",
  },
  {
    api_group: "payroll",
    api_name: "timesheet-codes",
  },
  {
    api_group: "pricebook",
    api_name: "categories",
  },
  {
    api_group: "pricebook",
    api_name: "equipment",
  },
  {
    api_group: "pricebook",
    api_name: "materials",
  },
  {
    api_group: "sales",
    api_name: "estimates",
  },
  {
    api_group: "sales",
    api_name: "estimates/export",
  },
  {
    api_group: "settings",
    api_name: "business-units",
  },
  {
    api_group: "settings",
    api_name: "employees",
  },
  {
    api_group: "settings",
    api_name: "technicians",
  },
];

// const instance_name = 'Expert Heating and Cooling Co LLC'
// const tenant_id = 1011756844;
// const app_key = 'ak1.ztsdww9rvuk0sjortd94dmxwx'
// const client_id = 'cid.jk53hfwwcq6a1zgtbh96byil4'
// const client_secret = "cs1.2hdc1yd19hpxzmdeg5rfuc6i3smpxy9iei0yhq1p7qp8mwyjda";

// const instance_name = "PARKER-ARNTZ PLUMBING AND HEATING, INC.";
// const tenant_id = 1475606437;
// const app_key = "ak1.w9fgjo8psqbyi84vocpvzxp8y";
// const client_id = "cid.r82bhd4u7htjv56h7sqjk0jya";
// const client_secret = "cs1.4q3yjgyhjb9yaeietpsoozzc8u2qgw80j8ze43ovz1308e7zz7";

const instance_name = "Family Heating & Cooling Co LLC";
const tenant_id = 1056112968;
const app_key = "ak1.h0wqje4yshdqvn1fso4we8cnu";
const client_id = "cid.qlr4t6egndd4mbvq3vu5tef11";
const client_secret = "cs1.v9jhueeo6kgcjx5in1r8716hpnmuh6pbxiddgsv5d3y0822jay";

const params_header = {
  createdOnOrAfter: "2023-08-01T00:00:00.00Z", // 2023-08-01T00:00:00.00Z
  includeTotal: true,
  pageSize: 2000,
};

const api_group = "settings";
const api_name = "business-units";
const inserting_batch_limit = 300;

async function flow_handler() {
  // const sql_client = await create_sql_connection();

  // signing a new access token in Service Titan's API
  const access_token = await getAccessToken(client_id, client_secret);

  // continuously fetching whole api data
  const { data_pool, flattenedSampleObj } = await getAPIData(
    access_token,
    app_key,
    tenant_id,
    api_group,
    api_name,
    params_header
  );

  // continuously fetching whole api data for to crete item
  // const { data_pool, flattenedSampleObj } = await getAPIDataItem(
  //   access_token,
  //   app_key,
  //   tenant_id,
  //   api_group,
  //   api_name,
  //   params_header
  // );

  // generating csv
  csv_generator(data_pool, flattenedSampleObj, api_group + "_" + api_name);

  // json_to_text_convertor
  json_to_text_convertor(data_pool, api_group, api_name);

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
