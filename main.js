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
const csv_generator = require("./modules/csv_generator");
const find_lenghthiest_header = require("./modules/find_lengthiest_header");

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
    api_name: "transfers",
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

const api_collection_for_items = [
  {
    api_group: "accounting",
    api_name: "invoices",
  },
  {
    api_group: "inventory",
    api_name: "adjustments",
  },
  {
    api_group: "inventory",
    api_name: "transfers",
  },
];

const instance_details = [
  {
    instance_name: "Expert Heating and Cooling Co LLC",
    tenant_id: 1011756844,
    app_key: "ak1.ztsdww9rvuk0sjortd94dmxwx",
    client_id: "cid.jk53hfwwcq6a1zgtbh96byil4",
    client_secret: "cs1.2hdc1yd19hpxzmdeg5rfuc6i3smpxy9iei0yhq1p7qp8mwyjda",
  },
  {
    instance_name: "PARKER-ARNTZ PLUMBING AND HEATING, INC.",
    tenant_id: 1475606437,
    app_key: "ak1.w9fgjo8psqbyi84vocpvzxp8y",
    client_id: "cid.r82bhd4u7htjv56h7sqjk0jya",
    client_secret: "cs1.4q3yjgyhjb9yaeietpsoozzc8u2qgw80j8ze43ovz1308e7zz7",
  },
  {
    instance_name: "Family Heating & Cooling Co LLC",
    tenant_id: 1056112968,
    app_key: "ak1.h0wqje4yshdqvn1fso4we8cnu",
    client_id: "cid.qlr4t6egndd4mbvq3vu5tef11",
    client_secret: "cs1.v9jhueeo6kgcjx5in1r8716hpnmuh6pbxiddgsv5d3y0822jay",
  },
];

const params_header = {
  createdOnOrAfter: "", // 2023-08-01T00:00:00.00Z
  includeTotal: true,
  pageSize: 2000,
};

const api_group = "settings";
const api_name = "business-units";
const inserting_batch_limit = 300;

async function flow_handler(
  api_group,
  api_name,
  instance_name,
  tenant_id,
  app_key,
  client_id,
  client_secret
) {
  // const sql_client = await create_sql_connection();

  // signing a new access token in Service Titan's API
  const access_token = await getAccessToken(client_id, client_secret);

  // continuously fetching whole api data
  const data_pool = await getAPIData(
    access_token,
    app_key,
    instance_name,
    tenant_id,
    api_group,
    api_name,
    params_header
  );

  // continuously fetching whole api data for to crete item
  // const data_pool = await getAPIDataItem(
  //   access_token,
  //   app_key,
  //   instance_name,
  //   tenant_id,
  //   api_group,
  //   api_name,
  //   params_header
  // );

  // generating csv
  // await csv_generator(
  //   data_pool,
  //   flattenedSampleObj,
  //   api_group + "_" + api_name,
  //   instance
  // );

  // json_to_text_convertor
  // json_to_text_convertor(data_pool, api_group, api_name);

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

  return data_pool;
}

// for automatic mass ETL
async function start_pipeline() {
  const data_lake = {};

  // collect all data from all the instance
  await Promise.all(
    instance_details.map(async (instance_data) => {
      const instance_name = instance_data["instance_name"];
      const tenant_id = instance_data["tenant_id"];
      const app_key = instance_data["app_key"];
      const client_id = instance_data["client_id"];
      const client_secret = instance_data["client_secret"];

      await Promise.all(
        api_collection.map(async (api_data) => {
          const api_group_temp = api_data["api_group"];
          const api_name_temp = api_data["api_name"];

          if (!data_lake[api_group_temp + "__" + api_name_temp]) {
            data_lake[api_group_temp + "__" + api_name_temp] = {
              instance_name: instance_name,
              data_pool: [],
              header_data: [],
            };
          }

          const data_pool = await flow_handler(
            api_group_temp,
            api_name_temp,
            instance_name,
            tenant_id,
            app_key,
            client_id,
            client_secret
          );

          data_lake[api_group_temp + "__" + api_name_temp]["data_pool"].push(
            ...data_pool
          );
        })
      );
    })
  );

  // find max and write into csv
  await Promise.all(
    Object.keys(data_lake).map(async (key) => {
      const current_data_pool = data_lake[key]["data_pool"];
      const current_instance_name = data_lake[key]["instance_name"];

      const [api_group, api_name] = key.split("__");

      // find lengthiest data
      data_lake[key]["header_data"] = await find_lenghthiest_header(
        current_data_pool
      );

      await csv_generator(
        current_data_pool,
        data_lake[key]["header_data"],
        api_group + "_" + api_name,
        current_instance_name
      );
    })
  );
}

start_pipeline();
