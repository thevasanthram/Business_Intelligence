const sql = require("mssql");

// modules
const create_sql_connection = require("./modules/create_sql_connection");
const create_sql_pool = require("./modules/create_sql_pool");
const getAccessToken = require("./modules/get_access_token");
const getAPIData = require("./modules/get_api_data");
const getAPIDataItem = require("./modules/get_api_data_item");
const create_flat_tables = require("./modules/create_flat_tables");
const flat_data_insertion = require("./modules/flat_data_insertion");
const flat_data_bulk_insertion = require("./modules/flat_data_bulk_insertion");
const create_star_tables = require("./modules/create_star_tables");
const star_schema_data_insertion = require("./modules/star_schema_data_insertion");
const json_to_text_convertor = require("./modules/json_to_text_convertor");
const csv_generator = require("./modules/csv_generator");
const find_lenghthiest_header = require("./modules/find_lengthiest_header");
const create_hvac_schema = require("./modules/create_hvac_schema");

// Service Titan's API parameters

const api_collection = [
  {
    api_group: "accounting",
    api_name: "invoices",
    mode: "items",
  },
  {
    api_group: "inventory",
    api_name: "adjustments",
    mode: "items",
  },
  {
    api_group: "inventory",
    api_name: "transfers",
    mode: "items",
  },
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
    api_group: "accounting",
    api_name: "ap-payments",
  },
  {
    api_group: "accounting",
    api_name: "journal-entries",
  },
  {
    api_group: "accounting",
    api_name: "payment-terms",
  },
  {
    api_group: "accounting",
    api_name: "payment-types",
  },
  {
    api_group: "accounting",
    api_name: "tax-zones",
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
    api_group: "crm",
    api_name: "booking-provider-tags",
  },
  {
    api_group: "crm",
    api_name: "leads",
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
    api_group: "dispatch",
    api_name: "non-job-appointments",
  },
  {
    api_group: "dispatch",
    api_name: "teams",
  },
  {
    api_group: "dispatch",
    api_name: "technician-shifts",
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
    api_group: "inventory",
    api_name: "purchase-order-markups",
  },
  {
    api_group: "inventory",
    api_name: "purchase-order-types",
  },
  {
    api_group: "jbce",
    api_name: "call-reasons",
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
    api_group: "jpm",
    api_name: "job-cancel-reasons",
  },
  {
    api_group: "jpm",
    api_name: "job-hold-reasons",
  },
  {
    api_group: "jpm",
    api_name: "project-statuses",
  },
  {
    api_group: "jpm",
    api_name: "project-substatuses",
  },
  {
    api_group: "marketing",
    api_name: "campaigns",
  },
  {
    api_group: "marketing",
    api_name: "categories",
  },
  {
    api_group: "marketing",
    api_name: "suppressions",
  },
  {
    api_group: "marketing",
    api_name: "costs",
  },
  {
    api_group: "marketingreputation",
    api_name: "reviews",
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
    api_group: "payroll",
    api_name: "activity-codes",
  },
  {
    api_group: "payroll",
    api_name: "locations/rates",
  },
  {
    api_group: "payroll",
    api_name: "non-job-timesheets",
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
    api_group: "pricebook",
    api_name: "discounts-and-fees",
  },
  {
    api_group: "pricebook",
    api_name: "images",
  },
  {
    api_group: "pricebook",
    api_name: "services",
  },
  {
    api_group: "pricebook",
    api_name: "materialsmarkup",
  },
  {
    api_group: "reporting",
    api_name: "report-categories",
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
    api_group: "sales",
    api_name: "estimates/items",
  },
  {
    api_group: "service-agreements",
    api_name: "service-agreements",
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
  {
    api_group: "settings",
    api_name: "tag-types",
  },
  {
    api_group: "settings",
    api_name: "user-roles",
  },
  {
    api_group: "taskmanagement",
    api_name: "data",
  },
  {
    api_group: "telecom",
    api_name: "export/calls",
  },
  {
    api_group: "telecom",
    api_name: "calls",
  },
  {
    api_group: "forms",
    api_name: "forms",
  },
  {
    api_group: "forms",
    api_name: "submissions",
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

const hvac_tables = [
  "legal_entity",
  "business_unit",
  "customer_details",
  "location",
  "job_details",
  "vendor_details",
  "sku_details",
  "cogs_equipment",
  "technician",
  "cogs_labor",
  "cogs_material",
  "invoice",
  "gross_profit",
];

const params_header = {
  createdOnOrAfter: "",
  createdBefore: "", // 2023-10-04T00:00:00.00Z
  includeTotal: true,
  pageSize: 2000,
};

const inserting_batch_limit = 300;

function startStopwatch(task_name) {
  let startTime = Date.now();
  let running = true;

  let elapsed_time_cache = "";

  const updateStopwatch = () => {
    if (!running) return;

    const elapsedTime = Date.now() - startTime;
    const seconds = Math.floor(elapsedTime / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);

    let formattedTime =
      String(hours).padStart(2, "0") +
      ":" +
      String(minutes % 60).padStart(2, "0") +
      ":" +
      String(seconds % 60).padStart(2, "0") +
      "." +
      String(elapsedTime % 1000).padStart(3, "0");

    process.stdout.write(`Time elapsed for ${task_name}: ${formattedTime}\r`);

    elapsed_time_cache = formattedTime;

    setTimeout(updateStopwatch, 10); // Update every 10 milliseconds
  };

  updateStopwatch();

  // Function to stop the stopwatch
  function stop() {
    running = false;
    return elapsed_time_cache;
  }

  return stop;
}

async function fetch_all_data(data_lake, instance_details, api_collection) {
  // collect all data from all the instance

  await Promise.all(
    instance_details.map(async (instance_data) => {
      const instance_name = instance_data["instance_name"];
      const tenant_id = instance_data["tenant_id"];
      const app_key = instance_data["app_key"];
      const client_id = instance_data["client_id"];
      const client_secret = instance_data["client_secret"];

      // signing a new access token in Service Titan's API
      const access_token = await getAccessToken(client_id, client_secret);

      await Promise.all(
        api_collection.map(async (api_data) => {
          const api_group_temp = api_data["api_group"];
          const api_name_temp = api_data["api_name"];

          if (!api_data["mode"]) {
            // for normal
            if (
              !data_lake[
                api_group_temp + "__" + api_name_temp + "&&" + "normal"
              ]
            ) {
              data_lake[
                api_group_temp + "__" + api_name_temp + "&&" + "normal"
              ] = {
                data_pool: [],
                header_data: [],
              };
            }

            // continuously fetching whole api data
            const data_pool = await getAPIData(
              access_token,
              app_key,
              instance_name,
              tenant_id,
              api_group_temp,
              api_name_temp,
              params_header
            );

            data_lake[api_group_temp + "__" + api_name_temp + "&&" + "normal"][
              "data_pool"
            ].push(...data_pool);
          } else {
            // for items
            if (
              !data_lake[api_group_temp + "__" + api_name_temp + "&&" + "items"]
            ) {
              data_lake[
                api_group_temp + "__" + api_name_temp + "&&" + "items"
              ] = {
                data_pool: [],
                header_data: [],
              };
            }

            // continuously fetching whole api data for to crete item
            const data_pool = await getAPIDataItem(
              access_token,
              app_key,
              instance_name,
              tenant_id,
              api_group_temp,
              api_name_temp,
              params_header
            );

            data_lake[api_group_temp + "__" + api_name_temp + "&&" + "items"][
              "data_pool"
            ].push(...data_pool);
          }
        })
      );
    })
  );
}

async function find_max_and_write_csv(data_lake) {
  // find max and write into csv
  const api_batch_limit = 1;
  console.log("find_max_and_write_csv");
  for (
    let api_count = 0;
    api_count < Object.keys(data_lake).length;
    api_count = api_count + api_batch_limit
  ) {
    console.log("inside for loop");

    const key = Object.keys(data_lake)[api_count];
    const current_data_pool = data_lake[key]["data_pool"];
    const current_instance_name = data_lake[key]["instance_name"];

    const [api_group, api_name_and_mode] = key.split("__");

    const [api_name, api_mode] = api_name_and_mode.split("&&");

    // find lengthiest data
    data_lake[key]["header_data"] = await find_lenghthiest_header(
      current_data_pool
    );

    if (api_mode == "normal") {
      console.log("csv_generator");
      await csv_generator(
        current_data_pool,
        data_lake[key]["header_data"],
        api_group + "_" + api_name
      );
      // json_to_text_convertor
      // json_to_text_convertor(current_data_pool, api_group, api_name);
    } else {
      console.log("csv_generator");
      await csv_generator(
        current_data_pool,
        data_lake[key]["header_data"],
        api_group + "_" + api_name + "_" + api_mode
      );
      // json_to_text_convertor
      // json_to_text_convertor(current_data_pool, api_group, api_name);
    }
  }
}

async function find_max_and_populate_db(data_lake) {
  // creating a client for azure sql database operations
  const sql_request = await create_sql_connection();

  const sql_pool = await create_sql_pool();

  // find max and populate the db
  await Promise.all(
    Object.keys(data_lake).map(async (key) => {
      const current_data_pool = data_lake[key]["data_pool"];

      const [api_group, api_name_and_mode] = key.split("__");

      const [api_name, api_mode] = api_name_and_mode.split("&&");

      // find lengthiest data
      data_lake[key]["header_data"] = await find_lenghthiest_header(
        current_data_pool
      );

      if (api_mode == "normal") {
        await azure_db_operations(
          sql_request,
          sql_pool,
          current_data_pool,
          data_lake[key]["header_data"],
          api_group + "_" + api_name
        );
      } else {
        await azure_db_operations(
          sql_request,
          sql_pool,
          current_data_pool,
          data_lake[key]["header_data"],
          api_group + "_" + api_name + "_" + api_mode
        );
      }
    })
  );
}

async function find_total_records(data_lake) {
  let total_records = 0;

  // console.log("data_lake: ", data_lake);

  Object.keys(data_lake).map((api_name) => {
    total_records = total_records + data_lake[api_name]["data_pool"].length;
  });

  console.log("total_records: ", total_records);
}

async function find_max_and_bulk_insert(data_lake) {
  // creating a client for azure sql database operations
  const sql_request = await create_sql_connection();

  const sql_pool = await create_sql_pool();

  // console.log("connection & sql pool", sql_request, sql_pool);

  // find max and populate the db

  for (
    let api_count = 0;
    api_count < Object.keys(data_lake).length;
    api_count++
  ) {
    const key = Object.keys(data_lake)[api_count];

    const current_data_pool = data_lake[key]["data_pool"];

    console.log("current_data_pool: ", current_data_pool);

    const [api_group, api_name_and_mode] = key.split("__");

    const [api_name, api_mode] = api_name_and_mode.split("&&");

    // find lengthiest data
    data_lake[key]["header_data"] = await find_lenghthiest_header(
      current_data_pool
    );

    if (api_mode == "normal") {
      await azure_db_operations(
        sql_request,
        sql_pool,
        current_data_pool,
        data_lake[key]["header_data"],
        api_group + "_" + api_name
      );
    } else {
      await azure_db_operations(
        sql_request,
        sql_pool,
        current_data_pool,
        data_lake[key]["header_data"],
        api_group + "_" + api_name + "_" + api_mode
      );
    }
  }

  // await Promise.all(Object.keys(data_lake).map(async (key) => {}));
  // Close the connection pool
  await sql.close();
}

async function azure_db_operations(
  sql_request,
  sql_pool,
  data_pool,
  header_data,
  table_name
) {
  // create flat tables in azure sql database
  if (Object.keys(header_data).length > 0) {
    await create_flat_tables(sql_request, header_data, table_name);
  }

  if (data_pool.length > 0) {
    await flat_data_bulk_insertion(
      sql_pool,
      data_pool,
      header_data,
      table_name
    );
  }

  // create star schema tables
  // await create_star_tables(sql_client, flattenedSampleObj, api_name);

  // insert data into flat_tables

  // await flat_data_insertion(
  //   sql_request,
  //   data_pool,
  //   header_data,
  //   table_name,
  //   inserting_batch_limit
  // );
}

// for automatic mass ETL
async function start_pipeline() {
  const data_lake = {};

  console.log("api_collection: ", api_collection.length);

  // fetching all data from Service Titan's API
  // const stop1 = startStopwatch("data fetching");
  // await fetch_all_data(data_lake, instance_details, api_collection); // taking 3 mins to fetch all data
  // console.log("Time taken for fetching data: ", stop1());

  // Storing Data into Azure SQL Database using bulk insert
  // const stop2 = startStopwatch("inserting data");
  // await find_max_and_bulk_insert(data_lake);
  // console.log("Time taken for inserting data: ", stop2());

  // await find_total_length(data_lake);

  // {
  // Creating CSVs
  // await find_max_and_write_csv(data_lake);
  // }

  // {
  //   // Storing Data into Azure SQL Database
  //   const stop = startStopwatch("inserting data");
  //   await find_max_and_populate_db(data_lake);
  //   console.log("Time taken for inserting data: ", stop());
  // }
}

start_pipeline();
