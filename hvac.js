const sql = require("mssql");

// modules
const create_sql_connection = require("./modules/create_sql_connection");
const create_sql_pool = require("./modules/create_sql_pool");
const getAccessToken = require("./modules/get_access_token");
const getAPIWholeData = require("./modules/get_api_whole_data");
const hvac_data_insertion = require("./modules/hvac_data_insertion");
const hvac_flat_data_insertion = require("./modules/hvac_flat_data_insertion");
const find_lenghthiest_header = require("./modules/find_lengthiest_header");
const create_hvac_schema = require("./modules/create_hvac_schema");
const kpi_data = require("./modules/business_units_details");

// Service Titan's API parameters
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

const hvac_tables = {
  legal_entity: {
    // manual entry
    columns: ["id", "legal_name"],
  },
  business_unit: {
    // settings business units
    columns: [
      "id",
      "business_unit_name",
      "business_unit_official_name",
      "account_type",
      "revenue_type",
      "trade_type",
      "legal_entity_id",
    ],
    local_table: {
      account_type: "",
      business_unit_name: "",
      business_unit_official_name: "",
      id: "",
      revenue_type: "",
      trade_type: "",
    },
    foreign_table: {
      legal_entity__id: "",
    },
  },
  customer_details: {
    columns: [
      "id",
      "name",
      "is_active",
      "type",
      "creation_date",
      "address_street",
      "address_unit",
      "address_city",
      "address_state",
      "address_zip",
    ],
    // crm customers
    local_table: {
      address_city: "",
      address_state: "",
      address_street: "",
      address_unit: "",
      address_zip: "",
      creation_date: "",
      id: "",
      is_active: "",
      name: "",
      type: "",
    },
    foreign_table: {},
  },
  location: {
    columns: [
      "id",
      "street",
      "unit",
      "city",
      "state",
      "zip",
      "taxzone",
      "zone_id",
    ],
    // crm locations
    local_table: {
      city: "",
      id: "",
      state: "",
      street: "",
      taxzone: "",
      unit: "",
      zip: "",
      zone_id: "",
    },
    foreign_table: {},
  },
  job_details: {
    columns: [
      "id",
      "job_type_id",
      "job_type_name",
      "job_number",
      "job_status",
      "job_start_time",
      "project_id",
      "job_completion_time",
      "business_unit_id",
      "location_id",
      "customer_details_id",
      "campaign_id",
      "created_by_id",
      "lead_call_id",
      "booking_id",
      "sold_by_id",
    ],
    // jpm -  jobs, job-types
    local_table: {
      booking_id: "",
      campaign_id: "",
      created_by_id: "",
      id: "",
      job_completion_time: "",
      job_number: "",
      job_start_time: "",
      job_type_id: "",
      job_type_name: "",
      lead_call_id: "",
      project_id: "",
      sold_by_id: "",
    },
    foreign_table: {
      business_unit__id: "",
      location__id: "",
      customer_details__id: "",
    },
  },
  vendor: {
    columns: ["id", "name", "is_active"],
    // inventory vendors
    local_table: {
      id: "",
      is_active: "",
      name: "",
    },
    foreign_table: {},
  },
  sku_details: {
    columns: ["id", "sku_name", "sku_type", "sku_unit_price", "vendor_id"],
    // materials, equipment, invoices_item
    local_table: {
      id: "",
      sku_name: "", // skuCode
      sku_type: "",
      sku_unit_price: "",
    },
    foreign_table: {
      vendor__id: "", // primaryVendor['vendorId']
    },
  },
  cogs_equipment: {
    // invoice api -- items
    columns: [
      "id",
      "quantity",
      "total_cost",
      "job_details_id",
      "sku_details_id",
    ],
    local_table: {
      id: "",
      quantity: "",
      total_cost: "",
    },
    foreign_table: {
      job_details__id: "",
      sku_details__id: "",
    },
  },
  technician: {
    columns: ["id", "name", "business_unit_id"],
    local_table: {
      id: "",
      name: "",
    },
    foreign_table: {
      business_unit__id: "",
    },
  },
  cogs_labor: {
    columns: [
      "id",
      "paid_duration",
      "burden_rate",
      "labor_cost",
      "burden_cost",
      "activity",
      "paid_time_type",
      "job_details_id",
      "technician_id",
    ],
    // payroll - gross pay items, payrolls
    local_table: {
      activity: "",
      burden_cost: "",
      burden_rate: "",
      id: "", // running number for cogs
      labor_cost: "",
      paid_duration: "",
      paid_time_type: "",
    },
    foreign_table: {
      job_details__id: "",
      technician__id: "",
    },
  },
  cogs_material: {
    columns: [
      "id",
      "quantity",
      "total_cost",
      "job_details_id",
      "sku_details_id",
    ],
    // invoice api-- items
    local_table: {
      id: "",
      quantity: "",
      total_cost: "",
    },
    foreign_table: {
      job_details__id: "",
      sku_details__id: "",
    },
  },
  invoice: {
    columns: [
      "id",
      "is_trial",
      "date",
      "subtotal",
      "tax",
      "total",
      "invoice_type_id",
      "invoice_type_name",
      "job_details_id",
    ],
    // invoice
    local_table: {
      date: "",
      id: "",
      invoice_type_id: "",
      invoice_type_name: "",
      is_trialdata: "",
      subtotal: "",
      tax: "",
      total: "",
    },
    foreign_table: {
      job_details__id: "",
    },
  },
  gross_profit: {
    columns: [
      "id",
      "revenue",
      "po_cost",
      "equipment_cost",
      "material_cost",
      "labor_cost",
      "burden",
      "gross_profit",
      "gross_margin",
      "units",
      "labor_hours",
      "invoice_id",
    ],
    local_table: {
      burden: "",
      equipment_cost: "",
      gross_margin: "",
      gross_profit: "",
      id: "",
      labor_cost: "",
      labor_hours: "",
      material_cost: "",
      po_cost: "",
      revenue: "",
      units: "",
    },
    foreign_table: {
      invoice__id: "",
    },
  },
};

const main_api_list = {
  // legal_entity: [
  //   {
  //     table_name: "legal_entity",
  //   },
  // ],
  // business_unit: [
  //   {
  //     api_group: "settings",
  //     api_name: "business-units",
  //     table_name: "business_unit",
  //   },
  // ],
  // customer_details: [
  //   {
  //     api_group: "crm",
  //     api_name: "customers",
  //     table_name: "customer_details",
  //   },
  // ],
  // location: [
  //   {
  //     api_group: "crm",
  //     api_name: "locations",
  //     table_name: "location",
  //   },
  // ],
  // job_details: [
  //   {
  //     api_group: "jpm",
  //     api_name: "jobs",
  //     table_name: "job_details",
  //   },
  //   {
  //     api_group: "jpm",
  //     api_name: "job-types",
  //     table_name: "job_details",
  //   },
  // ],
  // vendor: [
  //   {
  //     api_group: "inventory",
  //     api_name: "vendors",
  //     table_name: "vendor",
  //   },
  // ],
  sku_details: [
    {
      api_group: "pricebook",
      api_name: "materials",
      table_name: "sku_details",
    },
    {
      api_group: "pricebook",
      api_name: "equipment",
      table_name: "sku_details",
    },
  ],
  // technician: [
  //   {
  //     api_group: "settings",
  //     api_name: "technicians",
  //     table_name: "technician",
  //   },
  // ],
  // invoices: [
  //   {
  //     api_group: "accounting",
  //     api_name: "invoices",
  //     table_name: ["business_unit", "cogs_equipment", "cogs_material"],
  //   },
  // ],
  // cogs_labor: [
  //   {
  //     api_group: "payroll",
  //     api_name: "gross-pay-items",
  //     table_name: "cogs_labor",
  //   },
  //   {
  //     api_group: "payroll",
  //     api_name: "payrolls",
  //     table_name: "cogs_labor",
  //   },
  // ],
};

const params_header = {
  createdOnOrAfter: "", // 2023-08-01T00:00:00.00Z
  includeTotal: true,
  pageSize: 2000,
};

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

async function find_total_records(data_lake) {
  let total_records = 0;

  // console.log("data_lake: ", data_lake);

  Object.keys(data_lake).map((api_name) => {
    total_records = total_records + data_lake[api_name]["data_pool"].length;
  });

  console.log("total_records: ", total_records);
}

async function fetch_main_data(
  data_lake,
  instance_details,
  main_api_list,
  hvac_tables
) {
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
        Object.keys(main_api_list).map(async (api_key) => {
          if (!data_lake[api_key]) {
            data_lake[api_key] = [];
          }

          if (api_key == "legal_entity") {
            data_lake[api_key] = {
              data_pool: [
                { id: 1, legal_name: "Expert Heating and Cooling" },
                { id: 2, legal_name: "Parket-Arntz Plumbing and Heating" },
                { id: 3, legal_name: "Family Heating and Cooling" },
              ],
            };
          } else {
            const api_list = main_api_list[api_key];

            await Promise.all(
              api_list.map(async (api_data) => {
                const api_group_temp = api_data["api_group"];
                const api_name_temp = api_data["api_name"];

                if (
                  !data_lake[api_key][api_group_temp + "__" + api_name_temp]
                ) {
                  data_lake[api_key][api_group_temp + "__" + api_name_temp] = {
                    data_pool: [],
                  };
                }

                // continuously fetching whole api data
                const data_pool = await getAPIWholeData(
                  access_token,
                  app_key,
                  instance_name,
                  tenant_id,
                  api_group_temp,
                  api_name_temp,
                  params_header
                );

                data_lake[api_key][api_group_temp + "__" + api_name_temp][
                  "data_pool"
                ].push(...data_pool);
              })
            );
          }
        })
      );
    })
  );
}

async function azure_sql_operations(data_lake) {
  // creating a client for azure sql database operations
  const sql_request = await create_sql_connection();
  const sql_pool = await create_sql_pool();

  await create_hvac_schema(sql_request);

  await data_processor(data_lake, sql_pool, sql_request);

  // Close the connection pool
  await sql.close();
}

async function data_processor(data_lake, sql_pool, sql_request) {
  // for (let api_count = 1; api_count < 2; api_count++) {
  // Object.keys(data_lake).length
  const api_name = Object.keys(data_lake)[0];
  const api_data = data_lake[api_name];

  // const stop2 = startStopwatch("inserting data");

  console.log("table_name: ", api_name);

  // console.log("Time taken for inserting data: ", stop2());

  switch (api_name) {
    case "legal_entity": {
      const table_name = main_api_list[api_name][0]["table_name"];
      const data_pool = data_lake[api_name]["data_pool"];
      const header_data = hvac_tables[table_name]["columns"];

      await hvac_flat_data_insertion(
        sql_request,
        data_pool,
        header_data,
        table_name
      );

      break;
    }

    case "business_unit": {
      const table_name = main_api_list[api_name][0]["table_name"];
      const data_pool =
        data_lake[api_name]["settings__business-units"]["data_pool"];
      const header_data = hvac_tables[table_name]["columns"];

      // [
      //   "account_type",
      //   "business_unit_name",
      //   "business_unit_official_name",
      //   "id",
      //   "revenue_type",
      //   "trade_type",
      //   "legal_entity_id",
      // ],

      let final_data_pool = [];

      data_pool.map((record) => {
        final_data_pool.push({
          id: record["id"],
          business_unit_name: record["name"] ? record["name"] : "",
          business_unit_official_name: record["officialName"]
            ? record["officialName"]
            : "",
          account_type: kpi_data[record["id"]]["Account Type"]
            ? kpi_data[record["id"]]["Account Type"]
            : "",
          revenue_type: kpi_data[record["id"]]["Revenue Type"]
            ? kpi_data[record["id"]]["Revenue Type"]
            : "",
          trade_type: kpi_data[record["id"]]["Trade Type"]
            ? kpi_data[record["id"]]["Trade Type"]
            : "",
          legal_entity_id: record["instance_id"],
        });
      });

      await hvac_flat_data_insertion(
        sql_request,
        final_data_pool,
        header_data,
        table_name
      );

      break;
    }

    case "customer_details": {
      const table_name = main_api_list[api_name][0]["table_name"];
      const data_pool = data_lake[api_name]["crm__customers"]["data_pool"];
      const header_data = hvac_tables[table_name]["columns"];

      let final_data_pool = [];

      data_pool.map((record) => {
        final_data_pool.push({
          id: record["id"],
          name: record["name"] ? record["name"] : "",
          is_active: record["active"] ? 1 : 0,
          type: record["type"] ? record["type"] : "",
          creation_date: record["createdOn"],
          address_street: record["address"]["street"]
            ? record["address"]["street"]
            : "",
          address_unit: record["address"]["unit"]
            ? record["address"]["unit"]
            : "",
          address_city: record["address"]["city"]
            ? record["address"]["city"]
            : "",
          address_state: record["address"]["state"]
            ? record["address"]["state"]
            : "",
          address_zip: record["address"]["zip"] ? record["address"]["zip"] : "",
        });
      });

      await hvac_flat_data_insertion(
        sql_request,
        final_data_pool,
        header_data,
        table_name
      );

      break;
    }

    case "location": {
      const table_name = main_api_list[api_name][0]["table_name"];
      const data_pool = data_lake[api_name]["crm__locations"]["data_pool"];
      const header_data = hvac_tables[table_name]["columns"];

      let final_data_pool = [];

      // console.log("data_pool: ", data_pool);
      // console.log("header_data: ", header_data);

      data_pool.map((record) => {
        final_data_pool.push({
          id: record["id"],
          street: record["address"]["street"]
            ? record["address"]["street"]
            : "",
          unit: record["address"]["unit"] ? record["address"]["unit"] : "",
          city: record["address"]["city"] ? record["address"]["city"] : "",
          state: record["address"]["state"] ? record["address"]["state"] : "",
          zip: record["address"]["zip"] ? record["address"]["zip"] : "",
          taxzone: record["taxZoneId"] ? record["taxZoneId"] : 0,
          zone_id: record["zoneId"] ? record["zoneId"] : 0,
        });
      });

      // console.log("final_data_pool: ", final_data_pool);
      // console.log("header_data: ", header_data);

      await hvac_flat_data_insertion(
        sql_request,
        final_data_pool,
        header_data,
        table_name
      );

      break;
    }

    case "job_details": {
      const table_name = main_api_list[api_name][0]["table_name"];
      const jobs_data_pool = data_lake[api_name]["jpm__jobs"]["data_pool"];
      const job_types_data_pool =
        data_lake[api_name]["jpm__job-types"]["data_pool"];

      const header_data = hvac_tables[table_name]["columns"];

      let final_data_pool = [];

      // console.log("jobs_data_pool: ", jobs_data_pool);
      // console.log("header_data: ", header_data);

      jobs_data_pool.map((record) => {
        let job_type_name = "";
        job_types_data_pool.map((job_types_record) => {
          if (record["jobTypeId"] == job_types_record["id"]) {
            job_type_name = job_types_record["name"];
          }
        });
        final_data_pool.push({
          id: record["id"],
          job_type_id: record["jobTypeId"] ? record["jobTypeId"] : 0,
          job_type_name: job_type_name ? job_type_name : "default_job",
          job_number: record["jobNumber"],
          job_status: record["jobStatus"],
          job_start_time: record["createdOn"] ? record["createdOn"] : "",
          project_id: record["projectId"] ? record["projectId"] : 0,
          job_completion_time: record["completedOn"]
            ? record["completedOn"]
            : "",
          business_unit_id: record["businessUnitId"]
            ? record["businessUnitId"]
            : 0,
          location_id: record["locationId"] ? record["locationId"] : 0,
          customer_details_id: record["customerId"] ? record["customerId"] : 0,
          campaign_id: record["campaignId"] ? record["campaignId"] : 0,
          created_by_id: record["createdById"] ? record["createdById"] : 0,
          leadCallId: record["leadCallId"] ? record["leadCallId"] : 0,
          booking_id: record["bookingId"] ? record["bookingId"] : 0,
          sold_by_id: record["soldById"] ? record["soldById"] : 0,
        });
      });

      console.log("final_data_pool: ", final_data_pool);
      // console.log("header_data: ", header_data);

      await hvac_flat_data_insertion(
        sql_request,
        final_data_pool,
        header_data,
        table_name
      );

      break;
    }

    case "vendor": {
      const table_name = main_api_list[api_name][0]["table_name"];
      const data_pool = data_lake[api_name]["inventory__vendors"]["data_pool"];
      const header_data = hvac_tables[table_name]["columns"];

      let final_data_pool = [];

      // console.log("data_pool: ", data_pool);
      // console.log("header_data: ", header_data);

      data_pool.map((record) => {
        final_data_pool.push({
          id: record["id"],
          name: record["name"] ? record["name"] : "",
          is_active: record["active"] ? 1 : 0,
        });
      });

      // console.log("final_data_pool: ", final_data_pool);
      // console.log("header_data: ", header_data);

      await hvac_flat_data_insertion(
        sql_request,
        final_data_pool,
        header_data,
        table_name
      );

      break;
    }

    case "technician": {
      const table_name = main_api_list[api_name][0]["table_name"];
      const data_pool =
        data_lake[api_name]["settings__technicians"]["data_pool"];
      const header_data = hvac_tables[table_name]["columns"];

      let final_data_pool = [];

      // console.log("data_pool: ", data_pool);
      // console.log("header_data: ", header_data);

      data_pool.map((record) => {
        final_data_pool.push({
          id: record["id"],
          name: record["name"] ? record["name"] : "",
          business_unit_id: record["businessUnitId"]
            ? record["businessUnitId"]
            : record["instance_id"],
        });
      });

      console.log("final_data_pool: ", final_data_pool);
      // console.log("header_data: ", header_data);

      await hvac_flat_data_insertion(
        sql_request,
        final_data_pool,
        header_data,
        table_name
      );

      break;
    }

    case "sku_details": {
      const table_name = main_api_list[api_name][0]["table_name"];
      const materials_data_pool =
        data_lake[api_name]["pricebook__materials"]["data_pool"];
      const equipment_data_pool =
        data_lake[api_name]["pricebook__equipment"]["data_pool"];
      const header_data = hvac_tables[table_name]["columns"];

      let final_data_pool = [];

      console.log("data_pool: ", materials_data_pool);
      // console.log("data_pool: ", equipment_data_pool);
      // console.log("header_data: ", header_data);

      materials_data_pool.map((record) => {
        let vendor_id = record["instance_id"];
        if (record["primaryVendor"]) {
          vendor_id = record["primaryVendor"]["vendorId"]
            ? record["primaryVendor"]["vendorId"]
            : record["instance_id"];
        }
        final_data_pool.push({
          id: record["id"],
          sku_name: record["code"],
          sku_type: "Material",
          sku_unit_price: record["cost"] ? record["cost"] : 0,
          vendor_id: vendor_id,
        });
      });

      equipment_data_pool.map((record) => {
        let vendor_id = record["instance_id"];
        if (record["primaryVendor"]) {
          vendor_id = record["primaryVendor"]["vendorId"]
            ? record["primaryVendor"]["vendorId"]
            : record["instance_id"];
        }
        final_data_pool.push({
          id: record["id"],
          sku_name: record["code"] ? record["code"] : "",
          sku_type: "Equipment",
          sku_unit_price: record["cost"] ? record["cost"] : 0,
          vendor_id: vendor_id,
        });
      });

      // console.log("final_data_pool: ", final_data_pool);
      // console.log("header_data: ", header_data);

      await hvac_flat_data_insertion(
        sql_request,
        final_data_pool,
        header_data,
        table_name
      );

      break;
    }

    default: {
      console.log("default");
    }
  }
  // }
}

// for automatic mass ETL
async function start_pipeline() {
  const data_lake = {};

  // fetching all data from Service Titan's API
  const stop1 = startStopwatch("data fetching");
  await fetch_main_data(
    data_lake,
    instance_details,
    main_api_list,
    hvac_tables
  ); // taking 3 mins to fetch all data

  console.log("data_lake: ", data_lake);

  console.log("Time taken for fetching data: ", stop1());

  // await find_total_length(data_lake);

  await azure_sql_operations(data_lake);
}

start_pipeline();
