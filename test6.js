const sql = require("mssql");
const fs = require("fs");

// modules
const create_sql_connection = require("./modules/create_sql_connection");
const create_sql_pool = require("./modules/create_sql_pool");
const getAccessToken = require("./modules/get_access_token");
const getAPIWholeData = require("./modules/get_api_whole_data");
const hvac_data_insertion = require("./modules/hvac_data_insertion");
const hvac_flat_data_insertion = require("./modules/hvac_flat_data_insertion");
const find_lenghthiest_header = require("./modules/find_lengthiest_header");
const create_hvac_schema = require("./modules/create_hvac_schema");
const flush_hvac_schema = require("./modules/flush_hvac_schema");
const flush_hvac_data = require("./modules/flush_hvac_data");
const kpi_data = require("./modules/updated_business_unit_details");
const { compose } = require("async");

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

let timezoneOffsetHours = 0; // 0 hours ahead of UTC
let timezoneOffsetMinutes = 0; // 0 minutes ahead of UTC

// Check if the date is in daylight saving time (PDT)
const today = new Date();
const daylightSavingStart = new Date(today.getFullYear(), 2, 14); // March 14
const daylightSavingEnd = new Date(today.getFullYear(), 10, 7); // November 7

if (today >= daylightSavingStart && today < daylightSavingEnd) {
  // Date is in PDT
  timezoneOffsetHours = 7;
} else {
  // Date is in PST
  timezoneOffsetHours = 8;
}

let createdBeforeTime = new Date();

createdBeforeTime.setHours(createdBeforeTime.getHours() + timezoneOffsetHours);
createdBeforeTime.setMinutes(0); // Set minutes to 0
createdBeforeTime.setSeconds(0);
createdBeforeTime.setMilliseconds(0);

const params_header = {
  createdOnOrAfter: "", // 2023-06-01T00:00:00.00Z
  createdBefore: "", //createdBeforeTime.toISOString()
  // modifiedOnOrAfter: "2023-12-12T00:00:00.00Z", // 2023-06-01T00:00:00.00Z
  // modifiedBefore: "2023-12-13T00:00:00.00Z", //createdBeforeTime.toISOString()
  includeTotal: true,
  pageSize: 2000,
  active: "any",
};

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

      await Promise.all(
        Object.keys(main_api_list).map(async (api_key) => {
          if (!data_lake[api_key]) {
            data_lake[api_key] = {};
          }

          if (api_key == "legal_entity") {
            data_lake[api_key] = {
              data_pool: {
                1: { id: 1, legal_name: "Expert Heating and Cooling" },
                2: { id: 2, legal_name: "Parket-Arntz Plumbing and Heating" },
                3: { id: 3, legal_name: "Family Heating and Cooling" },
              },
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
                  if (api_name_temp == "gross-pay-items") {
                    data_lake[api_key][api_group_temp + "__" + api_name_temp] =
                      {
                        data_pool: [],
                      };
                  } else {
                    data_lake[api_key][api_group_temp + "__" + api_name_temp] =
                      {
                        data_pool: {},
                      };
                  }
                }

                // signing a new access token in Service Titan's API
                let access_token = "";

                do {
                  access_token = await getAccessToken(client_id, client_secret);
                } while (!access_token);

                // continuously fetching whole api data
                let data_pool_object = {};
                let data_pool = [];
                let page_count = 0;
                let has_error_occured = false;

                do {
                  ({
                    data_pool_object,
                    data_pool,
                    page_count,
                    has_error_occured,
                  } = await getAPIWholeData(
                    access_token,
                    app_key,
                    instance_name,
                    tenant_id,
                    api_group_temp,
                    api_name_temp,
                    params_header,
                    data_pool_object,
                    data_pool,
                    page_count
                  ));
                } while (has_error_occured);

                if (api_name_temp == "gross-pay-items") {
                  data_lake[api_key][api_group_temp + "__" + api_name_temp][
                    "data_pool"
                  ] = [
                    ...data_lake[api_key][
                      api_group_temp + "__" + api_name_temp
                    ]["data_pool"],
                    ...data_pool,
                  ];
                } else {
                  data_lake[api_key][api_group_temp + "__" + api_name_temp][
                    "data_pool"
                  ] = {
                    ...data_lake[api_key][
                      api_group_temp + "__" + api_name_temp
                    ]["data_pool"],
                    ...data_pool_object,
                  }; //;
                }
              })
            );
          }
        })
      );
    })
  );
}

async function starter() {
  const data_lake_true = {};
  const data_lake_false = {};

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
    // campaigns: [
    //   {
    //     api_group: "marketing",
    //     api_name: "campaigns",
    //     table_name: "campaigns",
    //   },
    // ],
    // bookings: [
    //   {
    //     api_group: "crm",
    //     api_name: "bookings",
    //     table_name: "bookings",
    //   },
    // ],
    // customer_details: [
    //   {
    //     api_group: "crm",
    //     api_name: "customers",
    //     table_name: "customer_details",
    //   },
    // ],
    location: [
      {
        api_group: "crm",
        api_name: "locations",
        table_name: "location",
      },
    ],
    // projects: [
    //   {
    //     api_group: "jpm",
    //     api_name: "projects",
    //     table_name: "projects",
    //   },
    // ],
    // call_details: [
    //   {
    //     api_group: "telecom",
    //     api_name: "calls",
    //     table_name: "call_details",
    //   },
    // ],
    // job_details: [
    // {
    //   api_group: "jpm",
    //   api_name: "jobs",
    //   table_name: "job_details",
    // },
    // {
    //   api_group: "jpm",
    //   api_name: "job-types",
    //   table_name: "job_details",
    // },
    // ],
    // appointments: [
    //   {
    //     api_group: "jpm",
    //     api_name: "appointments",
    //     table_name: "appointments",
    //   },
    // ],
    // sales_details: [
    //   {
    //     api_group: "sales",
    //     api_name: "estimates",
    //     table_name: "sales_details",
    //   },
    // ],
    // vendor: [
    //   {
    //     api_group: "inventory",
    //     api_name: "vendors",
    //     table_name: "vendor",
    //   },
    // ],
    // technician: [
    //   {
    //     api_group: "settings",
    //     api_name: "technicians",
    //     table_name: "technician",
    //   },
    // ],
    // appointment_assignments: [
    //   {
    //     api_group: "dispatch",
    //     api_name: "appointment-assignments",
    //     table_name: "appointment_assignments",
    //   },
    // ],
    // non_job_appointments: [
    //   {
    //     api_group: "dispatch",
    //     api_name: "non-job-appointments",
    //     table_name: "non_job_appointments",
    //   },
    // ],
    // sku_details: [
    //   {
    //     api_group: "pricebook",
    //     api_name: "materials",
    //     table_name: "sku_details",
    //   },
    //   {
    //     api_group: "pricebook",
    //     api_name: "equipment",
    //     table_name: "sku_details",
    //   },
    //   {
    //     api_group: "pricebook",
    //     api_name: "services",
    //     table_name: "sku_details",
    //   },
    // ],
    invoice: [
      {
        api_group: "accounting",
        api_name: "invoices",
        table_name: ["business_unit", "cogs_equipment", "cogs_material"],
      },
    ],
    // purchase_order: [
    //   {
    //     api_group: "inventory",
    //     api_name: "purchase-orders",
    //     table_name: "purchase_order",
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

  const hvac_tables = [];
  const result_true = await fetch_main_data(
    data_lake_true,
    instance_details,
    main_api_list,
    hvac_tables
  );

  const invoice_data_pool =
    data_lake_true["invoice"]["accounting__invoices"]["data_pool"];

  const location_data_pool =
    data_lake_true["location"]["crm__locations"]["data_pool"];

  let count = 0;

  Object.keys(invoice_data_pool).map((record_id) => {
    const record = invoice_data_pool[record_id];

    if (record["location"]) {
      if (location_data_pool[record["location"]["id"]]) {
        count = count + 1;
      }
    }
  });

  // const data_pool2 =
  //   data_lake_true["cogs_labor"]["payroll__payrolls"]["data_pool"];

  // const data_pool2 =
  //   data_lake_true["sku_details"]["pricebook__services"]["data_pool"];

  // console.log("total records count: ", Object.keys(data_pool));

  console.log("total records count: ", count);

  // console.log("total records count: ", Object.keys(data_pool2).length);

  // const unique_business_units = [...new Set(Object.keys(first_table))];

  // console.log("unique_business_units: ", unique_business_units.length);
}

starter();
