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
const kpi_data = require("./modules/business_units_details");
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
  createdOnOrAfter: "2023-06-01T00:00:00.00Z", // 2023-08-01T00:00:00.00Z
  createdBefore: createdBeforeTime.toISOString(),
  includeTotal: true,
  pageSize: 2000,
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
  const data_lake = {};
  main_api_list = {
    bookings: [
      {
        api_group: "jpm",
        api_name: "jobs",
        table_name: "invoices",
      },
      {
        api_group: "sales",
        api_name: "estimates",
        table_name: "jobs",
      },
      {
        api_group: "jpm",
        api_name: "appointments",
        table_name: "invoices",
      },
      {
        api_group: "dispatch",
        api_name: "appointment-assignments",
        table_name: "invoices",
      },
      {
        api_group: "dispatch",
        api_name: "non-job-appointments",
        table_name: "invoices",
      },
      {
        api_group: "accounting",
        api_name: "invoices",
        table_name: "invoices",
      },
      {
        api_group: "crm",
        api_name: "bookings",
        table_name: "bookings",
      },
      {
        api_group: "telecom",
        api_name: "calls",
        table_name: "bookings",
      },
    ],
  };
  const hvac_tables = [];
  const result = await fetch_main_data(
    data_lake,
    instance_details,
    main_api_list,
    hvac_tables
  );

  const first_table =
    data_lake["bookings"]["accounting__invoices"]["data_pool"];
  const comparing_table1 = data_lake["bookings"]["jpm__jobs"]["data_pool"];
  const comparing_table2 =
    data_lake["bookings"]["sales__estimates"]["data_pool"];
  const comparing_table3 =
    data_lake["bookings"]["jpm__appointments"]["data_pool"];
  const comparing_table4 =
    data_lake["bookings"]["dispatch__appointment-assignments"]["data_pool"];
  const comparing_table5 =
    data_lake["bookings"]["dispatch__non-job-appointments"]["data_pool"];
  const comparing_table6 =
    data_lake["bookings"]["accounting__invoices"]["data_pool"];
  const comparing_table7 = data_lake["bookings"]["crm__bookings"]["data_pool"];
  const comparing_table8 = data_lake["bookings"]["telecom__calls"]["data_pool"];

  const unique_job_id = [
    ...new Set(
      Object.keys(first_table).map((record_id) => {
        const record = first_table[record_id];

        if (record["job"]) {
          return record["job"]["id"];
        }
      })
    ),
  ];
  console.log("self job_ids count: ", unique_job_id.length);

  // comparison ==================================
  const new_job_id1 = {};

  Object.keys(comparing_table1).map((record_id) => {
    const record = comparing_table1[record_id];

    if (record["id"]) {
      new_job_id1[record["id"]] = "something";
    }
  });

  let count1 = 0;
  unique_job_id.map((job_id) => {
    if (new_job_id1[job_id]) {
      count1 = count1 + 1;
    }
  });

  console.log("jpm__jobs", count1);

  // comparison ==================================
  const new_job_id2 = {};

  Object.keys(comparing_table2).map((record_id) => {
    const record = comparing_table2[record_id];

    if (record["jobId"]) {
      new_job_id2[record["jobId"]] = "something";
    }
  });

  let count2 = 0;
  unique_job_id.map((job_id) => {
    if (new_job_id2[job_id]) {
      count2 = count2 + 1;
    }
  });

  console.log("sales__estimates", count2);

  // comparison ==================================
  const new_job_id3 = {};

  Object.keys(comparing_table3).map((record_id) => {
    const record = comparing_table3[record_id];

    if (record["jobId"]) {
      new_job_id3[record["jobId"]] = "something";
    }
  });

  let count3 = 0;
  unique_job_id.map((job_id) => {
    if (new_job_id3[job_id]) {
      count3 = count3 + 1;
    }
  });

  console.log("jpm__appointments", count3);

  // comparison ==================================
  const new_job_id4 = {};

  Object.keys(comparing_table4).map((record_id) => {
    const record = comparing_table4[record_id];

    if (record["jobId"]) {
      new_job_id4[record["jobId"]] = "something";
    }
  });

  let count4 = 0;
  unique_job_id.map((job_id) => {
    if (new_job_id4[job_id]) {
      count4 = count4 + 1;
    }
  });

  console.log("dispatch__appointment-assignments", count4);

  // comparison ==================================
  const new_job_id5 = {};

  Object.keys(comparing_table5).map((record_id) => {
    const record = comparing_table5[record_id];

    if (record["id"]) {
      new_job_id5[record["id"]] = "something";
    }
  });

  let count5 = 0;
  unique_job_id.map((job_id) => {
    if (new_job_id5[job_id]) {
      count5 = count5 + 1;
    }
  });

  console.log("dispatch__non-job-appointments", count5);

  // comparison ==================================
  const new_job_id6 = {};

  Object.keys(comparing_table6).map((record_id) => {
    const record = comparing_table6[record_id];

    if (record["job"]) {
      new_job_id6[record["job"]["id"]] = "something";
    }
  });

  let count6 = 0;
  unique_job_id.map((job_id) => {
    if (new_job_id6[job_id]) {
      count6 = count6 + 1;
    }
  });

  console.log("accounting__invoices", count6);

  // comparison ==================================
  const new_job_id7 = {};

  Object.keys(comparing_table7).map((record_id) => {
    const record = comparing_table7[record_id];

    if (record["jobId"]) {
      new_job_id7[record["jobId"]] = "something";
    }
  });

  let count7 = 0;
  unique_job_id.map((job_id) => {
    if (new_job_id7[job_id]) {
      count7 = count7 + 1;
    }
  });

  console.log("crm_bookings", count7);

  // comparison ==================================
  const new_job_id8 = {};

  Object.keys(comparing_table8).map((record_id) => {
    const record = comparing_table8[record_id];

    if (record["jobNumber"]) {
      new_job_id8[record["jobNumber"]] = "something";
    }
  });

  let count8 = 0;
  unique_job_id.map((job_id) => {
    if (new_job_id8[job_id]) {
      count8 = count8 + 1;
    }
  });

  console.log("telecom__calls", count8);
}

starter();
