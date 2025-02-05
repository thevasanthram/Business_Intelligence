const getAccessToken = require("./../modules/get_access_token");
const get_wip_report_data = require("./../modules/get_wip_report_data");
const hvac_data_insertion = require("./../modules/hvac_data_insertion");
const create_sql_connection = require("./../modules/create_sql_connection");
const fs = require("fs");

const data_lake = {};

const instance_details = [
  {
    instance_name: "Expert Heating and Cooling Co LLC",
    tenant_id: 1011756844,
    app_key: "ak1.ztsdww9rvuk0sjortd94dmxwx",
    client_id: "cid.jk53hfwwcq6a1zgtbh96byil4",
    client_secret: "cs1.2hdc1yd19hpxzmdeg5rfuc6i3smpxy9iei0yhq1p7qp8mwyjda",
    wip_report_id: 87933193,
  },
  {
    instance_name: "PARKER-ARNTZ PLUMBING AND HEATING, INC.",
    tenant_id: 1475606437,
    app_key: "ak1.w9fgjo8psqbyi84vocpvzxp8y",
    client_id: "cid.r82bhd4u7htjv56h7sqjk0jya",
    client_secret: "cs1.4q3yjgyhjb9yaeietpsoozzc8u2qgw80j8ze43ovz1308e7zz7",
    wip_report_id: 85353848,
  },
  {
    instance_name: "Family Heating & Cooling Co LLC",
    tenant_id: 1056112968,
    app_key: "ak1.h0wqje4yshdqvn1fso4we8cnu",
    client_id: "cid.qlr4t6egndd4mbvq3vu5tef11",
    client_secret: "cs1.v9jhueeo6kgcjx5in1r8716hpnmuh6pbxiddgsv5d3y0822jay",
    wip_report_id: 75359909,
  },
  {
    instance_name: "Swift Air Mechanical LLC",
    tenant_id: 2450322465,
    app_key: "ak1.bquspwfwag2gqtls6lgi0dl1b",
    client_id: "cid.ls3q3h7dmtoaiu3o5y5oddgca",
    client_secret: "cs1.75f7fqxtilh4xj5vkym3fda1aaz9kmpv701w91kaej9w2r2rxp",
    wip_report_id: 58999783,
  },
  {
    instance_name: "Jetstream Mechanicals LLC",
    tenant_id: 2450309401,
    app_key: "ak1.u9fb0767d46nh1mid81ow3pgz",
    client_id: "cid.my53hbp127i8vzxpn4o4h54ho",
    client_secret: "cs1.3t0bo8k8b8xgrnbdyf9j4cj8zeq2upq2z72x9h4wkmf1w692jc",
    wip_report_id: 180419843,
  },
  {
    instance_name: "All Star Plumbing and Heating",
    tenant_id: 3586728484,
    app_key: "ak1.1chwmlgkcdmmtx8voain2x95w",
    client_id: "cid.3moaukr4ztqov5abxuulbrxst",
    client_secret: "cs1.6u48l7qv7tbria2n5masyg3g5s6ropsm09ewj9317h8x3g6md7",
    wip_report_id: 14726308,
  },
];

const instance_list = [
  "Expert Heating and Cooling Co LLC",
  "PARKER-ARNTZ PLUMBING AND HEATING, INC.",
  "Family Heating & Cooling Co LLC",
  "Swift Air Mechanical LLC",
  "Jetstream Mechanicals LLC",
  "All Star Plumbing and Heating",
];

const wip_header = {
  columns: {
    Instance_id: {
      data_type: "NVARCHAR20",
      constraint: { primary: true, nullable: false },
    },
    ProjectNumber: {
      data_type: "NVARCHAR20",
      constraint: { primary: true, nullable: false },
    },
    ProjectName: {
      data_type: "NVARCHAR",
      constraint: { nullable: true },
    },
    ProjectStatus: {
      data_type: "NVARCHAR",
      constraint: { nullable: true },
    },
    ProjectContractStartDate: {
      data_type: "DATETIME2",
      constraint: { nullable: true },
    },
    ActualCompletionDate: {
      data_type: "DATETIME2",
      constraint: { nullable: true },
    },
    ContractValue: {
      data_type: "DECIMAL",
      constraint: { nullable: true },
    },
    ChangeOrderValue: {
      data_type: "DECIMAL",
      constraint: { nullable: true },
    },
    CostAdjustment: {
      data_type: "DECIMAL",
      constraint: { nullable: true },
    },
    TotalEstimatedCost: {
      data_type: "DECIMAL",
      constraint: { nullable: true },
    },
    EstimatedMargin: {
      data_type: "DECIMAL",
      constraint: { nullable: true },
    },
    EstimatedMarginPercentage: {
      data_type: "DECIMAL",
      constraint: { nullable: true },
    },
    TotalCost: {
      data_type: "DECIMAL",
      constraint: { nullable: true },
    },
    CostToComplete: {
      data_type: "DECIMAL",
      constraint: { nullable: true },
    },
    PercentCompleteCost: {
      data_type: "DECIMAL",
      constraint: { nullable: true },
    },
    EarnedRevenue: {
      data_type: "DECIMAL",
      constraint: { nullable: true },
    },
    TotalRevenue: {
      data_type: "DECIMAL",
      constraint: { nullable: true },
    },
    OverBilling: {
      data_type: "DECIMAL",
      constraint: { nullable: true },
    },
    UnderBilling: {
      data_type: "DECIMAL",
      constraint: { nullable: true },
    },
    UTC_update_date: {
      data_type: "DATETIME2",
      constraint: { nullable: true },
    },
  },
};

const wip_response = {
  "Expert Heating and Cooling Co LLC": {
    status: "failure",
  },
  "PARKER-ARNTZ PLUMBING AND HEATING, INC.": {
    status: "failure",
  },
  "Family Heating & Cooling Co LLC": {
    status: "failure",
  },
  "Swift Air Mechanical LLC": {
    status: "failure",
  },
  "Jetstream Mechanicals LLC": {
    status: "failure",
  },
  "All Star Plumbing and Heating": {
    status: "failure",
  },
};

async function wip_report(as_of_date) {
  console.log("=========================================");
  console.log("as of date: ", as_of_date);
  console.log("=========================================");

  await Promise.all(
    instance_details.map(async (instance_data) => {
      const instance_name = instance_data["instance_name"];
      const tenant_id = instance_data["tenant_id"];
      const app_key = instance_data["app_key"];
      const client_id = instance_data["client_id"];
      const client_secret = instance_data["client_secret"];
      const wip_report_id = instance_data["wip_report_id"];

      let access_token = "";

      // refreshing for the first time manually
      do {
        access_token = await getAccessToken(client_id, client_secret);
      } while (!access_token);

      const refreshAccessToken = async () => {
        // Signing a new access token in Service Titan's API
        do {
          access_token = await getAccessToken(client_id, client_secret);
        } while (!access_token);
      };

      setInterval(refreshAccessToken, 1000 * 60 * 3);

      // continuously fetching whole api data
      let data_pool = [];
      let page_count = 0;
      let has_error_occured = false;

      do {
        ({ data_pool, page_count, has_error_occured } =
          await get_wip_report_data(
            access_token,
            app_key,
            instance_name,
            tenant_id,
            wip_report_id,
            as_of_date,
            data_pool,
            page_count
          ));
      } while (has_error_occured);

      if (data_pool.length > 0) {
        data_lake[instance_name] = data_pool;
      }
    })
  );

  // creating a client for azure sql database operations
  let sql_request = "";
  do {
    sql_request = await create_sql_connection();
  } while (!sql_request);

  const header_data = wip_header["columns"];

  await Promise.all(
    Object.keys(data_lake).map(async (instance_name) => {
      const data_pool = data_lake[instance_name];

      // fs.writeFile(
      //   `./${instance_name}_wip_report.js`,
      //   JSON.stringify(data_pool),
      //   () => console.log("done")
      // );

      const is_data_exists = `SELECT DISTINCT UTC_update_date FROM wip_report WHERE UTC_update_date LIKE '${as_of_date}%' AND Instance_id = ${String(
        instance_list.indexOf(instance_name) + 1
      )}`;

      const is_data_exists_response = await sql_request.query(is_data_exists);

      if (!is_data_exists_response.recordset.length) {
        do {
          wip_response[instance_name]["status"] = await hvac_data_insertion(
            sql_request,
            data_pool,
            header_data,
            "wip_report",
            "UPDATING"
          );
        } while (wip_response[instance_name]["status"] != "success");
      }
    })
  );
}

async function wip_historical_report() {
  //   const to_dateString = to_date.toISOString().substring(0, 10);
  const current_date = new Date();

  await wip_report(current_date.toISOString().substring(0, 10));

  current_date.setDate(current_date.getDate() + 1);

  current_date.setUTCHours(7, 0, 0, 0);

  let iterator = true;

  do {
    const now = new Date();

    if (now < current_date) {
      // Schedule the next call after an day
      const timeUntilNextBatch = current_date - now; // Calculate milliseconds until the next day
      console.log(
        "timer funtion entering",
        timeUntilNextBatch,
        "<-->",
        current_date
      );

      await new Promise((resolve) => setTimeout(resolve, timeUntilNextBatch));
    } else {
      await wip_report(current_date.toISOString().substring(0, 10));

      if (current_date.toISOString().substring(0, 10) == "01-01-2020") {
        iterator = false;
      } else {
        current_date.setDate(current_date.getDate() + 1);
        current_date.setUTCHours(7, 0, 0, 0);
      }
    }
  } while (iterator);

  console.log("COMPLETED");
}

wip_historical_report();
