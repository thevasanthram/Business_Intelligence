const getAccessToken = require("./../modules/get_access_token");
const get_wip_report_data = require("./../modules/get_wip_report_data");
const hvac_data_insertion = require("./../modules/hvac_data_insertion");
const create_sql_connection = require("./../modules/create_sql_connection");

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
};

async function wip_report() {
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
            data_pool,
            page_count
          ));
      } while (has_error_occured);

      if (data_pool.length > 0) {
        data_lake[instance_name] = data_pool;
      }

      console.log("ended");
    })
  );

  // creating a client for azure sql database operations
  let sql_request = "";
  do {
    sql_request = await create_sql_connection();
  } while (!sql_request);

  const header_data = wip_header["columns"];

  //   console.log("header_data: ", header_data);
  //   console.log(
  //     "sample_data: ",
  //     data_lake["Expert Heating and Cooling Co LLC"][0]
  //   );

  await Promise.all(
    Object.keys(data_lake).map(async (instance_name) => {
      const data_pool = data_lake[instance_name];

      do {
        wip_response[instance_name]["status"] = await hvac_data_insertion(
          sql_request,
          data_pool,
          header_data,
          "wip_report"
        );
      } while (wip_response[instance_name]["status"] != "success");
    })
  );

  console.log("Data written to DB");
}

wip_report();