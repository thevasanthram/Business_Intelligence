const { table } = require("console");
const create_sql_connection = require("./modules/create_sql_connection");
const hvac_data_insertion = require("./modules/hvac_data_insertion");

const fs = require("fs");

async function sql_hit() {
  // creating a client for azure sql database operations
  let sql_request = "";
  do {
    sql_request = await create_sql_connection();
  } while (!sql_request);

  const target_table_name = "wip_temp";
  const source_table_name = "wip_report";

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

  const wip_report_data = await sql_request.query(
    `SELECT * FROM ${source_table_name} WHERE Instance_id = 1`
  );

  const records = wip_report_data["recordset"];

  console.log(records.length);

  let modified_data = [];

  records.map((record) => {
    let values = Object.values(record);
    const estimatedMargin = values[6] - values[9];
    let modified_values = [
      ...values.slice(0, 10),
      estimatedMargin,
      ...values.slice(10, values.length - 2),
      values[values.length - 1],
    ];

    let modified_record = Object.fromEntries(
      Object.keys(record).map((key, index) => [key, modified_values[index]])
    );

    if (
      record["ProjectNumber"] == "3947_01" &&
      record["UTC_update_date"] == "2024-04-15T00:00:00.000Z"
    ) {
      console.log(modified_record);
    }
    modified_data.push(modified_record);
  });

  // console.log("modified_data: ", modified_data);

  await hvac_data_insertion(
    sql_request,
    Object.values(modified_data),
    wip_header["columns"],
    "wip_report"
  );
}

async function sql_duplicate() {
  let sql_request = "";
  do {
    sql_request = await create_sql_connection();
  } while (!sql_request);

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

  const wip_query = "SELECT * FROM wip_temp;";
  const wip_result = await sql_request.query(wip_query);

  //   console.log(wip_result["recordsets"][0]);

  console.log("wip_result: ", wip_result["recordsets"][0].length);

  const feedback = await hvac_data_insertion(
    sql_request,
    wip_result["recordsets"][0],
    wip_header["columns"],
    "wip_report"
  );

  console.log("feedback: ", feedback);
}

async function total_count_checker() {
  let sql_request = "";

  // Retry connection until successful
  do {
    sql_request = await create_sql_connection();
  } while (!sql_request);

  const tables = [
    "purchase_order",
    "appointments",
    "inventory_bills",
    "technician",
    "sku_details",
    "invoice",
    "vendor",
    "cogs_labor",
    "cogs_material",
    "cogs_service",
    "cogs_equipment",
    "legal_entity",
    "us_cities",
    "business_unit",
    "project_business_unit",
    "employees",
    "campaigns",
    "bookings",
    "customer_details",
    "location",
    "payrolls",
    "job_types",
    "returns",
    "sales_details",
    "projects",
    "project_managers",
    "call_details",
    "job_details",
  ];

  const table_count = {};

  await Promise.all(
    tables.map(async (table_name) => {
      const query = `SELECT COUNT(*) as count FROM ${table_name}`;

      const response = await sql_request.query(query);

      table_count[table_name] = response.recordset[0]["count"];

      // console.log(table_name, ",", response.recordset[0]["count"]);
    })
  );

  let string = Object.keys(table_count)
    .map((table_name) => {
      return table_name + "," + table_count[table_name];
    })
    .join("\n");

  console.log(string);
}

// sql_hit();
// sql_duplicate();
total_count_checker();
