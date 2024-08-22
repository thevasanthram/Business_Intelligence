const sql = require("mssql");
const hvac_data_insertion = require("./../modules/hvac_data_insertion");

async function create_sql_connection(database_name) {
  const config = {
    user: process.env.DB_USER || "pinnacleadmin",
    password: process.env.DB_PASSWORD || "PiTestBi01",
    server: "pinnaclemep.database.windows.net",
    database: database_name,
    options: {
      encrypt: true,
      requestTimeout: 48 * 60 * 60 * 1000,
    },
    connectionTimeout: 3 * 60 * 1000,
  };

  try {
    const pool = new sql.ConnectionPool(config);
    await pool.connect();
    console.log(`Connected to ${database_name}`);
    return pool.request(); // Return a request object tied to this pool
  } catch (err) {
    console.error(`Error connecting to ${database_name}:`, err);
    return null;
  }
}

async function sql_duplicate() {
  let main_sql_request, hvac_sql_request;
  let attempt = 0;
  const maxRetries = 5;

  // Attempt connection to main_hvac_db
  do {
    main_sql_request = await create_sql_connection("main_hvac_db");
    attempt++;
    if (!main_sql_request && attempt < maxRetries) {
      console.log(
        `Retrying to connect to main_hvac_db... (${attempt}/${maxRetries})`
      );
      await new Promise((res) => setTimeout(res, 5000)); // Wait 5 seconds before retrying
    }
  } while (!main_sql_request && attempt < maxRetries);

  if (!main_sql_request) {
    console.error("Failed to connect to main_hvac_db after multiple attempts.");
    return;
  }

  // Reset attempt counter for the second connection
  attempt = 0;

  // Attempt connection to hvac_data_pool
  do {
    hvac_sql_request = await create_sql_connection("hvac_data_pool");
    attempt++;
    if (!hvac_sql_request && attempt < maxRetries) {
      console.log(
        `Retrying to connect to hvac_data_pool... (${attempt}/${maxRetries})`
      );
      await new Promise((res) => setTimeout(res, 5000)); // Wait 5 seconds before retrying
    }
  } while (!hvac_sql_request && attempt < maxRetries);

  if (!hvac_sql_request) {
    console.error(
      "Failed to connect to hvac_data_pool after multiple attempts."
    );
    return;
  }

  const wip_header = {
    columns: {
      Instance_id: { data_type: "NVARCHAR20", constraint: { nullable: false } },
      ProjectId: { data_type: "NVARCHAR20", constraint: { nullable: false } },
      ProjectNumber: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      ProjectName: { data_type: "NVARCHAR", constraint: { nullable: true } },
      CustomerName: { data_type: "NVARCHAR", constraint: { nullable: true } },
      ProjectStatus: { data_type: "NVARCHAR", constraint: { nullable: true } },
      ProjectContractStartDate: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      ActualCompletionDate: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      ContractValue: { data_type: "DECIMAL", constraint: { nullable: true } },
      ChangeOrderValue: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      Retainage: { data_type: "DECIMAL", constraint: { nullable: true } },
      TDRetainage: { data_type: "DECIMAL", constraint: { nullable: true } },
      CostAdjustment: { data_type: "DECIMAL", constraint: { nullable: true } },
      TotalEstimatedCost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      EstimatedMargin: { data_type: "DECIMAL", constraint: { nullable: true } },
      EstimatedMarginPercentage: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      TotalCost: { data_type: "DECIMAL", constraint: { nullable: true } },
      CostToComplete: { data_type: "DECIMAL", constraint: { nullable: true } },
      PercentCompleteCost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      EarnedRevenue: { data_type: "DECIMAL", constraint: { nullable: true } },
      TotalRevenue: { data_type: "DECIMAL", constraint: { nullable: true } },
      InvoiceTotalRevenue: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      BalanceToFinish: { data_type: "DECIMAL", constraint: { nullable: true } },
      OverBilling: { data_type: "DECIMAL", constraint: { nullable: true } },
      UnderBilling: { data_type: "DECIMAL", constraint: { nullable: true } },
      OriginalEstimateTemplate: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      ActualLaborQty: { data_type: "DECIMAL", constraint: { nullable: true } },
      LastInvoiceInvoicedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      TDOriginalEstimatedMargin: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      TDOriginalEstimatedMarginPercentage: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      ProjectBusinessUnit: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      TDInvoiceAndActualLaborCostPlusBurdenCost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      PermitNumber: { data_type: "NVARCHAR", constraint: { nullable: true } },
      UTC_update_date: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
    },
  };

  console.log("Started fetching data from hvac_data_pool...");

  const wip_active_query = "SELECT * FROM wip_active_projects;";
  const wip_result = await hvac_sql_request.query(wip_active_query);

  if (wip_result && wip_result.recordsets && wip_result.recordsets[0]) {
    console.log(
      `Fetched ${wip_result.recordsets[0].length} records from wip_active_projects.`
    );

    const feedback = await hvac_data_insertion(
      main_sql_request,
      wip_result.recordsets[0],
      wip_header.columns,
      "wip_report"
    );

    // console.log("Feedback from data insertion:", feedback);
  } else {
    console.error("No data fetched from hvac_data_pool.");
  }
}

sql_duplicate();
