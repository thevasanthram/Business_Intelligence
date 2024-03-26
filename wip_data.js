const create_sql_connection = require("./modules/create_sql_connection");
const hvac_data_insertion = require("./modules/hvac_data_insertion");

async function add_suffix(id, instance_id) {
  let suffix = "";

  // Append suffix based on instance_id
  if (instance_id == 1 && id != 1 && id != 2 && id != 3) {
    suffix = "_01";
  } else if (instance_id == 2 && id != 1 && id != 2 && id != 3) {
    suffix = "_02";
  } else if (instance_id == 3 && id != 1 && id != 2 && id != 3) {
    suffix = "_03";
  }

  // Append suffix to id
  let modified_id = id + suffix;

  // Return the modified id
  return modified_id;
}

async function wip_data() {
  let sql_request = "";
  do {
    sql_request = await create_sql_connection();
  } while (!sql_request);

  const projects_wip_data_header = {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      number: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      status: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      billed_amount: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      balance: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      contract_value: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      sold_contract_value: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      budget_expense: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      budget_hours: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      inventory_bill_amount: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      po_cost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      po_returns: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      equipment_cost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      material_cost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      labor_cost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      labor_hours: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      burden: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      accounts_receivable: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      expense: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      income: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      current_liability: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      membership_liability: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      actual_business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      customer_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      actual_customer_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      location_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      actual_location_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      startDate: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      targetCompletionDate: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      actualCompletionDate: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      createdOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      modifiedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      UTC_update_date: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
    },
  };

  const business_unit_query = "SELECT * FROM business_unit;";
  const business_result = await sql_request.query(business_unit_query);

  const business_unit_data_pool = {};
  business_result["recordsets"][0].map((record) => {
    business_unit_data_pool[record["id"].split("_")[0]] = record;
  });

  const wip_query = "SELECT * FROM projects_wip_data;";
  const wip_result = await sql_request.query(wip_query);

  //   console.log(wip_result["recordsets"][0]);

  const wip_final_data_pool = [];

  await Promise.all(
    wip_result["recordsets"][0].map(async (record) => {
      const business_unit_id = record["business_unit_id"];

      let instance_id = "";
      if (
        business_unit_id == 1 ||
        business_unit_id == 2 ||
        business_unit_id == 3
      ) {
        instance_id = String(business_unit_id);
      } else {
        instance_id =
          business_unit_data_pool[business_unit_id]["legal_entity_id"];
      }
      // ===

      record["id"] = await add_suffix(record["id"], instance_id);
      if (record["business_unit_id"]) {
        record["business_unit_id"] = await add_suffix(
          record["business_unit_id"],
          instance_id
        );
      }

      if (record["actual_business_unit_id"]) {
        record["actual_business_unit_id"] = await add_suffix(
          record["actual_business_unit_id"],
          instance_id
        );
      }

      if (record["customer_details_id"]) {
        record["customer_details_id"] = await add_suffix(
          record["customer_details_id"],
          instance_id
        );
      }

      if (record["actual_customer_details_id"]) {
        record["actual_customer_details_id"] = await add_suffix(
          record["actual_customer_details_id"],
          instance_id
        );
      }

      if (record["location_id"]) {
        record["location_id"] = await add_suffix(
          record["location_id"],
          instance_id
        );
      }

      if (record["actual_location_id"]) {
        record["actual_location_id"] = await add_suffix(
          record["actual_location_id"],
          instance_id
        );
      }

      wip_final_data_pool.push(record);
    })
  );

  console.log("wip_final_data_pool: ", wip_final_data_pool.length);

  const feedback = await hvac_data_insertion(
    sql_request,
    wip_final_data_pool,
    projects_wip_data_header["columns"],
    "projects_wip_data_duplicate"
  );

  console.log("feedback: ", feedback);
}

wip_data();
