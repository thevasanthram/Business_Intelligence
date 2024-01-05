const create_sql_connection = require("./modules/create_sql_connection");
const sql = require("mssql");

const hvac_merge_insertion = require("./modules/hvac_merge_insertion");

async function optimized_bulk_insertion() {
  const sql_request = await create_sql_connection();

  const data_pool = {
    1: {
      id: 1,
      name: "one",
      class: "one-one",
    },
    2: {
      id: 2,
      name: "three",
      class: "three-three",
    },
    3: {
      id: 3,
      name: "one",
      class: "one-one",
    },
  };

  const header_data = {
    id: {
      data_type: "INT",
      constraint: { primary: true, nullable: false },
    },
    name: {
      data_type: "NVARCHAR",
      constraint: { nullable: true },
    },
    class: {
      data_type: "NVARCHAR",
      constraint: { nullable: true },
    },
  };

  const table_name = "sample_table_testing";

  const response = await hvac_merge_insertion(
    sql_request,
    Object.values(data_pool),
    header_data,
    table_name
  );

  console.log("response: ", response);
}

optimized_bulk_insertion();
