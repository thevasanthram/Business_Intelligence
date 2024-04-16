const create_sql_connection = require("./modules/create_sql_connection");

async function sql_hit() {
  // creating a client for azure sql database operations
  let sql_request = "";
  do {
    sql_request = await create_sql_connection();
  } while (!sql_request);

  let id = "1000001_01";

  console.log(`SELECT id FROM business_unit WHERE id=${id}`);

  // checking business unit availlable or not for mapping
  const is_business_unit_available = await sql_request.query(
    `SELECT id FROM business_unit WHERE id=${id}`
  );

  console.log("is_business_unit_available: ", is_business_unit_available);
}

sql_hit();
