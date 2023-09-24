const fs = require("fs");

async function create_hvac_schema(sql_request) {
  try {
    const schema = fs.readFileSync("./modules/hvac_schema.sql", "utf-8");

    const isTableExistsQuery = `SELECT * FROM sys.tables WHERE name='gross_profit';`;

    // console.log("isTableExistsQuery: ", isTableExistsQuery);

    const isTableExists = await sql_request.query(isTableExistsQuery);

    if (isTableExists.recordset.length == 0) {
      await sql_request.query(schema);

      console.log("HVAC Schema created successfully");
    } else {
      console.log("HVAC Schema already exists");
    }
  } catch (error) {
    console.error("Error while creating the hvac schema: ", error);

    create_hvac_schema(sql_request);
  }
}

module.exports = create_hvac_schema;
