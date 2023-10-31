const fs = require("fs");
const path = require("path"); // Import the path module to work with file paths

async function create_hvac_schema(sql_request) {
  let create_hvac_schema_status = false;
  try {
    const schemaPath = path.join(__dirname, "hvac_schema.sql"); // Use an absolute path
    const schema = fs.readFileSync(schemaPath, "utf-8");

    const isTableExistsQuery = `SELECT * FROM sys.tables WHERE name='gross_profit';`;

    const isTableExists = await sql_request.query(isTableExistsQuery);

    if (isTableExists.recordset.length == 0) {
      await sql_request.query(schema);
      console.log("HVAC Schema created successfully");
    } else {
      console.log("HVAC Schema already exists");
    }

    create_hvac_schema_status = true;
  } catch (error) {
    console.error("Error while creating the hvac schema: ", error);
    create_hvac_schema_status = false;
  }

  return create_hvac_schema_status;
}

module.exports = create_hvac_schema;
