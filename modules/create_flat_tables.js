async function create_flat_tables(
  sql_client,
  flattenedObj,
  api_group,
  api_name
) {
  const formatted_api_group = api_group.replace(/-/g, "_");
  const formatted_api_name = api_name.replace(/-/g, "_");

  const isTableExistsQuery = `SELECT * FROM sys.tables WHERE name = '${
    formatted_api_group + "_" + formatted_api_name + "_flat_table"
  }'`;

  const isTableExists = await sql_client.query(isTableExistsQuery);

  if (isTableExists.recordset.length == 0) {
    try {
      const createTableSQL = `CREATE TABLE ${
        formatted_api_group + "_" + formatted_api_name + "_flat_table"
      } (${Object.keys(flattenedObj).map(
        (key) => `${key} NVARCHAR(255)`
      )} ) WITH
      (
          DATA_COMPRESSION = PAGE
      );`;

      // console.log("createTableSQL: ", createTableSQL);

      const createTable = await sql_client.query(createTableSQL);

      console.log(
        formatted_api_group +
          "_" +
          formatted_api_name +
          "_flat_table " +
          "created"
      );
    } catch (error) {
      console.log("Flat table creation failed. Try Again!", error);
    }
  } else {
    console.log(
      formatted_api_group +
        "_" +
        formatted_api_name +
        "_flat_table " +
        "already exists"
    );
  }
}

module.exports = create_flat_tables;
