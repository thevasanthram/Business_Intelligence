async function create_flat_tables(sql_request, flattenedObj, table_name) {
  const fomatted_table_name =
    table_name.replace(/-/g, "_").replace(/\//g, "_") + "_table";

  const isTableExistsQuery = `SELECT * FROM sys.tables WHERE name = '${fomatted_table_name}'`;

  const isTableExists = await sql_request.query(isTableExistsQuery);

  if (isTableExists.recordset.length == 0) {
    let query = "";
    try {
      const createTableSQL = `CREATE TABLE ${fomatted_table_name} (${Object.keys(
        flattenedObj
      ).map((key) => {
        if (key != "batchId") {
          return `[${key}] NVARCHAR(max)`;
        } else {
          return `[batch_Id] NVARCHAR(max)`;
        }
      })} ) WITH
      (
          DATA_COMPRESSION = PAGE
      );`;

      console.log("createTableSQL: ", createTableSQL);

      query = createTableSQL;
      const createTable = await sql_request.query(createTableSQL);

      // console.log(fomatted_table_name + " created");
      console.log(fomatted_table_name, "created ");
    } catch (error) {
      console.log(fomatted_table_name, "creation failed. Trying Again!", error);
      create_flat_tables(sql_request, flattenedObj, table_name);
    }
  } else {
    console.log(fomatted_table_name + " already exists");
  }
}

module.exports = create_flat_tables;
