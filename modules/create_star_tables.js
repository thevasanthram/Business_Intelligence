async function create_tables(data_pool, flattenedObj) {
  const tableName = "invoice_api_data"; // Replace with your table name

  await sql.connect(config); // Connect to the database
  const request = new sql.Request();
  console.log("Connected to Azure SQL Database");

  const isTableExistsQuery =
    "SELECT * FROM sys.tables WHERE name = 'invoice_details'";

  const isTableExists = await sql.query(isTableExistsQuery);

  if (isTableExists.recordset.length == 0) {
    // const createTableSQL = `CREATE TABLE ${tableName} (${Object.keys(
    //   flattenedSampleObj
    // ).map((key) => `${key} NVARCHAR(255)`)} );`;

    const customer_details_table_query = `CREATE TABLE customer_details (
        customer_id INT PRIMARY KEY,
        customer_name VARCHAR(255),
        customer_address_state VARCHAR(255),
        customer_address_unit VARCHAR(255),
        customer_address_city VARCHAR(255),
        customer_address_zip VARCHAR(255),
        customer_address_country VARCHAR(255)
      );`;

    const business_unit_details_table_query = `CREATE TABLE business_unit_details (
        business_unit_id INT PRIMARY KEY,
        business_unit_name VARCHAR(255)
      );`;

    const location_details_table_query = `CREATE TABLE location_details (
        location_id INT PRIMARY KEY,
        location_name VARCHAR(255),
        location_address_street VARCHAR(255),
        location_address_unit VARCHAR(255),
        location_address_city VARCHAR(255),
        location_address_state VARCHAR(255),
        location_address_zip VARCHAR(255),
        location_address_country VARCHAR(255)
      );`;

    const job_details_table_query = `CREATE TABLE job_details (
        job_id INT PRIMARY KEY,
        job_number VARCHAR(255),
        job_type VARCHAR(255),
      )`;

    const project_details_table_query = `CREATE TABLE project_details (
        project_id INT PRIMARY KEY,
        royalty_status VARCHAR(255),
        royalty_date VARCHAR(255),
        royalty_sent_on VARCHAR(255),
        royalty_memo VARCHAR(255),
        employee_info VARCHAR(255),
      )`;

    const invoice_details_table_query = `CREATE TABLE invoice_details (
        id INT PRIMARY KEY,
        syncStatus VARCHAR(50),
        referenceNumber VARCHAR(255),
        invoiceDate DATE,
        dueDate DATE,
        total DECIMAL(10, 2),
        balance DECIMAL(10, 2),
        invoiceType VARCHAR(50),
        created_on DATETIME,
        modified_on DATETIME,
        instance_name VARCHAR(255),
        customer_id INT,
        business_unit_id INT,
        location_id INT,
        job_id INT,
        FOREIGN KEY (customer_id) REFERENCES customer_details(customer_id),
        FOREIGN KEY (business_unit_id) REFERENCES business_unit_details(business_unit_id),
        FOREIGN KEY (location_id) REFERENCES location_details(location_id),
        FOREIGN KEY (job_id) REFERENCES job_details(job_id)
      );`;

    // project_id INT,
    // FOREIGN KEY (project_id) REFERENCES project_details(project_id)

    // Executing all queries to create tables
    // const createTable = await request.query(createTableSQL);
    const customer_details_table = await request.query(
      customer_details_table_query
    );

    const business_unit_details_table = await request.query(
      business_unit_details_table_query
    );

    const location_details_table = await request.query(
      location_details_table_query
    );

    const job_details_table = await request.query(job_details_table_query);

    // const project_details_table = await request.query(
    //   project_details_table_query
    // );

    const invoice_details_table = await request.query(
      invoice_details_table_query
    );

    console.log("Tables created");
  }
}

module.exports = create_tables;
