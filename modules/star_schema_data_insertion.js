async function star_schema_data_insertion(data_pool, flattenedSampleObj) {
  await sql.connect(config); // Connect to the database
  const request = new sql.Request();

  // Generate SQL statements to insert data
  console.log("-----Initiated storing data into db based on batch-wise------");
  const batchSize = 300;

  for (let i = 0; i < data_pool.length; i = i + batchSize) {
    // batchwise inserting data, each batch contains 300 insertions

    await Promise.all(
      data_pool.slice(i, i + batchSize).map(async (currentObj, index) => {
        const flattenedObj = flattenObject(currentObj);
        const filteredObj = extractMatchingValues(
          flattenedSampleObj,
          flattenedObj
        );

        // insertion into customer details table
        const table_column_data = {
          customer_details: {
            table: [
              "customer_id",
              "customer_name",
              "customer_address_state",
              "customer_address_unit",
              "customer_address_city",
              "customer_address_zip",
              "customer_address_country",
            ],
            api: [
              "customerid",
              "customername",
              "customerAddressstate",
              "customerAddressunit",
              "customerAddresscity",
              "customerAddresszip",
              "customerAddresscountry",
            ],
          },
          business_unit_details: {
            table: ["business_unit_id", "business_unit_name"],
            api: ["businessUnitid", "businessUnitname"],
          },
          location_details: {
            table: [
              "location_id",
              "location_name",
              "location_address_street",
              "location_address_unit",
              "location_address_city",
              "location_address_state",
              "location_address_zip",
              "location_address_country",
            ],
            api: [
              "locationid",
              "locationname",
              "locationAddressstreet",
              "locationAddressunit",
              "locationAddresscity",
              "locationAddressstate",
              "locationAddresszip",
              "locationAddresscountry",
            ],
          },
          job_details: {
            table: ["job_id", "job_number", "job_type"],
            api: ["jobid", "jobnumber", "jobtype"],
          },
          project_details: {
            table: [
              "project_id",
              "royalty_status",
              "royalty_date",
              "royalty_sent_on",
              "royalty_memo",
              "employee_info",
            ],
            api: [
              "projectId",
              "royaltystatus",
              "royaltydate",
              "royaltysentOn",
              "royaltymemo",
              "employeeInfo",
            ],
          },
          invoice_details: {
            table: [
              "id",
              "syncStatus",
              "referenceNumber",
              "invoiceDate",
              "dueDate",
              "total",
              "balance",
              "invoiceType",
              "created_on",
              "modified_on",
              "instance_name",
              "customer_id",
              "business_unit_id",
              "location_id",
              "job_id",
            ],
            api: [
              "id",
              "syncStatus",
              "referenceNumber",
              "invoiceDate",
              "dueDate",
              "total",
              "balance",
              "invoiceType",
              "createdOn",
              "modifiedOn",
              "instance_name",
              "customerid",
              "businessUnitid",
              "locationid",
              "jobid",
            ],
          },
        };

        // customer details table insert

        const insert_customer_details_query = `IF NOT EXISTS (SELECT 1 FROM customer_details WHERE customer_id = ${
          filteredObj["customerid"]
        })
          BEGIN 
            INSERT INTO customer_details (${table_column_data[
              "customer_details"
            ]["table"].join(", ")}) VALUES (${table_column_data[
          "customer_details"
        ]["api"]
          .map((value) => {
            return String(filteredObj[value]).includes(`'`)
              ? `'${filteredObj[value].replace(/'/g, `''`)}'`
              : `'${filteredObj[value]}'`;
          })
          .join(", ")});
          END;`;

        // businessUnit details table insert
        const insert_busines_unit_details_query = `INSERT INTO business_unit_details (${table_column_data[
          "business_unit_details"
        ]["table"].join(", ")}) VALUES (${table_column_data[
          "business_unit_details"
        ]["api"]
          .map((value) => {
            return String(filteredObj[value]).includes(`'`)
              ? `'${filteredObj[value].replace(/'/g, `''`)}'`
              : `'${filteredObj[value]}'`;
          })
          .join(", ")});`;

        // location details table insert
        const insert_location_details_query = `INSERT INTO location_details (${table_column_data[
          "location_details"
        ]["table"].join(", ")}) VALUES (${table_column_data["location_details"][
          "api"
        ]
          .map((value) => {
            return String(filteredObj[value]).includes(`'`)
              ? `'${filteredObj[value].replace(/'/g, `''`)}'`
              : `'${filteredObj[value]}'`;
          })
          .join(", ")});`;

        // job details table insert
        const insert_job_details_query = `INSERT INTO job_details (${table_column_data[
          "job_details"
        ]["table"].join(", ")}) VALUES (${table_column_data["job_details"][
          "api"
        ]
          .map((value) => {
            return String(filteredObj[value]).includes(`'`)
              ? `'${filteredObj[value].replace(/'/g, `''`)}'`
              : `'${filteredObj[value]}'`;
          })
          .join(", ")});`;

        // project details table insert
        // const insert_project_details_query = `INSERT INTO project_details (${table_column_data[
        //   "project_details"
        // ]["table"].join(", ")}) VALUES (${table_column_data["project_details"][
        //   "api"
        // ]
        //   .map((value) => {
        //     return String(filteredObj[value]).includes(`'`)
        //       ? `'${filteredObj[value].replace(/'/g, `''`)}'`
        //       : `'${filteredObj[value]}'`;
        //   })
        //   .join(", ")});`;

        // invoice details table insert
        const insert_invoice_details_query = `INSERT INTO invoice_details (${table_column_data[
          "invoice_details"
        ]["table"].join(", ")}) VALUES (${table_column_data["invoice_details"][
          "api"
        ]
          .map((value) => {
            if (value != "instance_name") {
              return String(filteredObj[value]).includes(`'`)
                ? `'${filteredObj[value].replace(/'/g, `''`)}'`
                : `'${filteredObj[value]}'`;
            } else {
              return `'Family Heating & Cooling Co LLC'`;
            }
          })
          .join(", ")});`;

        console.log(
          "insert_invoice_details_query: ",
          insert_invoice_details_query
        );

        try {
          const insert_customer_details = await request.query(
            insert_customer_details_query
          );

          const insert_busines_unit_details = await request.query(
            insert_busines_unit_details_query
          );

          const insert_location_details = await request.query(
            insert_location_details_query
          );

          const insert_job_details = await request.query(
            insert_job_details_query
          );

          // const insert_project_details = await request.query(
          //   insert_project_details_query
          // );

          const insert_invoice_details = await request.query(
            insert_invoice_details_query
          );
        } catch (error) {
          // console.log("error due to duplicate entry: ", error);
        }
      })
    );

    console.log(i, "Records", "completed");
  }

  // const createTable = await request.query(createTableSQL);
  await sql.close(); // closing database connection
}

module.exports = star_schema_data_insertion;
