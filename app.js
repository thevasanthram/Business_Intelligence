const sql = require("mssql");
const fs = require("fs");
const fastcsv = require("fast-csv");
const path = require("path");

// database configuration
const config = {
  user: "deevia",
  password: "kiran@123",
  server: "deevia-trial.database.windows.net",
  database: "crop_db",
  options: {
    encrypt: true, // Use this option for SSL encryption
  },
};

let access_token = "";
const auth_url = "https://auth.servicetitan.io/connect/token";
const api_url =
  "https://api.servicetitan.io/accounting/v2/tenant/1011756844/invoices";

const data_pool = [];
let flattenedSampleObj = {};
const tableName = "invoice_api_data"; // Replace with your table name

async function getAccessToken() {
  try {
    const auth_response = await fetch(auth_url, {
      method: "POST",
      headers: {
        "Content-type": "application/x-www-form-urlencoded",
      },
      body: new URLSearchParams({
        grant_type: "client_credentials",
        client_id: "cid.jk53hfwwcq6a1zgtbh96byil4",
        client_secret: "cs1.2hdc1yd19hpxzmdeg5rfuc6i3smpxy9iei0yhq1p7qp8mwyjda",
      }),
    });

    const auth_data = await auth_response.json();
    access_token = auth_data.access_token;
    // console.log('Access Token: ', access_token);
  } catch (error) {
    console.error("Error while getting access token:", error);
  }
}

// Function to flatten a nested object without any separator
function flattenObject(obj, parentKey = "") {
  let result = {};
  for (const key in obj) {
    if (typeof obj[key] === "object" && obj[key] !== null) {
      const flattened = flattenObject(obj[key], parentKey + key);
      result = { ...result, ...flattened };
    } else {
      result[parentKey + key] = obj[key];
    }
  }
  return result;
}

function extractMatchingValues(obj1, obj2) {
  const matchingValues = {};

  for (const key in obj1) {
    matchingValues[key] = obj2[key] ? obj2[key] : null;
  }

  return matchingValues;
}

async function getAPIData() {
  try {
    // automatic api fetch data code
    let count = 0;
    let modified_url = api_url;
    let shouldIterate = false;
    const continueFromList = [];

    console.log("data started fetching, wait untill a response appears");

    do {
      const api_response = await fetch(modified_url, {
        method: "GET",
        headers: {
          // 'Content-type': 'application/x-www-form-urlencoded',
          Authorization: access_token, // Include "Bearer" before access token
          "ST-App-Key": "ak1.ztsdww9rvuk0sjortd94dmxwx",
        },
      });

      const api_data = await api_response.json();

      // chaning the url with new continueFrom value
      modified_url =
        api_url + `?includeRecentChanges=true&from=${api_data["continueFrom"]}`;
      shouldIterate = api_data["hasMore"];
      // pushing api_data_objects into data
      data_pool.push(...api_data["data"]);

      // console.log("data_pool: ", data_pool.length);

      continueFromList.push(api_data["continueFrom"]);

      count = count + 1;
    } while (count < 1);

    console.log("data fetched");

    // Iterate all the elements in data_pool and fetch the object having maximum property
    let sampleObj = flattenObject(data_pool[0]); // Take a sample object to infer the table structure
    data_pool.map((response_data, index) => {
      const current_flattened_object = flattenObject(response_data);

      if (
        Object.keys(current_flattened_object).length >
        Object.keys(sampleObj).length
      ) {
        sampleObj = current_flattened_object;
      }
    });

    flattenedSampleObj = sampleObj;
  } catch (error) {
    console.error("Error while getting data from service titan:", error);
  }
}

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

async function insert_data_into_db(data_pool, flattenedSampleObj) {
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
          INSERT INTO customer_details (${table_column_data["customer_details"][
            "table"
          ].join(", ")}) VALUES (${table_column_data["customer_details"]["api"]
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

async function manager() {
  await getAccessToken();
  await getAPIData();
  await create_tables(data_pool, flattenedSampleObj);
  await insert_data_into_db(data_pool, flattenedSampleObj);
  console.log("done");
}

// Initial execution
manager();

// Refresh access token every 13 minutes
setInterval(() => {
  getAccessToken();
}, 780000);
