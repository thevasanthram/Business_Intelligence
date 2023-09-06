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
  "https://api.servicetitan.io/accounting/v2/tenant/1011756844/export/invoices";

const data_pool = [];
let flattenedSampleObj = {};
const csv_file_name = path.join(__dirname, "./bulk_insert.csv");
const tableName = "bulk_insert_check"; // Replace with your table name

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
    matchingValues[key] = obj2[key] ? obj2[key] : "null";
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

      // if (continueFromList.includes(api_data["continueFrom"])) {
      //   console.log("EXCEPTIONAL CASE: continueFrom value repeated");
      //   break;
      // } else {
      //   console.log("Should Iterate ?", api_data["hasMore"]);
      // }

      continueFromList.push(api_data["continueFrom"]);

      count = count + 1;
    } while (shouldIterate);

    console.log("data fetched");

    // Generate SQL statements to create the table
    const sampleObj = data_pool[0]; // Take a sample object to infer the table structure
    flattenedSampleObj = flattenObject(sampleObj);
  } catch (error) {
    console.error("Error while processing data or storing into db:", error);
  }
}

// Function to process and write data in batches
async function generate_csv(data_pool) {
  // Process and write data in batches
  const batchSize = 1000; // Set the batch size as needed
  let index = 0;
  let csvContent = "";

  // Function to write the next batch of data
  function writeNextBatch() {
    const batch = data_pool.slice(index, index + batchSize);

    console.log("data_pool: ", data_pool[data_pool.length - 1]);

    if (batch.length > 0) {
      for (const currentObj of batch) {
        const flattenedObj = flattenObject(currentObj);
        const filteredObj = extractMatchingValues(
          flattenedSampleObj,
          flattenedObj
        );

        // Convert the array of arrays to CSV format
        const csvData = Object.values(filteredObj).join(",");

        // Join the CSV rows with newline characters
        csvContent = csvContent + csvData + "\n";
      }

      // End the batch
      index += batchSize;

      // Process the next batch in the next event loop iteration
      setImmediate(writeNextBatch);
    } else {
      // No more data to process, end the streams
      fs.writeFileSync(`${csv_file_name}`, csvContent);
      console.log("Data appended to CSV file.");
    }
  }

  // Start processing by writing the first batch
  writeNextBatch();
}

async function insert_data_into_db(data_pool) {
  await sql.connect(config); // Connect to the database
  const request = new sql.Request();

  const isTableExistsQuery = `SELECT * FROM sys.tables WHERE name = '${tableName}'`;
  const isTableExists = await sql.query(isTableExistsQuery);

  if (isTableExists.recordset.length == 0) {
    const createTableSQL = `CREATE TABLE ${tableName} (${Object.keys(
      flattenedSampleObj
    ).map((key) => `${key} NVARCHAR(255)`)} );`;

    // Execute a query to create the table
    const createTable = await request.query(createTableSQL);
    console.log("Table created");
  }

  // Generate SQL statements to insert data
  console.log("-----Initiated storing data into db based on batch-wise------");

  const bulk_insert_query = `BULK INSERT ${tableName}
  FROM '${csv_file_name.replace(/\\/g, "/")}'
  WITH (
      FIRSTROW = 1,               -- Skip the header row if present
      FIELDTERMINATOR = ',',      -- Field delimiter (typically a comma)
      ROWTERMINATOR = '\n',       -- Row delimiter (newline)
      TABLOCK                     -- Minimal logging for faster inserts (optional)
  );
  `;
  const bulk_insert_response = await request.query(bulk_insert_query);
  console.log("Bulk insert successfull", bulk_insert_response);

  // const createTable = await request.query(createTableSQL);
  await sql.close(); // closing database connection
}

async function manager() {
  await getAccessToken();
  await getAPIData();
  await generate_csv(data_pool);
  // await insert_data_into_db(data_pool);
  console.log("done");
}

// Initial execution
manager();

// Refresh access token every 13 minutes
setInterval(() => {
  getAccessToken();
}, 780000);
