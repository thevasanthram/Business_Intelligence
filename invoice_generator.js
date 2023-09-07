const fs = require("fs");
const stream = fs.createWriteStream("./invoice.js", { flags: "a" }); // Use { flags: 'a' } to enable append mode

const api_url =
  "https://api.servicetitan.io/accounting/v2/tenant/1011756844/export/invoices";
let modified_url = api_url;
let access_token = "";
const auth_url = "https://auth.servicetitan.io/connect/token";

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

async function getAPIData() {
  try {
    // Now that you have the access_token, you can proceed with other API calls and processing.
    let count = 0;

    let shouldIterate = false;
    const continueFromList = [];

    const data_pool = [];
    do {
      console.log(count + 1, "requesting to: ", modified_url);

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

      //   stream.write(JSON.stringify(...api_data["data"]) + ",\n");

      // console.log("data_pool: ", data_pool.length);

      if (continueFromList.includes(api_data["continueFrom"])) {
        console.log("Continue from value repeated");
        break;
      } else {
        console.log("Should Iterate ?", api_data["hasMore"]);
      }

      continueFromList.push(api_data["continueFrom"]);

      console.log("======================================");

      count = count + 1;
    } while (shouldIterate);

    console.log("======================================");
    console.log(" END ");
    console.log("======================================");

    // Define the batch size (adjust this value as needed)
    const batchSize = 1000; // You can change this to a suitable batch size

    // Write data in batches to avoid memory issues
    for (let i = 0; i < data_pool.length; i += batchSize) {
      const batch = data_pool.slice(i, i + batchSize);
      console.log(batch);
      // Write the current batch as a JSON string to the file
      stream.write(
        JSON.stringify(batch).slice(1, JSON.stringify(batch).length - 1) + ",\n"
      );
      console.log("batch", i + 1, "completed");
    }

    stream.end(() => {
      console.log("Data has been written to the file successfully.");
    });
  } catch (error) {
    console.error("Error while getting access token:", error);
  }
}

async function manager() {
  await getAccessToken();
  await getAPIData();
}

// Initial execution
manager();
