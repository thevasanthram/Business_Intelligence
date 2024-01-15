const fs = require("fs");
const path = require("path");

async function getAPIWholeData(
  access_token,
  app_key,
  instance_name,
  tenant_id,
  api_group,
  api_name,
  params_header,
  data_pool_object,
  data_pool,
  page_count,
  testing
) {
  // let data_pool_object = {};
  // let data_pool = [];

  let has_error_occured = false;

  try {
    // automatic api fetch data code
    let shouldIterate = false;

    const instance_list = [
      "Expert Heating and Cooling Co LLC",
      "PARKER-ARNTZ PLUMBING AND HEATING, INC.",
      "Family Heating & Cooling Co LLC",
    ];

    const formatted_api_group = api_group.replace(/-/g, "_").replace("/", "_");
    const formatted_api_name = api_name.replace(/-/g, "_").replace("/", "_");

    const api_url = `https://api.servicetitan.io/${api_group}/v2/tenant/${tenant_id}/${api_name}`;

    // console.log("Getting API data, wait untill a response appears");

    const params_condition =
      "?" +
      Object.keys(params_header)
        .map((param_name) => {
          if (params_header[param_name] && params_header[param_name] !== "") {
            if (
              (param_name === "modifiedOnOrAfter" ||
                param_name === "modifiedBefore") &&
              api_name === "gross-pay-items"
            ) {
              // Handle special cases for modifiedOnOrAfter and modifiedBefore
              if (param_name === "modifiedOnOrAfter") {
                return `dateOnOrAfter=${params_header[param_name]}`;
              } else {
                return `dateOnOrBefore=${params_header[param_name]}`;
              }
            } else {
              // Handle other cases
              return `${param_name}=${params_header[param_name]}`;
            }
          }
          // Return undefined for cases where the condition is not met
          return undefined;
        })
        .filter((param) => param !== undefined) // Remove undefined values
        .join("&");

    do {
      const filtering_condition = `${params_condition}&page=${page_count + 1}`;

      console.log("request url: ", api_url + filtering_condition);

      const api_response = await fetch(api_url + filtering_condition, {
        method: "GET",
        headers: {
          // 'Content-type': 'application/x-www-form-urlencoded',
          Authorization: access_token, // Include "Bearer" before access token
          "ST-App-Key": app_key,
        },
      });

      const api_data = await api_response.json();

      shouldIterate = api_data["hasMore"];

      try {
        const pusing_item = api_data["data"];

        // pushing api_data_objects into data
        if (pusing_item.length > 0) {
          pusing_item.map((record) => {
            record["instance_id"] = instance_list.indexOf(instance_name) + 1;
            if (api_name != "calls") {
              data_pool_object[record["id"]] = record;
            } else {
              data_pool_object[record["leadCall"]["id"]] = record;
            }
          });

          data_pool.push(...pusing_item);

          console.log(
            data_pool.length,
            "/",
            api_data["totalCount"],
            " records  fetched  successfully"
          );
        }
      } catch {
        // if theres a exceptional response in some api

        // Create the folder if it doesn't exist
        const folderPath = "./json_responses";
        if (!fs.existsSync(folderPath)) {
          fs.mkdirSync(folderPath, { recursive: true });
        }

        // Create the file path
        const filePath = path.join(
          folderPath,
          formatted_api_group + "_" + formatted_api_name + ".txt"
        );

        fs.writeFile(
          filePath,
          JSON.stringify(api_data),
          { flag: "w" },
          (err) => {
            if (err) {
              console.error("Error writing to file:", err);
            } else {
              console.log("Data has been written to", filePath);
            }
          }
        );

        break;
      }

      page_count = page_count + 1;
    } while (shouldIterate);

    // console.log("Data fetching completed successfully");
  } catch (error) {
    console.error(
      `Data fetching failed for ${api_group} - ${api_name}. Try Again!:`,
      error
    );

    // data_pool_object = getAPIWholeData(
    //   access_token,
    //   app_key,
    //   instance_name,
    //   tenant_id,
    //   api_group,
    //   api_name,
    //   params_header
    // );

    has_error_occured = true;
  }

  // if (api_name == "gross-pay-items") {
  //   return data_pool;
  // } else {
  //   return data_pool_object;
  // }

  if ((testing = "testing")) {
    console.log("data_pool_object: ", data_pool_object);
    console.log("data_pool: ", data_pool);
  }

  return { data_pool_object, data_pool, page_count, has_error_occured };
}

module.exports = getAPIWholeData;
