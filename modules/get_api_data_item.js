const flattenObject = require("./new_flatten_object");
const fs = require("fs");
const path = require("path");

async function getAPIDataItem(
  access_token,
  app_key,
  instance_name,
  tenant_id,
  api_group,
  api_name,
  params_header
) {
  try {
    // automatic api fetch data code
    let count = 0;
    let shouldIterate = false;

    const api_url = `https://api.servicetitan.io/${api_group}/v2/tenant/${tenant_id}/${api_name}`;

    // console.log("Getting API data, wait untill a response appears");

    let data_pool = [];
    const items_pool = [];

    const params_condition =
      "?" +
      Object.keys(params_header)
        .map((param_name) => {
          if (params_header[param_name] && params_header[param_name] !== "") {
            return `${param_name}=${params_header[param_name]}`;
          }
        })
        .filter((param) => param !== undefined) // Remove undefined values
        .join("&");

    do {
      const filtering_condition = `${params_condition}&page=${count + 1}`;

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

      // console.log("api_data: ", api_data);

      // code for api's having different format of response
      if (!api_data["data"]) {
        try {
          const pushing_item = api_data.map((record, index) => {
            if (record["end"]) {
              const temp = record["end"];
              record["_end"] = temp;
              delete record["end"];
            }

            // delete record["items"];
            const temp = {
              instance_name: instance_name,
              ...record,
            };

            return temp;
          });

          if (pushing_item.length > 0) {
            data_pool.push(...pushing_item);

            console.log(
              data_pool.length,
              "/",
              data_pool.length,
              "records fetched successfully"
            );
          }
        } catch (err) {
          console.log("exceptional response from the api");
          // if theres a exceptional response in some api
          const formatted_api_group = api_group
            .replace(/-/g, "_")
            .replace("/", "_");
          const formatted_api_name = api_name
            .replace(/-/g, "_")
            .replace("/", "_");

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
        }

        break;
      }

      shouldIterate = api_data["hasMore"];

      api_data["data"];

      try {
        const pushing_item = api_data["data"].map((record, index) => {
          if (record["end"]) {
            const temp = record["end"];
            record["_end"] = temp;
            delete record["end"];
          }

          if (record["items"]) {
            record["items"].map((item) => {
              const temp_obj = {
                parent_id: record["id"],
                ...item,
              };
              items_pool.push(temp_obj);
            });
          }

          // delete record["items"];

          const temp = {
            instance_name: instance_name,
            ...record,
          };

          return temp;
        });

        // pushing api_data_objects into data
        if (pushing_item.length > 0) {
          data_pool.push(...pushing_item);

          console.log(
            data_pool.length,
            "/",
            api_data["totalCount"],
            "records fetched successfully"
          );
        }
      } catch (err) {
        console.log("exceptional response from the api", err);
        // if theres a exceptional response in some api
        const formatted_api_group = api_group
          .replace(/-/g, "_")
          .replace("/", "_");
        const formatted_api_name = api_name
          .replace(/-/g, "_")
          .replace("/", "_");

        break;
      }

      count = count + 1;
    } while (shouldIterate);

    // console.log("Data fetching completed successfully");

    data_pool = items_pool;

    return data_pool;
  } catch (error) {
    console.error(
      `Data fetching failed for ${api_group} - ${api_name}. Try Again!:`,
      error
    );
  }
}

module.exports = getAPIDataItem;
