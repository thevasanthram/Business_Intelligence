const flattenObject = require("./../modules/new_flatten_object");

async function getAPIData(access_token, api_group, api_name, params_header) {
  try {
    // automatic api fetch data code
    let count = 0;
    let shouldIterate = false;

    const api_url = `https://api.servicetitan.io/${api_group}/v2/tenant/1011756844/${api_name}`;

    console.log("Getting API data, wait untill a response appears");

    const data_pool = [];

    const params_condition =
      "?" +
      Object.keys(params_header)
        .map((param_name) => {
          if (params_header[param_name] && params_header[param_name] !== "") {
            console.log("param_name: ", param_name);
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
          "ST-App-Key": "ak1.ztsdww9rvuk0sjortd94dmxwx",
        },
      });

      const api_data = await api_response.json();

      // code for api's having different format of response
      if (!api_data["data"]) {
        data_pool.push(...api_data);
        console.log(
          data_pool.length,
          "/",
          data_pool.length,
          "records fetched successfully"
        );
        break;
      }

      shouldIterate = api_data["hasMore"];

      // pushing api_data_objects into data
      data_pool.push(...api_data["data"]);

      // console.log("data_pool: ", data_pool.length);

      console.log(
        data_pool.length,
        "/",
        api_data["totalCount"],
        "records fetched successfully"
      );

      count = count + 1;
    } while (shouldIterate);

    console.log("Data fetching completed successfully");

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

    return { data_pool, flattenedSampleObj };
  } catch (error) {
    console.error("Data fetching failed. Try Again!:", error);
  }
}

module.exports = getAPIData;
