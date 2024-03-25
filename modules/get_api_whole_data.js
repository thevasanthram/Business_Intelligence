const fs = require("fs");
const path = require("path");

async function add_suffix(id, instance_id) {
  let suffix = "";

  // Append suffix based on instance_id
  if (instance_id == 1) {
    suffix = "01";
  } else if (instance_id == 2) {
    suffix = "02";
  } else if (instance_id == 3) {
    suffix = "03";
  }

  // Append suffix to id
  let modified_id = id + suffix;

  // Return the modified id
  return modified_id;
}

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
  continueFrom
) {
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
      let filtering_condition = "";

      if (api_name != "export/inventory-bills") {
        filtering_condition = `${params_condition}&page=${page_count + 1}`;
      } else {
        // fetching inventory-bills records based on continueFrom property in response
        filtering_condition = `${params_condition}&from=${continueFrom}`;
      }

      if (!params_header["payrollIds"]) {
        console.log("request url: ", api_url + filtering_condition);
      }

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
        let pushing_item = api_data["data"];

        if (pushing_item.length > 0) {
          // pushing api_data_objects into data

          await Promise.all(
            pushing_item.map(async (record) => {
              record["instance_id"] = instance_list.indexOf(instance_name) + 1;

              if (record["id"]) {
                record["id"] = await add_suffix(
                  record["id"],
                  record["instance_id"]
                );
              }

              if (record["businessUnitId"]) {
                record["businessUnitId"] = await add_suffix(
                  record["businessUnitId"],
                  record["instance_id"]
                );
              }

              if (record["campaignId"]) {
                record["campaignId"] = await add_suffix(
                  record["campaignId"],
                  record["instance_id"]
                );
              }

              if (record["jobId"]) {
                record["jobId"] = await add_suffix(
                  record["jobId"],
                  record["instance_id"]
                );
              }

              if (record["taxZoneId"]) {
                record["taxZoneId"] = await add_suffix(
                  record["taxZoneId"],
                  record["instance_id"]
                );
              }

              if (record["zoneId"]) {
                record["zoneId"] = await add_suffix(
                  record["zoneId"],
                  record["instance_id"]
                );
              }

              if (record["payrollId"]) {
                record["payrollId"] = await add_suffix(
                  record["payrollId"],
                  record["instance_id"]
                );
              }

              if (record["projectId"]) {
                record["projectId"] = await add_suffix(
                  record["projectId"],
                  record["instance_id"]
                );
              }

              if (record["invoiceId"]) {
                record["invoiceId"] = await add_suffix(
                  record["invoiceId"],
                  record["instance_id"]
                );
              }

              if (record["purchaseOrderId"]) {
                record["purchaseOrderId"] = await add_suffix(
                  record["purchaseOrderId"],
                  record["instance_id"]
                );
              }

              if (record["vendorId"]) {
                record["vendorId"] = await add_suffix(
                  record["vendorId"],
                  record["instance_id"]
                );
              }

              if (record["soldBy"]) {
                record["soldBy"] = await add_suffix(
                  record["soldBy"],
                  record["instance_id"]
                );
              }

              if (record["locationId"]) {
                record["locationId"] = await add_suffix(
                  record["locationId"],
                  record["instance_id"]
                );
              }

              if (record["customerId"]) {
                record["customerId"] = await add_suffix(
                  record["customerId"],
                  record["instance_id"]
                );
              }

              if (record["leadCallId"]) {
                record["leadCallId"] = await add_suffix(
                  record["leadCallId"],
                  record["instance_id"]
                );
              }

              if (record["bookingId"]) {
                record["bookingId"] = await add_suffix(
                  record["bookingId"],
                  record["instance_id"]
                );
              }

              if (record["jobTypeId"]) {
                record["jobTypeId"] = await add_suffix(
                  record["jobTypeId"],
                  record["instance_id"]
                );
              }

              if (record["createdById"]) {
                record["createdById"] = await add_suffix(
                  record["createdById"],
                  record["instance_id"]
                );
              }

              if (record["soldById"]) {
                record["soldById"] = await add_suffix(
                  record["soldById"],
                  record["instance_id"]
                );
              }

              if (record["technicianId"]) {
                record["technicianId"] = await add_suffix(
                  record["technicianId"],
                  record["instance_id"]
                );
              }

              if (record["appointmentId"]) {
                record["appointmentId"] = await add_suffix(
                  record["appointmentId"],
                  record["instance_id"]
                );
              }

              if (record["assignedById"]) {
                record["assignedById"] = await add_suffix(
                  record["assignedById"],
                  record["instance_id"]
                );
              }

              if (record["employeeId"]) {
                record["employeeId"] = await add_suffix(
                  record["employeeId"],
                  record["instance_id"]
                );
              }

              if (record["type"]) {
                if (record["type"]["id"]) {
                  record["type"]["id"] = await add_suffix(
                    record["type"]["id"],
                    record["instance_id"]
                  );
                }
              }

              if (record["primaryVendor"]) {
                if (record["primaryVendor"]["vendorId"]) {
                  record["primaryVendor"]["vendorId"] = await add_suffix(
                    record["primaryVendor"]["vendorId"],
                    record["instance_id"]
                  );
                }
              }

              if (record["category"]) {
                if (record["category"]["id"]) {
                  record["category"]["id"] = await add_suffix(
                    record["category"]["id"],
                    record["instance_id"]
                  );
                }
              }

              if (record["businessUnit"]) {
                if (record["businessUnit"]["id"]) {
                  record["businessUnit"]["id"] = await add_suffix(
                    record["businessUnit"]["id"],
                    record["instance_id"]
                  );
                }
              }

              if (record["vendor"]) {
                if (record["vendor"]["id"]) {
                  record["vendor"]["id"] = await add_suffix(
                    record["vendor"]["id"],
                    record["instance_id"]
                  );
                }
              }

              if (record["job"]) {
                if (record["job"]["id"]) {
                  record["job"]["id"] = await add_suffix(
                    record["job"]["id"],
                    record["instance_id"]
                  );
                }
              }

              if (record["location"]) {
                if (record["location"]["id"]) {
                  record["location"]["id"] = await add_suffix(
                    record["location"]["id"],
                    record["instance_id"]
                  );
                }
              }

              if (record["customer"]) {
                if (record["customer"]["id"]) {
                  record["customer"]["id"] = await add_suffix(
                    record["customer"]["id"],
                    record["instance_id"]
                  );
                }
              }

              if (record["invoiceType"]) {
                if (record["invoiceType"]["id"]) {
                  record["invoiceType"]["id"] = await add_suffix(
                    record["invoiceType"]["id"],
                    record["instance_id"]
                  );
                }
              }

              if (record["items"]) {
                const modifiedItems = await Promise.all(
                  record["items"].map(async (items_record) => {
                    if (items_record["skuId"]) {
                      return {
                        ...items_record,
                        skuId: await add_suffix(
                          items_record["skuId"],
                          record["instance_id"]
                        ),
                      };
                    } else {
                      return items_record;
                    }
                  })
                );

                // Replace the original items array with the modified items array
                record["items"] = modifiedItems;
              }

              if (record["projectManagerIds"]) {
                await Promise.all(
                  record["projectManagerIds"].map(async (manager_id, index) => {
                    if (manager_id) {
                      record["projectManagerIds"][index] = await add_suffix(
                        manager_id,
                        record["instance_id"]
                      );
                    }
                  })
                );
              }

              if (api_name != "calls") {
                data_pool_object[record["id"]] = record;
              } else {
                if (record["leadCall"]) {
                  if (record["leadCall"]["id"]) {
                    record["leadCall"]["id"] = await add_suffix(
                      record["leadCall"]["id"],
                      record["instance_id"]
                    );
                  }

                  if (record["leadCall"]["customer"]) {
                    if (record["leadCall"]["customer"]["id"]) {
                      record["leadCall"]["customer"]["id"] = await add_suffix(
                        record["leadCall"]["customer"]["id"],
                        record["instance_id"]
                      );
                    }
                  }

                  if (record["leadCall"]["campaign"]) {
                    if (record["leadCall"]["campaign"]["id"]) {
                      record["leadCall"]["campaign"]["id"] = await add_suffix(
                        record["leadCall"]["campaign"]["id"],
                        record["instance_id"]
                      );
                    }
                  }

                  if (record["leadCall"]["agent"]) {
                    if (record["leadCall"]["agent"]["id"]) {
                      record["leadCall"]["agent"]["id"] = await add_suffix(
                        record["leadCall"]["agent"]["id"],
                        record["instance_id"]
                      );
                    }
                  }

                  if (record["leadCall"]["agent"]) {
                    if (record["leadCall"]["agent"]["externalId"]) {
                      record["leadCall"]["agent"]["externalId"] =
                        await add_suffix(
                          record["leadCall"]["agent"]["externalId"],
                          record["instance_id"]
                        );
                    }
                  }
                }

                data_pool_object[record["leadCall"]["id"]] = record;
              }
            })
          );

          data_pool.push(...pushing_item);

          if (!params_header["payrollIds"]) {
            console.log(
              data_pool.length,
              "/",
              api_data["totalCount"],
              " records  fetched  successfully"
            );
          }
        }
      } catch (inside_err) {
        // if theres a exceptional response in some api

        // Create the folder if it doesn't exist
        console.log("inside_err: ", inside_err);
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

      if (api_name != "export/inventory-bills") {
        page_count = page_count + 1;
      } else {
        continueFrom = api_data["continueFrom"] ? api_data["continueFrom"] : "";
      }
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

  return {
    data_pool_object,
    data_pool,
    page_count,
    continueFrom,
    has_error_occured,
  };
}

module.exports = getAPIWholeData;
