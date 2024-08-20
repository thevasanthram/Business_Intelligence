const fs = require("fs");
const path = require("path");
const extractMatchingValues = require("./extract_matching_values");
const { exit } = require("process");

async function add_suffix(id, instance_id) {
  let suffix = "";

  // Append suffix based on instance_id
  if (instance_id == 1) {
    suffix = "_01";
  } else if (instance_id == 2) {
    suffix = "_02";
  } else if (instance_id == 3) {
    suffix = "_03";
  } else if (instance_id == 4) {
    suffix = "_04";
  } else if (instance_id == 5) {
    suffix = "_05";
  }

  // Append suffix to id
  let modified_id = id + suffix;

  // Return the modified id
  return modified_id;
}

async function get_custom_wip_report_data(
  access_token,
  app_key,
  instance_name,
  tenant_id,
  wip_report_id,
  as_of_date,
  data_pool,
  page_count,
  wip_table_name
) {
  let has_error_occured = false;

  const wip_header_data = {
    Instance_id: "",
    ProjectId: "",
    ProjectNumber: "",
    ProjectName: "",
    CustomerName: "",
    ProjectStatus: "",
    ProjectContractStartDate: "",
    ActualCompletionDate: "",
    ContractValue: "",
    ChangeOrderValue: "",
    Retainage: "",
    TDRetainage: "",
    CostAdjustment: "",
    TotalEstimatedCost: "",
    EstimatedMargin: "",
    EstimatedMarginPercentage: "",
    TotalCost: "",
    CostToComplete: "",
    PercentCompleteCost: "",
    EarnedRevenue: "",
    TotalRevenue: "",
    InvoiceTotalRevenue: "",
    BalanceToFinish: "",
    OverBilling: "",
    UnderBilling: "",
    OriginalEstimateTemplate: "",
    ActualLaborQty: "",
    LastInvoiceInvoicedOn: "",
    TDOriginalEstimatedMargin: "",
    TDOriginalEstimatedMarginPercentage: "",
    ProjectBusinessUnit: "",
    TDInvoiceAndActualLaborCostPlusBurdenCost: "",
    PermitNumber: "",
    UTC_update_date: "",
  };

  const instance_list = [
    "Expert Heating and Cooling Co LLC",
    "PARKER-ARNTZ PLUMBING AND HEATING, INC.",
    "Family Heating & Cooling Co LLC",
    "Swift Air Mechanical LLC",
    "Jetstream Mechanicals LLC",
  ];

  try {
    let shouldIterate = false;
    do {
      const api_endpoint = `https://api.servicetitan.io/reporting/v2/tenant/${tenant_id}/report-category/operations/reports/${wip_report_id}/data`;

      const api_url =
        api_endpoint + `?page=${page_count + 1}&includeTotal=true`;

      const parameters = [
        {
          name: "From",
          value: "1900-01-01",
        },
        {
          name: "To",
          value: as_of_date,
        },
        {
          name: "TransactionAsOfDateOptions",
          value: "E",
        },
        {
          name: "TransactionAsOfDate",
          value: as_of_date,
        },
        {
          name: "InvoiceCostStatus",
          value: "2",
        },
        {
          name: "TransactionDateType",
          value: "I",
        },
        {
          name: "InvoiceRevenueStatus",
          value: "2",
        },
        {
          name: "ReturnSyncStatus",
          value: "2",
        },
        {
          name: "ReturnStatus",
          value: "R",
        },
        {
          name: "VendorBillStatus",
          value: "2",
        },
        {
          name: "VendorCostType",
          value: "B",
        },
      ];

      if (wip_table_name == "wip_completed_projects") {
        parameters.push({
          name: "ProjectFilter",
          value: "5",
        });
      }

      const request = await fetch(api_url, {
        method: "POST",
        headers: {
          "Content-type": "application/json",
          Authorization: access_token, // Include "Bearer" before access token
          "ST-App-Key": app_key,
        },
        body: JSON.stringify({
          parameters: parameters,
        }),
      });

      const response = await request.json();

      if (response["status"] == 429) {
        // wait for specified seconds
        waiting_time = parseInt(response["title"].match(/\d+/)[0]) + 3;

        console.log(instance_name, response["title"]);

        await (async () => {
          await new Promise((resolve) =>
            setTimeout(resolve, waiting_time * 1000)
          );
          shouldIterate = true;
        })();

        continue;
      }

      const fields = response.fields;
      const data = response.data;

      shouldIterate = response["hasMore"];

      const pushing_item = [];

      // console.log("response: ", response);

      const updatedData = await Promise.all(
        data.map(async (record, index) => {
          // organizing data with their fields
          const formatted_record = fields.reduce((obj, field, index) => {
            obj[field.name] = record[index];
            return obj;
          }, {});

          const instance_id = String(instance_list.indexOf(instance_name) + 1);

          if (formatted_record["ProjectId"]) {
            formatted_record["ProjectId"] = await add_suffix(
              formatted_record["ProjectId"],
              instance_id
            );
          }

          if (formatted_record["ProjectNumber"]) {
            formatted_record["ProjectNumber"] = await add_suffix(
              formatted_record["ProjectNumber"],
              instance_id
            );
          }

          if (formatted_record["ProjectStartDate"]) {
            formatted_record["ProjectContractStartDate"] =
              formatted_record["ProjectStartDate"];

            delete formatted_record["ProjectStartDate"];
          }

          if (instance_id == 1 || instance_id == 4) {
            // EXP and SFT
            formatted_record["EstimatedMargin"] =
              formatted_record["ContractValue"] -
              formatted_record["TotalEstimatedCost"];
          }

          if (formatted_record["PermitNumber(s)(separatepermitsusingcommas)"]) {
            formatted_record["PermitNumber"] =
              formatted_record["PermitNumber(s)(separatepermitsusingcommas)"];
            delete formatted_record[
              "PermitNumber(s)(separatepermitsusingcommas)"
            ];
          }

          formatted_record["Instance_id"] = instance_id;
          formatted_record["UTC_update_date"] = as_of_date;

          const final_record = extractMatchingValues(
            wip_header_data,
            formatted_record
          );

          // console.log("final_record: ", final_record);
          return final_record;
        })
      );

      // fs.writeFile(
      //   `./${instance_name}_wip_report.js`,
      //   JSON.stringify(updatedData),
      //   () => console.log("done")
      // );

      data_pool = [...data_pool, ...updatedData];

      console.log(instance_name, data_pool.length, "/", response["totalCount"]);

      page_count = page_count + 1;
    } while (shouldIterate);
  } catch (error) {
    console.error(
      `WIP Report fetching failed for ${instance_name} - ${as_of_date}. Try Again!:`,
      error
    );

    has_error_occured = true;
  }

  return {
    data_pool,
    page_count,
    has_error_occured,
  };
}

module.exports = get_custom_wip_report_data;
