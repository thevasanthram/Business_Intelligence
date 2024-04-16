const fs = require("fs");
const path = require("path");

async function add_suffix(id, instance_id) {
  let suffix = "";

  // Append suffix based on instance_id
  if (instance_id == 1) {
    suffix = "_01";
  } else if (instance_id == 2) {
    suffix = "_02";
  } else if (instance_id == 3) {
    suffix = "_03";
  }

  // Append suffix to id
  let modified_id = id + suffix;

  // Return the modified id
  return modified_id;
}

async function get_wip_report_data(
  access_token,
  app_key,
  instance_name,
  tenant_id,
  wip_report_id,
  as_of_date,
  data_pool,
  page_count
) {
  let has_error_occured = false;

  const instance_list = [
    "Expert Heating and Cooling Co LLC",
    "PARKER-ARNTZ PLUMBING AND HEATING, INC.",
    "Family Heating & Cooling Co LLC",
  ];

  try {
    let shouldIterate = false;
    do {
      const api_endpoint = `https://api.servicetitan.io/reporting/v2/tenant/${tenant_id}/report-category/accounting/reports/${wip_report_id}/data`;

      const api_url =
        api_endpoint + `?page=${page_count + 1}&includeTotal=true`;
      const request = await fetch(api_url, {
        method: "POST",
        headers: {
          "Content-type": "application/json",
          Authorization: access_token, // Include "Bearer" before access token
          "ST-App-Key": app_key,
        },
        body: JSON.stringify({
          parameters: [
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
              value: "0",
            },
            {
              name: "InvoiceRevenueStatus",
              value: "0",
            },
            {
              name: "ReturnSyncStatus",
              value: "0",
            },
            {
              name: "ReturnStatus",
              value: "R",
            },
            {
              name: "VendorBillStatus",
              value: "0",
            },
            {
              name: "VendorCostType",
              value: "B",
            },
          ],
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

      const data = response.data;

      shouldIterate = response["hasMore"];

      const pushing_item = [];

      const updatedData = await Promise.all(
        data.map(async (record) => {
          const instance_id = String(instance_list.indexOf(instance_name) + 1);
          if (record[0]) {
            record[0] = await add_suffix(record[0], instance_id);
          }

          const modified_record = {
            ProjectNumber: record[0],
            ProjectName: record[1],
            ProjectStatus: record[2],
            ProjectContractStartDate: record[3],
            ActualCompletionDate: record[4],
            ContractValue: record[5],
            ChangeOrderValue: record[6],
            CostAdjustment: record[7],
            TotalEstimatedCost: record[8],
            EstimatedMargin: record[9],
            EstimatedMarginPercentage: record[10],
            TotalCost: record[11],
            CostToComplete: record[12],
            PercentCompleteCost: record[13],
            EarnedRevenue: record[14],
            TotalRevenue: record[15],
            OverBilling: record[16],
            UnderBilling: record[17],
          };

          return {
            instance_id: instance_id,
            ...modified_record,
            UTC_update_date: as_of_date,
          };
        })
      );

      // Now `updatedData` contains the modified records with the updated `UTC_update_date`

      data_pool = [...data_pool, ...updatedData];

      console.log(instance_name, data_pool.length, "/", response["totalCount"]);

      page_count = page_count + 1;
    } while (shouldIterate);
  } catch (error) {
    console.error(
      `WIP Report fetching failed for ${instance_name}. Try Again!:`,
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

module.exports = get_wip_report_data;
