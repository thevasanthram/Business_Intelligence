const response_format_csv_generator = require("./response_format_csv_generator");
const flattenObject = require("./new_flatten_object");

const response_header = {
  hasMore: true,
  continueFrom: "string",
  data: [
    {
      id: 0,
      jobId: 0,
      appointmentNumber: "string",
      start: "string",
      end: "string",
      arrivalWindowStart: "string",
      arrivalWindowEnd: "string",
      status: {},
      specialInstructions: "string",
      createdOn: "string",
      modifiedOn: "string",
      customerId: 0,
      active: true,
    },
  ],
};

const api_group = "jpm";
const api_name = "appointments";

const formatted_api_group = api_group.replace(/-/g, "_");
const formatted_api_name = api_name.replace(/-/g, "_");

const csv_folder_path = "./../csv_response_formats";

const csv_file_name = formatted_api_group + "_" + formatted_api_name;

const flattenedObj = flattenObject(response_header.data[0]);

response_format_csv_generator(flattenedObj, csv_folder_path, csv_file_name);
