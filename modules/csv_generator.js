const fs = require("fs");
const path = require("path");
const flattenObject = require("./new_flatten_object");
const extractMatchingValues = require("./extract_matching_values");

// Function to process and write data in batches
async function csv_generator(data_pool, flattenedSampleObj, csv_file_name) {
  // Process and write data in batches
  const batchSize = 100; // Set the batch size as needed
  let index = 0;

  const csv_folder_path = "./flat_tables";

  csv_file_name = csv_file_name.replace(/-/g, "_").replace("/", "_");

  // console.log("csv_generator function");

  // Create the folder if it doesn't exist
  if (!fs.existsSync(csv_folder_path)) {
    fs.mkdirSync(csv_folder_path, { recursive: true });
  }

  // Create the file path
  const filePath = path.join(csv_folder_path, csv_file_name + ".csv");

  // write the header
  async function write_header() {
    const csvHeader = Object.keys(flattenedSampleObj).join(",");

    // Create a new CSV file and write the header
    fs.writeFileSync(filePath, csvHeader + "\n");
  }

  const modifed_data_pool = Object.keys(data_pool).map((key) => {
    return data_pool[key];
  });

  // Function to write the next batch of data
  async function writeNextBatch() {
    const batch = modifed_data_pool.slice(index, index + batchSize);

    if (batch.length > 0) {
      for (const currentObj of batch) {
        const flattenedObj = flattenObject(currentObj);
        const filteredObj = extractMatchingValues(
          flattenedSampleObj,
          flattenedObj
        );

        // Convert the array of values to CSV format
        const csvData = Object.values(filteredObj)
          .map((value) => {
            if (value && value != "") {
              return String(value)
                .replace(/,/g, "")
                .replace(/\n/g, "")
                .replace(/\r\n/g, "")
                .replace(/\r/g, "");
            } else {
              if (value == "") {
                return "null";
              } else {
                return 0;
              }
            }
          })
          .join(",");

        // Write the CSV data to the file
        fs.appendFileSync(filePath, csvData + "\n");
      }

      // End the batch
      index += batchSize;

      // Process the next batch in the next event loop iteration
      setImmediate(writeNextBatch);
    } else {
      // No more data to process
      console.log(csv_file_name + " file has been created");
    }
  }

  // Start processing by writing the header and all batch
  await write_header();
  await writeNextBatch();
}

module.exports = csv_generator;
