const flattenObject = require("./flatten_object");
const extractMatchingValues = require("./extract_matching_values");
const fs = require("fs");
const path = require("path");

// Function to process and write data in batches
async function csv_generator(data_pool, flattenedSampleObj, csv_file_name) {
  // Process and write data in batches
  const batchSize = 1000; // Set the batch size as needed
  let index = 0;
  let csvContent = Object.keys(flattenedSampleObj).join(",");

  const csv_folder_path = "./flat_tables";

  csv_file_name = csv_file_name.replace(/-/g, "_").replace("/", "_");

  // Create the folder if it doesn't exist
  if (!fs.existsSync(csv_folder_path)) {
    fs.mkdirSync(csv_folder_path, { recursive: true });
  }

  // Create the file path
  const filePath = path.join(csv_folder_path, csv_file_name + ".csv");

  // Function to write the next batch of data
  function writeNextBatch() {
    const batch = data_pool.slice(index, index + batchSize);

    if (batch.length > 0) {
      for (const currentObj of batch) {
        const flattenedObj = flattenObject(currentObj);
        const filteredObj = extractMatchingValues(
          flattenedSampleObj,
          flattenedObj
        );

        // Convert the array of arrays to CSV format
        const csvData = Object.values(filteredObj)
          .map((value) => {
            if (value) {
              return String(value).replace(",", ";");
            } else {
              return "null";
            }
          })
          .join(",");

        // Join the CSV rows with newline characters
        csvContent = csvContent + csvData + "\n";
      }

      // End the batch
      index += batchSize;

      // Process the next batch in the next event loop iteration
      setImmediate(writeNextBatch);
    } else {
      // No more data to process, end the streams
      fs.writeFileSync(`${filePath}`, csvContent);
      console.log("Data appended to CSV file.");
    }
  }

  // Start processing by writing the first batch
  writeNextBatch();
}

module.exports = csv_generator;
