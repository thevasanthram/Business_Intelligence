const fs = require("fs");
const path = require("path");

async function response_format_csv_generator(data, csv_file_name) {
  // Process and write data in batches
  let csvContent = "";
  const csv_folder_path = "./csv_response_formats";

  csv_file_name = csv_file_name.replace(/-/g, "_").replace("/", "_");

  // Create the folder if it doesn't exist
  if (!fs.existsSync(csv_folder_path)) {
    fs.mkdirSync(csv_folder_path, { recursive: true });
  }

  // Create the file path
  const filePath = path.join(csv_folder_path, csv_file_name + ".csv");

  // Join the CSV rows with newline characters
  csvContent = Object.keys(data)
    .map((key) => {
      return key + "," + data[key];
    })
    .join("\n");

  fs.writeFileSync(`${filePath}`, csvContent);
  console.log("Data appended to CSV file.");
}

module.exports = response_format_csv_generator;
