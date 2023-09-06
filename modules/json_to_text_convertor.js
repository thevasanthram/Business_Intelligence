const fs = require("fs");
const path = require("path");

function json_to_text_convertor(data_pool, api_group, api_name) {
  const formatted_api_group = api_group.replace(/-/g, "_").replace("/", "_");
  const formatted_api_name = api_name.replace(/-/g, "_").replace("/", "_");

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

  fs.writeFile(filePath, JSON.stringify(data_pool), { flag: "w" }, (err) => {
    if (err) {
      console.error("Error writing to file:", err);
    } else {
      console.log("Data has been written to", filePath);
    }
  });
}

module.exports = json_to_text_convertor;
