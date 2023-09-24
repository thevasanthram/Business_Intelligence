const fs = require("fs");

// Read the SQL file
const sqlFileContent = fs.readFileSync(
  "./error_responses/location.txt",
  "utf8"
);

// Regular expression pattern to match "VALUES (number,"
const pattern = /VALUES \((\d+),/g;

// Array to store extracted IDs
const ids = [];

// Iterate through each line and find all matches
let match;
while ((match = pattern.exec(sqlFileContent)) !== null) {
  const id = parseInt(match[1], 10);
  ids.push(id);
}

fs.writeFile(
  "./error_responses/new.txt",
  JSON.stringify(ids),
  { flag: "a" },
  (err) => {
    if (err) {
      console.error("Error writing to file:", err);
    }
  }
);

console.log(ids);
