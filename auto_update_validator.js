const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");
const create_sql_connection = require("./modules/create_sql_connection");

const updated_data = {};

async function readCSV(filePath) {
  const results = [];

  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (data) => results.push(data))
      .on("end", () => {
        resolve(results);
      })
      .on("error", (error) => {
        reject(error);
      });
  });
}

async function updated_data_collector(directoryPath) {
  try {
    const files = fs.readdirSync(directoryPath);

    const csvFiles = files.filter((file) => path.extname(file) === ".csv");

    for (const file of csvFiles) {
      const filePath = path.join(directoryPath, file);
      const data = await readCSV(filePath);
      const key = path.basename(file, path.extname(file)); // Use file name without extension as the key
      updated_data[key] = data;
    }
  } catch (error) {
    console.error("Error reading directory or CSV files:", error);
  }
}

async function areValuesEquivalent(val1, val2) {
  let normalizedVal1 =
    val1 === "0" || val1 === 0 ? "0" : val1 === null ? "0" : val1;
  let normalizedVal2 =
    val2 === "0" || val2 === 0 ? 0 : val2 === null ? 0 : val2;

  // If val2 is a Date object, convert it to a string up to seconds for comparison
  if (val2 instanceof Date) {
    normalizedVal1 = normalizedVal1.slice(0, 10);
    normalizedVal2 = val2.toISOString().split(".")[0].slice(0, 10); // Convert to ISO string and remove microseconds

    // console.log(normalizedVal1, normalizedVal2);
  }

  // console.log(normalizedVal1 == normalizedVal2, normalizedVal1);

  return normalizedVal1 == normalizedVal2; // Non-strict comparison to handle '0' == 0 and 0 == null
}

async function isUpdatedRecordValid(updatedRecord, originalRecord) {
  const updatedKeys = Object.keys(updatedRecord);

  for (let key of updatedKeys) {
    if (
      !originalRecord.hasOwnProperty(key) ||
      !(await areValuesEquivalent(updatedRecord[key], originalRecord[key]))
    ) {
      console.log("key: ", key);
      return false; // Key is missing in originalRecord or values are not equivalent
    }
  }

  return true; // All keys and values in updatedRecord match those in originalRecord
}

async function validator() {
  let sql_request = "";

  // Retry connection until successful
  do {
    sql_request = await create_sql_connection();
  } while (!sql_request);

  await Promise.all(
    Object.keys(updated_data).map(async (table_name) => {
      if (
        table_name != "cogs_labor" &&
        table_name != "cogs_equipment" &&
        table_name != "cogs_material" &&
        table_name != "cogs_service"
      ) {
        const table_data = updated_data[table_name];

        let flag = true;

        for (let i = 0; i < table_data.length; i++) {
          const updated_record = table_data[i];
          //   console.log("updated_record: ", updated_record);

          const validation_query = `SELECT * FROM ${table_name} WHERE id = '${updated_record["id"]}'`;

          const response = await sql_request.query(validation_query);

          if (response.recordset.length > 0) {
            const original_record = response.recordset[0];

            // console.log("original_record: ", original_record);

            if (
              !(await isUpdatedRecordValid(updated_record, original_record))
            ) {
              flag = false;
              const folderPath = `./mismatching_records/`;
              if (!fs.existsSync(folderPath)) {
                fs.mkdirSync(folderPath, { recursive: true });
              }

              // Create the file path
              const filePath = path.join(folderPath, `${table_name}.csv`);
              fs.appendFile(
                filePath,
                Object.values(updated_record).join(",") + "\n",
                (err) => {
                  if (err) {
                    console.error("Error writing to file:", err);
                  } else {
                    console.log(
                      `Error has been noted to ${table_name}`,
                      filePath
                    );
                  }
                }
              );
            }
          }
        }

        if (flag) {
          console.log(table_name, "verification successfull");
        }
      }
    })
  );
}

async function main() {
  const directoryPath = "./auto_update_files"; // Replace with the path to your directory containing CSV files
  await updated_data_collector(directoryPath);

  await validator();
}

main();
