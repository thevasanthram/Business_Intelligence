const fs = require("fs");
const path = require("path");
const flattenObject = require("./../modules/new_flatten_object");
const extractMatchingValues = require("./../modules/extract_matching_values");

async function flat_data_insertion(
  sql_client,
  data_pool,
  header_data,
  table_name
) {
  // Generate SQL statements to insert data
  let written_records_count = 0;

  const inserting_batch_limit = 300;

  let whole_insertion_query = "";

  for (let i = 0; i < data_pool.length; i = i + inserting_batch_limit) {
    // batchwise inserting data, each batch contains 300 insertions

    await Promise.all(
      data_pool
        .slice(i, i + inserting_batch_limit)
        .map(async (currentObj, index) => {
          const flat_data_insertion_query = `INSERT INTO ${table_name} (${header_data
            .map((column) => `[${column}]`)
            .join(", ")}) VALUES (${Object.values(currentObj)
            .map((value) => {
              if (typeof value == "string") {
                return value.includes(`'`)
                  ? `'${value.replace(/'/g, `''`)}'`
                  : `'${value}'`;
              } else {
                return value;
              }
            })
            .join(", ")});`;

          try {
            // console.log(
            //   "flat_data_insertion_query: ",
            //   flat_data_insertion_query
            // );
            const flat_data_insertion = await sql_client.query(
              flat_data_insertion_query
            );
          } catch (error) {
            // if theres a exceptional response in some api
            console.log("insertion_query: ", flat_data_insertion_query);
            console.log("insertion_error: ", error);
            // Create the folder if it doesn't exist
            const folderPath = "./error_responses";
            if (!fs.existsSync(folderPath)) {
              fs.mkdirSync(folderPath, { recursive: true });
            }

            // Create the file path
            const filePath = path.join(folderPath, table_name + ".txt");

            const error_response_msg =
              "\n" +
              flat_data_insertion_query +
              "\n\n" +
              JSON.stringify(error) +
              "\n\n\n";

            fs.writeFile(filePath, error_response_msg, { flag: "a" }, (err) => {
              if (err) {
                console.error("Error writing to file:", err);
              }
            });
          }
        })
    );

    written_records_count =
      written_records_count +
      data_pool.slice(i, i + inserting_batch_limit).length;

    console.log(written_records_count, "/", data_pool.length, "records stored");
  }

  // // Create the folder if it doesn't exist
  // const folderPath = "./error_responses";
  // if (!fs.existsSync(folderPath)) {
  //   fs.mkdirSync(folderPath, { recursive: true });
  // }

  // // Create the file path
  // const filePath = path.join(folderPath, table_name + ".txt");

  // fs.writeFile(filePath, whole_insertion_query, { flag: "a" }, (err) => {
  //   if (err) {
  //     console.error("Error writing to file:", err);
  //   }
  // });

  console.log(table_name, "data insertion successfull");
}

module.exports = flat_data_insertion;
