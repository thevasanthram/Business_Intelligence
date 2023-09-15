const fs = require("fs");
const path = require("path");
const flattenObject = require("./../modules/new_flatten_object");
const extractMatchingValues = require("./../modules/extract_matching_values");

async function flat_data_insertion(
  sql_client,
  data_pool,
  header_data,
  table_name,
  inserting_batch_limit
) {
  // Generate SQL statements to insert data
  let written_records_count = 0;

  const fomatted_table_name =
    table_name.replace(/-/g, "_").replace(/\//g, "_") + "_table";

  for (let i = 0; i < data_pool.length; i = i + inserting_batch_limit) {
    // batchwise inserting data, each batch contains 300 insertions

    await Promise.all(
      data_pool
        .slice(i, i + inserting_batch_limit)
        .map(async (currentObj, index) => {
          const flattenedObj = flattenObject(currentObj);
          const filteredObj = extractMatchingValues(header_data, flattenedObj);

          const flat_data_insertion_query = `INSERT INTO ${fomatted_table_name} (${Object.keys(
            filteredObj
          )
            .map((column) => `[${column}]`)
            .join(", ")}) VALUES (${Object.values(filteredObj)
            .map((value) => {
              return String(value).includes(`'`)
                ? `'${value.replace(/'/g, `''`)}'`
                : `'${value}'`;
            })
            .join(", ")});`;

          try {
            const flat_data_insertion = await sql_client.query(
              flat_data_insertion_query
            );
          } catch (error) {
            // if theres a exceptional response in some api
            // console.log("header data: ", header_data);
            // console.log("row data: ", flattenedObj);
            // console.log("filteredObj: ", filteredObj);
            console.log("insertion_query: ", flat_data_insertion_query);
            console.log("insertion_error: ", error);
            // Create the folder if it doesn't exist
            const folderPath = "./error_responses";
            if (!fs.existsSync(folderPath)) {
              fs.mkdirSync(folderPath, { recursive: true });
            }

            // Create the file path
            const filePath = path.join(
              folderPath,
              fomatted_table_name + ".txt"
            );

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

  console.log(fomatted_table_name, "data insertion successfull");
}

module.exports = flat_data_insertion;
