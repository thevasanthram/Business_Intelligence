const flattenObject = require("./../modules/new_flatten_object");
const extractMatchingValues = require("./../modules/extract_matching_values");

async function flat_data_insertion(
  sql_client,
  data_pool,
  flattenedSampleObj,
  api_group,
  api_name,
  inserting_batch_limit
) {
  // Generate SQL statements to insert data
  console.log("Storing Data into Database initiated successfully");

  let written_records_count = 0;

  const formatted_api_group = api_group.replace(/-/g, "_");
  const formatted_api_name = api_name.replace(/-/g, "_");

  for (let i = 0; i < data_pool.length; i = i + inserting_batch_limit) {
    // batchwise inserting data, each batch contains 300 insertions

    await Promise.all(
      data_pool
        .slice(i, i + inserting_batch_limit)
        .map(async (currentObj, index) => {
          const flattenedObj = flattenObject(currentObj);
          const filteredObj = extractMatchingValues(
            flattenedSampleObj,
            flattenedObj
          );

          const flat_data_insertion_query = `INSERT INTO ${
            formatted_api_group + "_" + formatted_api_name + "_flat_table"
          } (${Object.keys(filteredObj).join(", ")}) VALUES (${Object.values(
            filteredObj
          )
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
            // console.log("Flat-Data insertion failed. Try Again!: ", error);
          }
        })
    );

    written_records_count =
      written_records_count +
      data_pool.slice(i, i + inserting_batch_limit).length;

    console.log(written_records_count, "/", data_pool.length, "records stored");
  }

  console.log("Data Insertion Successfull");
}

module.exports = flat_data_insertion;
