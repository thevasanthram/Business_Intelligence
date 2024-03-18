const flattenObject = require("./new_flatten_object");

async function find_lenghthiest_header(data_pool) {
  // Iterate all the elements in data_pool and fetch the object having maximum property
  let sampleObj = {};

  if (data_pool.length > 0) {
    sampleObj = flattenObject(data_pool[0]); // Take a sample object to infer the table structure
  }

  Object.keys(data_pool).map((response_data_id, index) => {
    const response_data = data_pool[response_data_id];
    const current_flattened_object = flattenObject(response_data);
    if (
      Object.keys(current_flattened_object).length >
      Object.keys(sampleObj).length
    ) {
      sampleObj = current_flattened_object;
    }
  });

  const flattenedSampleObj = sampleObj;

  return flattenedSampleObj;
}

module.exports = find_lenghthiest_header;
