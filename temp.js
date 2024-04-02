const business_unit_data = require("./modules/business_unit_details");

// console.log(Object.keys(business_unit_data).length);

let modified_business_unit = {};

Object.keys(business_unit_data).map((bu_id) => {
  const record = business_unit_data[bu_id];

  let modified_business_id = "";
  const business = record["Business"];
  if (business == "NMI") {
    modified_business_id = bu_id + "_03";
  } else if (business == "PA") {
    modified_business_id = bu_id + "_02";
  } else if (business == "EXP") {
    modified_business_id = bu_id + "_01";
  } else {
    console.log("edge");
  }

  modified_business_unit[modified_business_id] = {
    ...record,
    "BU ID": modified_business_id,
  };
});

const fs = require("fs");

fs.writeFile("zzz.js", JSON.stringify(modified_business_unit), () =>
  console.log("done")
);
