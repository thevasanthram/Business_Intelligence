function areValuesEquivalent(val1, val2) {
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

function isUpdatedRecordValid(updatedRecord, originalRecord) {
  const updatedKeys = Object.keys(updatedRecord);

  for (let key of updatedKeys) {
    if (
      !originalRecord.hasOwnProperty(key) ||
      !areValuesEquivalent(updatedRecord[key], originalRecord[key])
    ) {
      return false; // Key is missing in originalRecord or values are not equivalent
    }
  }

  return true; // All keys and values in updatedRecord match those in originalRecord
}

// Example usage:
const updatedRecord = {
  id: "90573272_01",
  name: "Kevin Botz",
  source: "General",
  status: "New",
  customer_type: "default",
  start: "2024-05-30T12:00:00Z",
  bookingProviderId: "73789073_01",
  createdOn: "2024-05-27T11:17:24.325544Z",
  modifiedOn: "2024-05-27T11:17:24.5654476Z",
  address_street: "22150 Harmon Street",
  address_unit: "default",
  address_city: "Taylor",
  address_state: "MI",
  address_zip: "48180",
  address_country: "USA",
  business_unit_id: "1",
  actual_business_unit_id: "1",
  campaign_id: "1",
  actual_campaign_id: "1",
  job_details_id: "1",
};

const originalRecord = {
  id: "90573272_01",
  name: "Kevin Botz",
  source: "General",
  status: "New",
  customer_type: "default",
  start: new Date("2024-05-30T12:00:00.000Z"),
  bookingProviderId: "73789073_01",
  createdOn: new Date("2024-05-27T11:17:24.000Z"),
  modifiedOn: new Date("2024-05-27T11:17:25.000Z"),
  address_street: "22150 Harmon Street",
  address_unit: "default",
  address_city: "Taylor",
  address_state: "MI",
  address_zip: "48180",
  address_country: "USA",
  business_unit_id: "1",
  actual_business_unit_id: "1",
  campaign_id: "1",
  actual_campaign_id: "1",
  job_details_id: "1",
};

console.log(isUpdatedRecordValid(updatedRecord, originalRecord)); // Output: false (modifiedOn differs)
