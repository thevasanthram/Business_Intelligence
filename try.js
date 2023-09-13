const getAPIData = require("./modules/get_api_data");
const extract_matching_values = require("./modules/extract_matching_values");

const instance_name = "Family Heating & Cooling Co LLC";
const tenant_id = 1056112968;
const app_key = "ak1.h0wqje4yshdqvn1fso4we8cnu";
const client_id = "cid.qlr4t6egndd4mbvq3vu5tef11";
const client_secret = "cs1.v9jhueeo6kgcjx5in1r8716hpnmuh6pbxiddgsv5d3y0822jay";

const api_group = "accounting";
const api_name = "invoices";

const params_header = {
  createdOnOrAfter: "", // 2023-08-01T00:00:00.00Z
  includeTotal: true,
  pageSize: 2000,
};

const access_token =
  "eyJhbGciOiJSUzI1NiIsImtpZCI6IjYwNkZEM0Y3NzgxNzM4N0U3NjVDRTY5NkUxNzU0RTM3ODNBODU3MkJSUzI1NiIsInR5cCI6ImF0K2p3dCIsIng1dCI6IllHX1Q5M2dYT0g1MlhPYVc0WFZPTjRPb1Z5cyJ9.eyJuYmYiOjE2OTQ1ODg5MjYsImV4cCI6MTY5NDU4OTgyNiwiaXNzIjoiaHR0cHM6Ly9hdXRoLnNlcnZpY2V0aXRhbi5pbyIsImNsaWVudF9pZCI6ImNpZC5xbHI0dDZlZ25kZDRtYnZxM3Z1NXRlZjExIiwiYm9va2luZ19wcm92aWRlciI6InB3em5vZTd6ZzZ1eXo1b3Bwc2dubWZkaGMiLCJyZXBvcnRfY2F0ZWdvcnkiOiJwd3pub2U3emc2dXl6NW9wcHNnbm1mZGhjIiwidGVuYW50IjoiMTA1NjExMjk2OCIsImFwaWFwcF9pZCI6InBxOWpuZzNiZnc4bnYiLCJvd25lcl9pZCI6InRlbmFudC0xMDU2MTEyOTY4LWVudi1wcm9kIiwic2NvcGVzX3ZlcnNpb24iOiIxIiwiZXh0X2RhdGFfZ3VpZCI6ImFhYzc5YWYwLTUxNTYtNGRmMS05YmFkLTRjYTU5MmU2ZjNjYSIsImp0aSI6IjY4Q0RGMDc1NTE1NUQzQTUzRUJDRjhFMjNBRkFEMkY4IiwiaWF0IjoxNjk0NTg4OTI2LCJzY29wZSI6WyJ0bi5hY2MuYXBwYXltZW50czpyIiwidG4uYWNjLmludmVudG9yeWFkanVzdG1lbnRzOnIiLCJ0bi5hY2MuaW52ZW50b3J5YmlsbHM6ciIsInRuLmFjYy5pbnZlbnRvcnlyZWNlaXB0czpyIiwidG4uYWNjLmludmVudG9yeXRyYW5zZmVyczpyIiwidG4uYWNjLmludm9pY2VpdGVtczpyIiwidG4uYWNjLmludm9pY2VzOnIiLCJ0bi5hY2Muam91cm5hbGVudHJpZXM6ciIsInRuLmFjYy5wYXltZW50czpyIiwidG4uYWNjLnBheW1lbnR0ZXJtczpyIiwidG4uYWNjLnBheW1lbnR0eXBlczpyIiwidG4uYWNjLnB1cmNoYXNlb3JkZXJzOnIiLCJ0bi5hY2MucHVyY2hhc2VyZXR1cm5zOnIiLCJ0bi5hY2MudGF4em9uZXM6ciIsInRuLmNybS5ib29raW5ncHJvdmlkZXJ0YWdzOnIiLCJ0bi5jcm0uYm9va2luZ3M6ciIsInRuLmNybS5jdXN0b21lcnM6ciIsInRuLmNybS5sZWFkczpyIiwidG4uY3JtLmxvY2F0aW9uczpyIiwidG4uY3JtLnRhZ3M6ciIsInRuLmNzaS50ZWNobmljaWFucmF0aW5nOnIiLCJ0bi5kaXMuYXBwb2ludG1lbnRhc3NpZ25tZW50czpyIiwidG4uZGlzLmNhcGFjaXR5OnIiLCJ0bi5kaXMubm9uam9iYXBwb2ludG1lbnRzOnIiLCJ0bi5kaXMudGVhbXM6ciIsInRuLmRpcy50ZWNobmljaWFuc2hpZnRzOnIiLCJ0bi5kaXMuem9uZXM6ciIsInRuLmVxcy5pbnN0YWxsZWRlcXVpcG1lbnQ6ciIsInRuLmZybS5mb3JtczpyIiwidG4uZnJtLmpvYnM6ciIsInRuLmZybS5zdWJtaXNzaW9uczpyIiwidG4uaW52LmFkanVzdG1lbnRzOnIiLCJ0bi5pbnYucHVyY2hhc2VvcmRlcm1hcmt1cHM6ciIsInRuLmludi5wdXJjaGFzZW9yZGVyczpyIiwidG4uaW52LnB1cmNoYXNlb3JkZXJ0eXBlczpyIiwidG4uaW52LnJlY2VpcHRzOnIiLCJ0bi5pbnYucmV0dXJuczpyIiwidG4uaW52LnRyYW5zZmVyczpyIiwidG4uaW52LnRydWNrczpyIiwidG4uaW52LnZlbmRvcnM6ciIsInRuLmludi53YXJlaG91c2VzOnIiLCJ0bi5qYmNlLmNhbGxyZWFzb25zOnIiLCJ0bi5qcG0uYXBwb2ludG1lbnRzOnIiLCJ0bi5qcG0uam9iY2FuY2VscmVhc29uczpyIiwidG4uanBtLmpvYmhvbGRyZWFzb25zOnIiLCJ0bi5qcG0uam9iczpyIiwidG4uanBtLmpvYnR5cGVzOnIiLCJ0bi5qcG0ucHJvamVjdHM6ciIsInRuLmpwbS5wcm9qZWN0c3RhdHVzZXM6ciIsInRuLmpwbS5wcm9qZWN0c3Vic3RhdHVzZXM6ciIsInRuLm1hZHMuZXh0ZXJuYWxjYWxsYXR0cmlidXRpb25zOnIiLCJ0bi5tYWRzLndlYmJvb2tpbmdhdHRyaWJ1dGlvbnM6ciIsInRuLm1hZHMud2VibGVhZGZvcm1hdHRyaWJ1dGlvbnM6ciIsInRuLm1lbS5pbnZvaWNldGVtcGxhdGVzOnIiLCJ0bi5tZW0ubWVtYmVyc2hpcHM6ciIsInRuLm1lbS5tZW1iZXJzaGlwdHlwZXM6ciIsInRuLm1lbS5yZWN1cnJpbmdzZXJ2aWNlZXZlbnRzOnIiLCJ0bi5tZW0ucmVjdXJyaW5nc2VydmljZXM6ciIsInRuLm1lbS5yZWN1cnJpbmdzZXJ2aWNldHlwZXM6ciIsInRuLm1yZXAucmV2aWV3czpyIiwidG4ubXJrLmNhbXBhaWduczpyIiwidG4ubXJrLmNhdGVnb3JpZXM6ciIsInRuLm1yay5jb3N0czpyIiwidG4ubXJrLnN1cHByZXNzaW9uczpyIiwidG4ucGIuY2F0ZWdvcmllczpyIiwidG4ucGIuZGlzY291bnRzYW5kZmVlczpyIiwidG4ucGIuZXF1aXBtZW50OnIiLCJ0bi5wYi5pbWFnZXM6ciIsInRuLnBiLm1hdGVyaWFsczpyIiwidG4ucGIucHJpY2Vib29rOnIiLCJ0bi5wYi5zZXJ2aWNlczpyIiwidG4ucHJsLmFjdGl2aXR5Y29kZXM6ciIsInRuLnBybC5lbXBsb3llZXM6ciIsInRuLnBybC5ncm9zc3BheWl0ZW1zOnIiLCJ0bi5wcmwuam9iczpyIiwidG4ucHJsLmxvY2F0aW9uczpyIiwidG4ucHJsLm5vbmpvYnRpbWVzaGVldHM6ciIsInRuLnBybC5wYXlyb2xsYWRqdXN0bWVudHM6ciIsInRuLnBybC5wYXlyb2xsczpyIiwidG4ucHJsLnRlY2huaWNpYW5zOnIiLCJ0bi5wcmwudGltZXNoZWV0Y29kZXM6ciIsInRuLnJwci5keW5hbWljdmFsdWVzZXRzOnIiLCJ0bi5ycHIucmVwb3J0Y2F0ZWdvcmllczpyIiwidG4ucnByLnJlcG9ydHM6ciIsInRuLnNhbC5lc3RpbWF0ZXM6ciIsInRuLnN0dC5idXNpbmVzc3VuaXRzOnIiLCJ0bi5zdHQuZW1wbG95ZWVzOnIiLCJ0bi5zdHQudGFndHlwZXM6ciIsInRuLnN0dC50ZWNobmljaWFuczpyIiwidG4uc3R0LnVzZXJyb2xlczpyIiwidG4udGxjLmNhbGxzOnIiLCJ0bi50c20uZGF0YTpyIiwidG4udHNtLnRhc2tzOnIiXX0.DXm7QbOmkTYeAAUtRofImRZM54ROEOjyk340qHXgUMBeVncAP_sfNQSZkTFZpN6FQaYrNpZy7Tx4pqhKosBaYrG5bkv7rcS0EA6IWNqPtGSGRdkt-ubYUhqVHNjat7dIWVr0-Yu7hJapDsoM3rI-12q7-BFOIXEEEvII4qHDteFxkRTFKoIeKGtyfFy_49wGzpOeu5PG7PnjPWAUOFiUedSbbwuCwwojtZ3470GlD0yA94hMaWG2yGz7KBlOshkD6dxGD_SZg7CzLavavFLhfmFWmKuCCfeKe7DNWsap2s0x2ZKUQDB1tL7hMuTMQc-LnAHlSKxTY-NqJUMtCcsbDA";

async function try1() {
  const { data_pool, flattenedSampleObj } = await getAPIData(
    access_token,
    app_key,
    instance_name,
    tenant_id,
    api_group,
    api_name,
    params_header
  );

  console.log("flattenedSampleObj: ", flattenedSampleObj);
}

try1();

const ob = {
  instance_name: "Family Heating & Cooling Co LLC",
  id: 28217532,
  syncStatus: "Exported",
  summary:
    "Proposal to install new Water Heater\n" +
    "Water Heater Change-out\n" +
    " 40 gallon electric Size of W/H Type of W/H________________\n" +
    "Includes Labor, 2 flex connectors, 1 shut off valve, 1 drop pipe\n" +
    "Removal and disposal of old water heater. -customer WILL HAVE THE WALL OPEN AND DUCT WORK REMOVED FOR ACCESS TO THE \n" +
    "\n" +
    "The ductwork will need to be taken apart to change water heater. Closet opening isn't large enough. \n" +
    "\n" +
    'The closest with the existing water heater will need to be opened by a carpenter with dimensions of 24"w and 50"h prior to arrival and restored by carpenter upon completion. ',
  referenceNumber: "28217529",
  invoiceDate: "2023-06-08T00:00:00Z",
  dueDate: "2023-06-08T00:00:00Z",
  subTotal: "1782.00",
  salesTax: "0.00",
  salesTaxCode: null,
  total: "1782.00",
  balance: "0.00",
  invoiceType: { id: 75, name: "Service" },
  customer: { id: 20844895, name: "ZIEMKE, LISA" },
  customerAddress: {
    street: "4440 Longpoint Drive",
    unit: null,
    city: "Cheboygan",
    state: "MI",
    zip: "49721",
    country: "USA",
  },
  location: { id: 20879434, name: "ZIEMKE, LISA" },
  locationAddress: {
    street: "4440 Longpoint Drive",
    unit: null,
    city: "Cheboygan",
    state: "MI",
    zip: "49721",
    country: "USA",
  },
  businessUnit: { id: 13107315, name: "SPR PLUM AOR RES" },
  termName: "Due Upon Receipt",
  createdBy: "MichelleSteiner",
  batch: { id: 55168563, number: "157", name: "SPRY AR INV 6/8/23 BS " },
  depositedOn: "2023-07-16T01:18:06.9781795Z",
  createdOn: "2023-06-07T17:12:07.1198717Z",
  modifiedOn: "2023-07-16T01:18:28.25019Z",
  adjustmentToId: null,
  job: {
    id: 28217529,
    number: "28217529",
    type: "Install/ChangeOut - Wtr Heater     RES/SPR",
  },
  projectId: null,
  royalty: { status: "Pending", date: null, sentOn: null, memo: null },
  employeeInfo: {
    id: 11122849,
    name: "MichelleSteiner",
    modifiedOn: "2023-08-30T13:35:18.8814677Z",
  },
  commissionEligibilityDate: null,
  customFields: null,
};

const obje = {
  instance_name: "Family Heating & Cooling Co LLC",
  id: 28303666,
  syncStatus: "Exported",
  summary:
    "Tested unit is dirty outside and low on charge. Cleaned the condenser . Brought charge up to factory specifications. Saw no damage from electrician, unit is operating properly at this time.",
  referenceNumber: "28303663",
  invoiceDate: "2023-06-09T00:00:00Z",
  dueDate: "2023-06-09T00:00:00Z",
  subTotal: "251.67",
  salesTax: "0.00",
  salesTaxCode: null,
  total: "251.67",
  balance: "251.67",
  invoiceType: null,
  customer: { id: 22934660, name: "KNL - Meilke, David" },
  customerAddress: {
    street: "703 E. Mitchell St.",
    unit: "",
    city: "Petoskey",
    state: "MI",
    zip: "49770",
    country: "",
  },
  location: { id: 22937406, name: "KNL - Meilke, David" },
  locationAddress: {
    street: "703 E. Mitchell St.",
    unit: "",
    city: "Petoskey",
    state: "MI",
    zip: "49770",
    country: "",
  },
  businessUnit: { id: 16118478, name: "KNL HVAC SRV RES" },
  termName: "",
  createdBy: "ScottBennett",
  batch: { id: 55147653, number: "133", name: "KNL AR inv 6/9/23 dm" },
  depositedOn: "2023-07-16T01:10:02.253459Z",
  createdOn: "2023-06-09T12:00:36.5472956Z",
  modifiedOn: "2023-07-16T01:10:20.8356387Z",
  adjustmentToId: null,
  job: { id: 28303663, number: "28303663", type: "No Cool     RES/KNL" },
  projectId: null,
  royalty: { status: "Pending", date: null, sentOn: null, memo: null },
  employeeInfo: {
    id: 11138231,
    name: "ScottBennett",
    modifiedOn: "2023-08-15T20:50:45.1232863Z",
  },
  commissionEligibilityDate: null,
  customFields: null,
};

const header = {
  instance_name: "Family Heating & Cooling Co LLC",
  id: 28217532,
  syncStatus: "Exported",
  summary:
    "Proposal to install new Water Heater\n" +
    "Water Heater Change-out\n" +
    " 40 gallon electric Size of W/H Type of W/H________________\n" +
    "Includes Labor, 2 flex connectors, 1 shut off valve, 1 drop pipe\n" +
    "Removal and disposal of old water heater. -customer WILL HAVE THE WALL OPEN AND DUCT WORK REMOVED FOR ACCESS TO THE \n" +
    "\n" +
    "The ductwork will need to be taken apart to change water heater. Closet opening isn't large enough. \n" +
    "\n" +
    'The closest with the existing water heater will need to be opened by a carpenter with dimensions of 24"w and 50"h prior to arrival and restored by carpenter upon completion. ',
  referenceNumber: "28217529",
  invoiceDate: "2023-06-08T00:00:00Z",
  dueDate: "2023-06-08T00:00:00Z",
  subTotal: "1782.00",
  salesTax: "0.00",
  salesTaxCode: null,
  total: "1782.00",
  balance: "0.00",
  invoiceType: null,
  invoiceTypeid: 75,
  invoiceTypename: "Service",
  customer: null,
  customerid: 20844895,
  customername: "ZIEMKE, LISA",
  customerAddress: null,
  customerAddressstreet: "4440 Longpoint Drive",
  customerAddressunit: null,
  customerAddresscity: "Cheboygan",
  customerAddressstate: "MI",
  customerAddresszip: "49721",
  customerAddresscountry: "USA",
  location: null,
  locationid: 20879434,
  locationname: "ZIEMKE, LISA",
  locationAddress: null,
  locationAddressstreet: "4440 Longpoint Drive",
  locationAddressunit: null,
  locationAddresscity: "Cheboygan",
  locationAddressstate: "MI",
  locationAddresszip: "49721",
  locationAddresscountry: "USA",
  businessUnit: null,
  businessUnitid: 13107315,
  businessUnitname: "SPR PLUM AOR RES",
  termName: "Due Upon Receipt",
  createdBy: "MichelleSteiner",
  batch: null,
  batchid: 55168563,
  batchnumber: "157",
  batchname: "SPRY AR INV 6/8/23 BS ",
  depositedOn: "2023-07-16T01:18:06.9781795Z",
  createdOn: "2023-06-07T17:12:07.1198717Z",
  modifiedOn: "2023-07-16T01:18:28.25019Z",
  adjustmentToId: null,
  job: null,
  jobid: 28217529,
  jobnumber: "28217529",
  jobtype: "Install/ChangeOut - Wtr Heater     RES/SPR",
  projectId: null,
  royalty: null,
  royaltystatus: "Pending",
  royaltydate: null,
  royaltysentOn: null,
  royaltymemo: null,
  employeeInfo: null,
  employeeInfoid: 11122849,
  employeeInfoname: "MichelleSteiner",
  employeeInfomodifiedOn: "2023-08-30T13:35:18.8814677Z",
  commissionEligibilityDate: null,
  customFields: null,
};

console.log("header: ", header);

const row = {
  instance_name: "Family Heating & Cooling Co LLC",
  id: 28303666,
  syncStatus: "Exported",
  summary:
    "Tested unit is dirty outside and low on charge. Cleaned the condenser . Brought charge up to factory specifications. Saw no damage from electrician, unit is operating properly at this time.",
  referenceNumber: "28303663",
  invoiceDate: "2023-06-09T00:00:00Z",
  dueDate: "2023-06-09T00:00:00Z",
  subTotal: "251.67",
  salesTax: "0.00",
  salesTaxCode: null,
  total: "251.67",
  balance: "251.67",
  invoiceType: null,
  customerid: 22934660,
  customername: "KNL - Meilke, David",
  customerAddressstreet: "703 E. Mitchell St.",
  customerAddressunit: "",
  customerAddresscity: "Petoskey",
  customerAddressstate: "MI",
  customerAddresszip: "49770",
  customerAddresscountry: "",
  locationid: 22937406,
  locationname: "KNL - Meilke, David",
  locationAddressstreet: "703 E. Mitchell St.",
  locationAddressunit: "",
  locationAddresscity: "Petoskey",
  locationAddressstate: "MI",
  locationAddresszip: "49770",
  locationAddresscountry: "",
  businessUnitid: 16118478,
  businessUnitname: "KNL HVAC SRV RES",
  termName: "",
  createdBy: "ScottBennett",
  batchid: 55147653,
  batchnumber: "133",
  batchname: "KNL AR inv 6/9/23 dm",
  depositedOn: "2023-07-16T01:10:02.253459Z",
  createdOn: "2023-06-09T12:00:36.5472956Z",
  modifiedOn: "2023-07-16T01:10:20.8356387Z",
  adjustmentToId: null,
  jobid: 28303663,
  jobnumber: "28303663",
  jobtype: "No Cool     RES/KNL",
  projectId: null,
  royaltystatus: "Pending",
  royaltydate: null,
  royaltysentOn: null,
  royaltymemo: null,
  employeeInfoid: 11138231,
  employeeInfoname: "ScottBennett",
  employeeInfomodifiedOn: "2023-08-15T20:50:45.1232863Z",
  commissionEligibilityDate: null,
  customFields: null,
};

const resultant_object = extract_matching_values(header, row);

console.log("resultant_object: ", resultant_object);

const data = Object.values(resultant_object)
  .map((value) => {
    if (value && value != "") {
      return String(value).replace(/,/g, "").replace(/\n/g, "");
    } else {
      return "null";
    }
  })
  .join(",");

console.log("data: ", data);

console.log("==============================================================");

let csvData = [];
for (let i = 0; i < Object.values(resultant_object).length; i++) {
  const value = Object.values(resultant_object)[i];
  if (value && value != "") {
    csvData.push(String(value).replace(/,/g, "_").replace(/\n/g, "_"));
  } else {
    csvData.push("null");
  }
}

csvData = csvData.join(",");

console.log(csvData);
