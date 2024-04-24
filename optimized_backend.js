const sql = require("mssql");
const fs = require("fs");

// modules
const create_sql_connection = require("./modules/create_sql_connection");
const getAccessToken = require("./modules/get_access_token");
const getAPIWholeData = require("./modules/get_api_whole_data");
const hvac_data_insertion = require("./modules/hvac_data_insertion");
const hvac_merge_insertion = require("./modules/hvac_merge_insertion");
const hvac_flat_data_insertion = require("./modules/hvac_flat_data_insertion");
const find_lenghthiest_header = require("./modules/find_lengthiest_header");
const create_hvac_schema = require("./modules/create_hvac_schema");
const flush_hvac_schema = require("./modules/flush_hvac_schema");
const flush_hvac_data = require("./modules/flush_hvac_data");
const kpi_data = require("./modules/updated_business_unit_details");
const us_cities_list = require("./modules/us_cities");
const { type } = require("os");
// const csv_generator = require("./modules/csv_generator");

// Service Titan's API parameters
const instance_details = [
  {
    instance_name: "Expert Heating and Cooling Co LLC",
    tenant_id: 1011756844,
    app_key: "ak1.ztsdww9rvuk0sjortd94dmxwx",
    client_id: "cid.jk53hfwwcq6a1zgtbh96byil4",
    client_secret: "cs1.2hdc1yd19hpxzmdeg5rfuc6i3smpxy9iei0yhq1p7qp8mwyjda",
  },
  {
    instance_name: "PARKER-ARNTZ PLUMBING AND HEATING, INC.",
    tenant_id: 1475606437,
    app_key: "ak1.w9fgjo8psqbyi84vocpvzxp8y",
    client_id: "cid.r82bhd4u7htjv56h7sqjk0jya",
    client_secret: "cs1.4q3yjgyhjb9yaeietpsoozzc8u2qgw80j8ze43ovz1308e7zz7",
  },
  {
    instance_name: "Family Heating & Cooling Co LLC",
    tenant_id: 1056112968,
    app_key: "ak1.h0wqje4yshdqvn1fso4we8cnu",
    client_id: "cid.qlr4t6egndd4mbvq3vu5tef11",
    client_secret: "cs1.v9jhueeo6kgcjx5in1r8716hpnmuh6pbxiddgsv5d3y0822jay",
  },
];

let timezoneOffsetHours = 5; // 0 hours ahead of UTC
let timezoneOffsetMinutes = 30; // 0 minutes ahead of UTC

// Check if the date is in daylight saving time (PDT)
// const today = new Date();
// const daylightSavingStart = new Date(today.getFullYear(), 2, 14); // March 14
// const daylightSavingEnd = new Date(today.getFullYear(), 10, 7); // November 7

// if (today >= daylightSavingStart && today < daylightSavingEnd) {
//   // Date is in PDT
//   timezoneOffsetHours = 7;
// } else {
//   // Date is in PST
//   timezoneOffsetHours = 8;
// }

let modifiedBeforeTime = new Date();

modifiedBeforeTime.setUTCHours(7, 0, 0, 0);

let modifiedOnOrAfterTime = new Date();

modifiedOnOrAfterTime.setDate(modifiedOnOrAfterTime.getDate() - 1);

modifiedOnOrAfterTime.setUTCHours(7, 0, 0, 0);

const params_header = {
  modifiedOnOrAfter: modifiedOnOrAfterTime.toISOString(), // 2023-12-25T00:00:00.00Z
  modifiedBefore: modifiedBeforeTime.toISOString(), //createdBeforeTime.toISOString()
  includeTotal: true,
  pageSize: 2000,
  active: "any",
};

console.log("params_header: ", params_header);

let lastInsertedId = 0;

let data_lake = {};
let should_auto_update = false;

const hvac_tables = {
  legal_entity: {
    // manual entry
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      legal_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
    },
  },
  us_cities: {
    columns: {
      id: {
        data_type: "INT",
        constraint: { primary: true, nullable: false },
      },
      latitude: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      longitude: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      city: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      state: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      county: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
    },
  },
  business_unit: {
    // settings business units
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      business_unit_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      business_unit_official_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      trade_type: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      segment_type: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      revenue_type: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      business: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      is_active: {
        data_type: "TINYINT",
        constraint: { nullable: true },
      },
      legal_entity_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
    },
  },
  project_business_unit: {
    // settings business units
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      business_unit_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      business_unit_official_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      trade_type: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      segment_type: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      revenue_type: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      business: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      is_active: {
        data_type: "TINYINT",
        constraint: { nullable: true },
      },
      legal_entity_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      legal_entity_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
    },
  },
  employees: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      role: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
    },
  },
  campaigns: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      is_active: {
        data_type: "TINYINT",
        constraint: { nullable: true },
      },
      createdOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      modifiedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      category_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      category_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      is_category_active: {
        data_type: "TINYINT",
        constraint: { nullable: true },
      },
      source: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      medium: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
    },
  },
  bookings: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      source: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      status: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      customer_type: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      start: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      bookingProviderId: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      createdOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      modifiedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      address_street: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      address_unit: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      address_city: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      address_state: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      address_zip: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      address_country: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      campaign_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_campaign_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
    },
  },
  customer_details: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      is_active: {
        data_type: "TINYINT",
        constraint: { nullable: true },
      },
      type: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      creation_date: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      address_street: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      address_unit: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      address_city: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      address_state: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      address_zip: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
    },
    // crm customers
  },
  location: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      street: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      unit: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      city: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      state: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      country: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      address_zip: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      acutal_address_zip: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      latitude: {
        data_type: "DECIMAL96",
        constraint: { nullable: true },
      },
      longitude: {
        data_type: "DECIMAL96",
        constraint: { nullable: true },
      },
      taxzone: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      zone_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      full_address: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
    },
  },
  payrolls: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      burdenRate: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      employeeId: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      employeeType: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      status: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
    },
  },
  job_types: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      job_type_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
    },
  },
  returns: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      status: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      purchaseOrderId: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      jobId: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      returnAmount: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      taxAmount: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
    },
  },
  purchase_order: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      status: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      total: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      tax: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      date: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      requiredOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      sentOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      receivedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      createdOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      modifiedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      invoice_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_invoice_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      project_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_project_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      vendor_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_vendor_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
    },
  },
  sales_details: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      project_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_project_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      job_number: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      soldOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      soldBy: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      soldBy_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      is_active: {
        data_type: "TINYINT",
        constraint: { nullable: true },
      },
      subtotal: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      estimates_age: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      estimates_sold_hours: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      budget_expense: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      budget_hours: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      status_value: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      status_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      createdOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      modifiedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      businessUnitName: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      location_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_location_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      customer_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_customer_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
    },
  },
  projects: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      number: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      status: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      customer_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_customer_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      location_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_location_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      startDate: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      targetCompletionDate: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      actualCompletionDate: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      createdOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      modifiedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      legal_entity_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
    },
  },
  projects_wip_data: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      number: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      status: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      billed_amount: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      balance: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      contract_value: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      sold_contract_value: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      budget_expense: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      budget_hours: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      inventory_bill_amount: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      po_cost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      po_returns: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      equipment_cost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      material_cost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      labor_cost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      labor_hours: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      burden: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      accounts_receivable: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      expense: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      income: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      current_liability: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      membership_liability: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      actual_business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      customer_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      actual_customer_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      location_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      actual_location_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      startDate: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      targetCompletionDate: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      actualCompletionDate: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      createdOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      modifiedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      UTC_update_date: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
    },
  },
  project_managers: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      manager_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_manager_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
    },
  },
  call_details: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      instance_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      job_number: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      project_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_project_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      createdOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      modifiedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      receivedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      duration: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      from: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      to: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      direction: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      call_type: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      customer_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_customer_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      is_customer_active: {
        data_type: "TINYINT",
        constraint: { nullable: true },
      },
      customer_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      street_address: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      street: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      unit: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      city: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      state: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      country: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      zip: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      latitude: {
        data_type: "DECIMAL96",
        constraint: { nullable: true },
      },
      longitude: {
        data_type: "DECIMAL96",
        constraint: { nullable: true },
      },
      customer_import_id: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      customer_type: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      campaign_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      campaign_category: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      campaign_source: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      campaign_medium: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      campaign_dnis: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      campaign_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      campaign_createdOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      campaign_modifiedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      is_campaign_active: {
        data_type: "TINYINT",
        constraint: { nullable: true },
      },
      agent_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      agent_externalId: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      agent_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      business_unit_active: {
        data_type: "TINYINT",
        constraint: { nullable: true },
      },
      business_unit_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      business_unit_official_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      type_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      type_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      type_modifiedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
    },
  },
  job_details: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      job_type_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_job_type_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      job_number: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      job_status: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      job_completion_time: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      location_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_location_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      customer_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_customer_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      project_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_project_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      campaign_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_campaign_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      createdOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      modifiedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      created_by_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      lead_call_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_lead_call_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      booking_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_booking_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      sold_by_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
    },
  },
  appointments: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      appointmentNumber: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      start: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      end: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      arrivalWindowStart: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      arrivalWindowEnd: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      status: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      createdOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      modifiedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      customer_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_customer_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
    },
  },
  vendor: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      is_active: {
        data_type: "TINYINT",
        constraint: { nullable: true },
      },
    },
  },
  inventory_bills: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      purchase_order_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_purchase_order_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      syncStatus: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      referenceNumber: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      vendorNumber: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      billDate: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      billAmount: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      taxAmount: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      shippingAmount: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      createdOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      dueDate: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      vendor_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_vendor_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      job_number: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
    },
  },
  technician: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      is_active: {
        data_type: "TINYINT",
        constraint: { nullable: true },
      },
      team: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
    },
  },
  sku_details: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      sku_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      sku_type: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      sku_unit_price: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      vendor_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_vendor_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
    },
  },
  invoice: {
    columns: {
      id: {
        data_type: "NVARCHAR20",
        constraint: { primary: true, nullable: false },
      },
      syncStatus: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      date: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      dueDate: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      subtotal: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      tax: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      total: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      balance: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      depositedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      createdOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      modifiedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      invoice_type_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      invoice_type_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      project_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_project_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_business_unit_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      location_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_location_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      address_street: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      address_unit: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      address_city: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      address_state: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      address_country: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      address_zip: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      acutal_address_zip: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      customer_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_customer_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      customer_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
    },
  },
  cogs_material: {
    columns: {
      quantity: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      cost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      total_cost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      price: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      sku_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      sku_total: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      generalLedgerAccountid: {
        data_type: "BIGINT",
        constraint: { nullable: true },
      },
      generalLedgerAccountname: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      generalLedgerAccountnumber: {
        data_type: "BIGINT",
        constraint: { nullable: true },
      },
      generalLedgerAccounttype: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      generalLedgerAccountdetailType: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      project_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_project_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      invoice_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      sku_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_sku_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
    },
  },
  cogs_equipment: {
    // invoice api -- items
    columns: {
      quantity: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      cost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      total_cost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      price: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      sku_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      sku_total: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      generalLedgerAccountid: {
        data_type: "BIGINT",
        constraint: { nullable: true },
      },
      generalLedgerAccountname: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      generalLedgerAccountnumber: {
        data_type: "BIGINT",
        constraint: { nullable: true },
      },
      generalLedgerAccounttype: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      generalLedgerAccountdetailType: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      project_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_project_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      invoice_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      sku_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_sku_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
    },
  },
  cogs_service: {
    // invoice api -- items
    columns: {
      quantity: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      cost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      total_cost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      price: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      sku_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      sku_total: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      generalLedgerAccountid: {
        data_type: "BIGINT",
        constraint: { nullable: true },
      },
      generalLedgerAccountname: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      generalLedgerAccountnumber: {
        data_type: "BIGINT",
        constraint: { nullable: true },
      },
      generalLedgerAccounttype: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      generalLedgerAccountdetailType: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      project_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_project_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      invoice_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      sku_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_sku_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
    },
  },
  cogs_labor: {
    columns: {
      paid_duration: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      labor_cost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      activity: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      paid_time_type: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      date: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      startedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      endedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      isPrevailingWageJob: {
        data_type: "TINYINT",
        constraint: { nullable: true },
      },
      job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_job_details_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      invoice_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_invoice_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      project_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_project_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      payrollId: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      acutal_payrollId: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
      technician_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: false },
      },
      actual_technician_id: {
        data_type: "NVARCHAR20",
        constraint: { nullable: true },
      },
    },
  },
};

const hvac_tables_responses = {
  legal_entity: {
    status: "",
  },
  us_cities: {
    status: "",
  },
  business_unit: {
    status: "",
  },
  project_business_unit: {
    status: "",
  },
  employees: {
    status: "",
  },
  campaigns: {
    status: "",
  },
  bookings: {
    status: "",
  },
  customer_details: {
    status: "",
  },
  call_details: {
    status: "",
  },
  location: {
    status: "",
  },
  payrolls: {
    status: "",
  },
  job_types: {
    status: "",
  },
  projects: {
    status: "",
  },
  project_managers: {
    status: "",
  },
  job_details: {
    status: "",
  },
  appointments: {
    status: "",
  },
  sales_details: {
    status: "",
  },
  vendor: {
    status: "",
  },
  technician: {
    status: "",
  },
  sku_details: {
    status: "",
  },
  cogs_material: {
    status: "",
  },
  cogs_equipment: {
    status: "",
  },
  cogs_service: {
    status: "",
  },
  cogs_labor: {
    status: "",
  },
  invoice: {
    status: "",
  },
  purchase_order: {
    status: "",
  },
  inventory_bills: {
    status: "",
  },
  returns: {
    status: "",
  },
};

let start_time = "";
let end_time = "";

const main_api_list = {
  legal_entity: [
    {
      table_name: "legal_entity",
    },
  ],
  us_cities: [
    {
      table_name: "us_cities",
    },
  ],
  business_unit: [
    {
      api_group: "settings",
      api_name: "business-units",
      table_name: "business_unit",
    },
  ],
  employees: [
    {
      api_group: "settings",
      api_name: "employees",
      table_name: "employees",
    },
  ],
  campaigns: [
    {
      api_group: "marketing",
      api_name: "campaigns",
      table_name: "campaigns",
    },
  ],
  bookings: [
    {
      api_group: "crm",
      api_name: "bookings",
      table_name: "bookings",
    },
  ],
  customer_details: [
    {
      api_group: "crm",
      api_name: "customers",
      table_name: "customer_details",
    },
  ],
  location: [
    {
      api_group: "crm",
      api_name: "locations",
      table_name: "location",
    },
  ],
  projects: [
    {
      api_group: "jpm",
      api_name: "projects",
      table_name: "projects",
    },
  ],
  call_details: [
    {
      api_group: "telecom",
      api_name: "calls",
      table_name: "call_details",
    },
  ],
  job_details: [
    {
      api_group: "jpm",
      api_name: "jobs",
      table_name: "job_details",
    },
    {
      api_group: "jpm",
      api_name: "job-types",
      table_name: "job_details",
    },
  ],
  inventory_bills: [
    {
      api_group: "accounting",
      api_name: "export/inventory-bills",
      table_name: "inventory_bills",
    },
  ],
  sales_details: [
    {
      api_group: "sales",
      api_name: "estimates",
      table_name: "sales_details",
    },
  ],
  appointments: [
    {
      api_group: "jpm",
      api_name: "appointments",
      table_name: "appointments",
    },
  ],
  vendor: [
    {
      api_group: "inventory",
      api_name: "vendors",
      table_name: "vendor",
    },
  ],
  technician: [
    {
      api_group: "settings",
      api_name: "technicians",
      table_name: "technician",
    },
  ],
  sku_details: [
    {
      api_group: "pricebook",
      api_name: "materials",
      table_name: "sku_details",
    },
    {
      api_group: "pricebook",
      api_name: "equipment",
      table_name: "sku_details",
    },
    {
      api_group: "pricebook",
      api_name: "services",
      table_name: "sku_details",
    },
  ],
  invoice: [
    {
      api_group: "accounting",
      api_name: "invoices",
      table_name: "invoices",
    },
  ],
  returns: [
    {
      api_group: "inventory",
      api_name: "returns",
      table_name: "returns",
    },
  ],
  purchase_order: [
    {
      api_group: "inventory",
      api_name: "purchase-orders",
      table_name: "purchase_order",
    },
  ],
  cogs_labor: [
    {
      api_group: "payroll",
      api_name: "gross-pay-items",
      table_name: "cogs_labor",
    },
    {
      api_group: "payroll",
      api_name: "payrolls",
      table_name: "cogs_labor",
    },
  ],
};

let unique_us_zip_codes = {};

function startStopwatch(task_name) {
  let startTime = Date.now();
  let running = true;

  let elapsed_time_cache = "";

  const updateStopwatch = () => {
    if (!running) return;

    const elapsedTime = Date.now() - startTime;
    const seconds = Math.floor(elapsedTime / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);

    let formattedTime =
      String(hours).padStart(2, "0") +
      ":" +
      String(minutes % 60).padStart(2, "0") +
      ":" +
      String(seconds % 60).padStart(2, "0") +
      "." +
      String(elapsedTime % 1000).padStart(3, "0");

    // process.stdout.write(`Time elapsed for ${task_name}: ${formattedTime}\r`);

    elapsed_time_cache = formattedTime;

    setTimeout(updateStopwatch, 10); // Update every 10 milliseconds
  };

  updateStopwatch();

  // Function to stop the stopwatch
  function stop() {
    running = false;
    return elapsed_time_cache;
  }

  return stop;
}

async function find_total_records(data_lake) {
  let total_records = 0;

  // console.log("data_lake: ", data_lake);

  Object.keys(data_lake).map((api_name) => {
    total_records = total_records + data_lake[api_name]["data_pool"].length;
  });

  console.log("total_records: ", total_records);
}

async function fetch_main_data(
  data_lake,
  instance_details,
  main_api_list,
  hvac_tables
) {
  // collect all data from all the instance

  await Promise.all(
    instance_details.map(async (instance_data) => {
      const instance_name = instance_data["instance_name"];
      const tenant_id = instance_data["tenant_id"];
      const app_key = instance_data["app_key"];
      const client_id = instance_data["client_id"];
      const client_secret = instance_data["client_secret"];

      await Promise.all(
        Object.keys(main_api_list).map(async (api_key) => {
          if (!data_lake[api_key]) {
            data_lake[api_key] = {};
          }

          if (api_key == "legal_entity") {
            data_lake[api_key] = {
              data_pool: {
                1: { id: 1, legal_name: "EXP" },
                2: { id: 2, legal_name: "PA" },
                3: { id: 3, legal_name: "NMI" },
              },
            };
          } else if (api_key == "us_cities") {
            data_lake[api_key] = {
              zip_codes: {
                data_pool: {},
              },
            };
            us_cities_list.map((city) => {
              const zip_code_index = Number(city["zip_code"]);

              unique_us_zip_codes[zip_code_index] = {
                city: city["city"],
              };

              data_lake[api_key]["zip_codes"]["data_pool"][zip_code_index] = {
                id: Number(city["zip_code"]),
                latitude: String(city["latitude"]),
                longitude: String(city["longitude"]),
                city: city["city"],
                state: city["state"],
                county: city["county"],
              };
            });
          } else {
            const api_list = main_api_list[api_key];

            await Promise.all(
              api_list.map(async (api_data) => {
                const api_group_temp = api_data["api_group"];
                const api_name_temp = api_data["api_name"];

                if (
                  !data_lake[api_key][api_group_temp + "__" + api_name_temp]
                ) {
                  if (api_name_temp == "gross-pay-items") {
                    data_lake[api_key][api_group_temp + "__" + api_name_temp] =
                      {
                        data_pool: [],
                      };
                  } else {
                    data_lake[api_key][api_group_temp + "__" + api_name_temp] =
                      {
                        data_pool: {},
                      };
                  }
                }

                // signing a new access token in Service Titan's API
                let access_token = "";

                do {
                  access_token = await getAccessToken(client_id, client_secret);
                } while (!access_token);

                // continuously fetching whole api data
                let data_pool_object = {};
                let data_pool = [];
                let page_count = 0;
                let has_error_occured = false;

                do {
                  ({
                    data_pool_object,
                    data_pool,
                    page_count,
                    has_error_occured,
                  } = await getAPIWholeData(
                    access_token,
                    app_key,
                    instance_name,
                    tenant_id,
                    api_group_temp,
                    api_name_temp,
                    params_header,
                    data_pool_object,
                    data_pool,
                    page_count
                  ));
                } while (has_error_occured);

                if (api_name_temp == "gross-pay-items") {
                  data_lake[api_key][api_group_temp + "__" + api_name_temp][
                    "data_pool"
                  ] = [
                    ...data_lake[api_key][
                      api_group_temp + "__" + api_name_temp
                    ]["data_pool"],
                    ...data_pool,
                  ];
                } else {
                  data_lake[api_key][api_group_temp + "__" + api_name_temp][
                    "data_pool"
                  ] = {
                    ...data_lake[api_key][
                      api_group_temp + "__" + api_name_temp
                    ]["data_pool"],
                    ...data_pool_object,
                  }; //;
                }
              })
            );
          }
        })
      );
    })
  );
}

async function azure_sql_operations(data_lake, table_list) {
  // creating a client for azure sql database operations
  let sql_request = "";
  do {
    sql_request = await create_sql_connection();
  } while (!sql_request);

  let create_hvac_schema_status = false;
  do {
    create_hvac_schema_status = await create_hvac_schema(sql_request);
  } while (!create_hvac_schema_status);

  // entering into auto update table
  end_time = "0001-01-01T00:00:00.00Z";

  const timeDifferenceInMilliseconds = 0;

  // Convert the time difference to minutes
  const timeDifferenceInMinutes = timeDifferenceInMilliseconds / (1000 * 60);

  // entry into auto_update table
  try {
    const auto_update_query = `INSERT INTO auto_update(
      query_date,
      start_time,
      end_time,
      total_minutes,
      legal_entity,
      us_cities,
      business_unit,
      project_business_unit,
      employees,
      campaigns,
      bookings,
      customer_details,
      call_details,
      [location],
      payrolls,
      job_types,
      projects,
      projects_wip_data,
      project_managers,
      job_details,
      appointments,
      sales_details,
      vendor,
      inventory_bills,
      technician,
      sku_details,
      invoice,
      cogs_material,
      cogs_equipment,
      cogs_service,
      cogs_labor,
      returns,
      purchase_order,
      overall_status)
      OUTPUT INSERTED.id -- Return the inserted ID
      VALUES ('${
        params_header["modifiedBefore"]
      }','${start_time.toISOString()}','${end_time}','${timeDifferenceInMinutes}','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated', 'not yet updated')`;

    // Execute the INSERT query and retrieve the ID
    const result = await sql_request.query(auto_update_query);

    if (result.recordset.length > 0) {
      // Assuming 'id' is the name of the auto-incrementing primary key column
      lastInsertedId = result.recordset[0].id;
      console.log("Last inserted ID:", lastInsertedId);
    } else {
      console.log("No ID retrieved");
    }

    console.log("Auto_Update log created ");
  } catch (err) {
    console.log("Error while inserting into auto_update", err);
  }

  const pushing_time = startStopwatch("pushing data");

  // console.log(
  //   "*************************************************CHECK MEM**********************************************************************"
  // );
  // await new Promise((resolve) => setTimeout(resolve, 15000));
  // console.log(
  //   "***********************************************************************************************************************"
  // );

  await data_processor(data_lake, sql_request, table_list);
  console.log("Time Taken for pushing all data: ", pushing_time());

  // Close the connection pool
  await sql.close();
}

async function data_processor(data_lake, sql_request, table_list) {
  for (let api_count = 0; api_count < table_list.length; api_count++) {
    // Object.keys(data_lake).length
    // table_list.length

    // if (api_count < table_list.length - 1) {
    //   continue;
    // }

    const api_name = table_list[api_count];

    console.log("table_name: ", api_name);

    switch (api_name) {
      case "legal_entity": {
        // entry into auto_update table
        try {
          hvac_tables_responses["legal_entity"]["status"] = "success";
          const auto_update_query = `UPDATE auto_update SET legal_entity = 'no changes' WHERE id=${lastInsertedId}`;
          await sql_request.query(auto_update_query);

          console.log("Auto_Update log created ");
        } catch (err) {
          console.log("Error while inserting into auto_update", err);
        }

        delete data_lake[api_name];

        break;
      }

      case "us_cities": {
        // entry into auto_update table
        try {
          hvac_tables_responses["us_cities"]["status"] = "success";
          const auto_update_query = `UPDATE auto_update SET us_cities = 'no changes' WHERE id=${lastInsertedId}`;

          await sql_request.query(auto_update_query);

          console.log("Auto_Update log created ");
        } catch (err) {
          console.log("Error while inserting into auto_update", err);
        }

        break;
      }

      case "business_unit": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool =
          data_lake[api_name]["settings__business-units"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];
        const project_business_unit_header_data =
          hvac_tables["project_business_unit"]["columns"];

        let final_data_pool = [];
        let project_business_unit_final_data_pool = [];

        // processing campaingns data for pushing into db
        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];

          // console.log("id: ", record["id"]);
          // console.log("Acc type", kpi_data[record["id"]]["Account Type"]);
          // console.log("Trade type", kpi_data[record["id"]]["Trade Type"]);

          let trade_type = "DEF";
          let segment_type = "DEF";
          let revenue_type = "DEF";
          let business = "DEF";
          let business_unit_official_name = "DEF";
          let business_unit_name = "DEF";

          // let legal_entity_id = record["instance_id"];
          const instance_code = {
            1: "EXP",
            2: "PA",
            3: "NMI",
          };

          if (kpi_data[record["id"]]) {
            trade_type = kpi_data[record["id"]]["Trade"]
              ? kpi_data[record["id"]]["Trade"]
              : "DEF";
            segment_type = kpi_data[record["id"]]["Segment"]
              ? kpi_data[record["id"]]["Segment"]
              : "DEF";
            revenue_type = kpi_data[record["id"]]["Type"]
              ? kpi_data[record["id"]]["Type"]
              : "DEF";
            // business = kpi_data[record["id"]]["Business"]
            //   ? kpi_data[record["id"]]["Business"]
            //   : "DEF";
            business_unit_official_name = kpi_data[record["id"]]["Name"]
              ? kpi_data[record["id"]]["Name"]
              : "DEF";
            business_unit_name = kpi_data[record["id"]]["Invoice Business Unit"]
              ? kpi_data[record["id"]]["Invoice Business Unit"]
              : "DEF";
            let legal_entity_group = kpi_data[record["id"]]["Business"]
              ? kpi_data[record["id"]]["Business"]
              : "DEF";

            // if (legal_entity_group != "DEF") {
            //   legal_entity_id = instance_code[legal_entity_group];
            // } else {
            //   legal_entity_id = record["instance_id"];
            // }

            business = instance_code[record["instance_id"]];
          }

          final_data_pool.push({
            id: record["id"],
            business_unit_name: business_unit_name,
            business_unit_official_name: business_unit_official_name,
            trade_type: trade_type,
            segment_type: segment_type,
            revenue_type: revenue_type,
            business: business,
            is_active: record["active"] ? 1 : 0,
            legal_entity_id: record["instance_id"],
          });

          project_business_unit_final_data_pool.push({
            id: record["id"],
            business_unit_name: business_unit_name,
            business_unit_official_name: business_unit_official_name,
            trade_type: trade_type,
            segment_type: segment_type,
            revenue_type: revenue_type,
            business: business,
            is_active: record["active"] ? 1 : 0,
            legal_entity_id: record["instance_id"],
          });
        });

        console.log("business unit data: ", final_data_pool.length);

        // console.log("final data pool", final_data_pool);
        // await hvac_flat_data_insertion(
        //   sql_request,
        //   final_data_pool,
        //   header_data,
        //   table_name
        // );

        if (final_data_pool.length > 0) {
          // await csv_generator(final_data_pool, header_data, table_name);

          do {
            hvac_tables_responses["business_unit"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (
            hvac_tables_responses["business_unit"]["status"] != "success"
          );

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET business_unit = '${hvac_tables_responses["business_unit"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["business_unit"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET business_unit = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        if (project_business_unit_final_data_pool.length > 0) {
          do {
            hvac_tables_responses["project_business_unit"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                project_business_unit_final_data_pool,
                project_business_unit_header_data,
                "project_business_unit"
              );
          } while (
            hvac_tables_responses["project_business_unit"]["status"] !=
            "success"
          );

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET project_business_unit = '${hvac_tables_responses["project_business_unit"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["project_business_unit"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET project_business_unit = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        delete data_lake[api_name];

        break;
      }

      case "employees": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool =
          data_lake[api_name]["settings__employees"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        const batchSize = 50;

        for (let i = 0; i < Object.keys(data_pool).length; i = i + batchSize) {
          // processing campaingns data for pushing into db
          await Promise.all(
            Object.keys(data_pool)
              .slice(i, i + batchSize)
              .map(async (record_id) => {
                const record = data_pool[record_id];

                let business_unit_id = record["instance_id"];
                let actual_business_unit_id = record["businessUnitId"]
                  ? record["businessUnitId"]
                  : record["instance_id"];
                if (record["businessUnit"]) {
                  // checking business unit availlable or not for mapping
                  const is_business_unit_available = await sql_request.query(
                    `SELECT id FROM business_unit WHERE id='${record["businessUnitId"]}'`
                  );

                  if (is_business_unit_available["recordset"].length > 0) {
                    business_unit_id = record["businessUnit"];
                  }
                }

                final_data_pool.push({
                  id: record["id"],
                  name: record["name"] ? record["name"] : "default",
                  role: record["role"] ? record["role"] : "default",
                  business_unit_id: business_unit_id,
                  actual_business_unit_id: actual_business_unit_id,
                });
              })
          );
        }

        console.log("employees data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          do {
            hvac_tables_responses["employees"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (hvac_tables_responses["employees"]["status"] != "success");

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET employees = '${hvac_tables_responses["employees"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["employees"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET employees = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        delete data_lake[api_name];

        break;
      }

      case "campaigns": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool =
          data_lake[api_name]["marketing__campaigns"]["data_pool"];

        const header_data = hvac_tables[table_name]["columns"];

        const final_data_pool = [];

        const batchSize = 50;

        for (let i = 0; i < Object.keys(data_pool).length; i = i + batchSize) {
          // processing campaingns data for pushing into db
          await Promise.all(
            Object.keys(data_pool)
              .slice(i, i + batchSize)
              .map(async (record_id) => {
                const record = data_pool[record_id];
                let createdOn = "2000-01-01T00:00:00.00Z";
                let modifiedOn = "2000-01-01T00:00:00.00Z";

                if (record["createdOn"]) {
                  if (
                    new Date(record["createdOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    createdOn = record["createdOn"];
                  }
                } else {
                  createdOn = "2001-01-01T00:00:00.00Z";
                }

                if (record["modifiedOn"]) {
                  if (
                    new Date(record["modifiedOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    modifiedOn = record["modifiedOn"];
                  }
                } else {
                  modifiedOn = "2001-01-01T00:00:00.00Z";
                }

                let category_id = "0";
                let category_name = "default";
                let is_category_active = 0;
                if (record["category"]) {
                  category_id = record["category"]["id"]
                    ? record["category"]["id"]
                    : 0;
                  category_name = record["category"]["name"]
                    ? record["category"]["name"]
                    : "default";
                  is_category_active = record["category"]["active"] ? 1 : 0;
                }

                let business_unit_id = record["instance_id"];
                let actual_business_unit_id = record["instance_id"];
                if (record["businessUnit"] && record["businessUnit"]["id"]) {
                  actual_business_unit_id = record["businessUnit"]["id"]
                    ? record["businessUnit"]["id"]
                    : record["instance_id"];

                  // checking business unit availlable or not for mapping
                  const is_business_unit_available = await sql_request.query(
                    `SELECT id FROM business_unit WHERE id='${record["businessUnit"]["id"]}'`
                  );

                  if (is_business_unit_available["recordset"].length > 0) {
                    business_unit_id = record["businessUnit"]["id"];
                  }
                }

                final_data_pool.push({
                  id: record["id"],
                  name: record["name"] ? record["name"] : "default",
                  is_active: record["active"] ? 1 : 0,
                  createdOn: createdOn,
                  modifiedOn: modifiedOn,
                  category_id: category_id,
                  category_name: category_name,
                  is_category_active: is_category_active,
                  source: record["source"] ? record["source"] : "default",
                  medium: record["medium"] ? record["medium"] : "default",
                  business_unit_id: business_unit_id,
                  actual_business_unit_id: actual_business_unit_id,
                });
              })
          );
        }

        console.log("campaigns data: ", final_data_pool.length);

        // console.log("final data pool", final_data_pool);
        // await hvac_flat_data_insertion(
        //   sql_request,
        //   final_data_pool,
        //   header_data,
        //   table_name
        // );

        if (final_data_pool.length > 0) {
          // await csv_generator(final_data_pool, header_data, table_name);

          do {
            hvac_tables_responses["campaigns"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (hvac_tables_responses["campaigns"]["status"] != "success");

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET campaigns = '${hvac_tables_responses["campaigns"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["campaigns"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET campaigns = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        delete data_lake[api_name];

        break;
      }

      case "bookings": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool = data_lake[api_name]["crm__bookings"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        const batchSize = 100;

        for (let i = 0; i < Object.keys(data_pool).length; i = i + batchSize) {
          await Promise.all(
            Object.keys(data_pool)
              .slice(i, i + batchSize)
              .map(async (record_id) => {
                const record = data_pool[record_id];

                let start = "2000-01-01T00:00:00.00Z";

                if (record["start"]) {
                  if (
                    new Date(record["start"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    start = record["start"];
                  }
                } else {
                  start = "2001-01-01T00:00:00.00Z";
                }

                let createdOn = "2000-01-01T00:00:00.00Z";

                if (record["createdOn"]) {
                  if (
                    new Date(record["createdOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    createdOn = record["createdOn"];
                  }
                } else {
                  createdOn = "2001-01-01T00:00:00.00Z";
                }

                let modifiedOn = "2000-01-01T00:00:00.00Z";

                if (record["modifiedOn"]) {
                  if (
                    new Date(record["modifiedOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    modifiedOn = record["modifiedOn"];
                  }
                } else {
                  modifiedOn = "2001-01-01T00:00:00.00Z";
                }

                let address_street = "default";
                let address_unit = "default";
                let address_city = "default";
                let address_state = "default";
                let address_zip = "default";
                let address_country = "default";
                if (record["address"]) {
                  address_street = record["address"]["street"]
                    ? record["address"]["street"]
                    : "default";
                  address_unit = record["address"]["unit"]
                    ? record["address"]["unit"]
                    : "default";
                  address_city = record["address"]["city"]
                    ? record["address"]["city"]
                    : "default";
                  address_state = record["address"]["state"]
                    ? record["address"]["state"]
                    : "default";
                  address_zip = record["address"]["zip"]
                    ? record["address"]["zip"]
                    : "default";
                  address_country = record["address"]["country"]
                    ? record["address"]["country"]
                    : "default";
                }

                let business_unit_id = record["instance_id"];
                let actual_business_unit_id = record["businessUnitId"]
                  ? record["businessUnitId"]
                  : record["instance_id"];

                if (record["businessUnitId"]) {
                  // checking business unit availlable or not for mapping
                  const is_business_unit_available = await sql_request.query(
                    `SELECT id FROM business_unit WHERE id='${record["businessUnitId"]}'`
                  );

                  if (is_business_unit_available["recordset"].length > 0) {
                    business_unit_id = record["businessUnitId"];
                  }
                }

                let campaign_id = record["instance_id"];
                let actual_campaign_id = record["campaignId"]
                  ? record["campaignId"]
                  : record["instance_id"];

                if (record["campaignId"]) {
                  // checking campaignId availlable or not for mapping
                  const is_campaigns_available = await sql_request.query(
                    `SELECT id FROM campaigns WHERE id='${record["campaignId"]}'`
                  );

                  if (is_campaigns_available["recordset"].length > 0) {
                    campaign_id = record["campaignId"];
                  }
                }

                final_data_pool.push({
                  id: record["id"],
                  name: record["name"] ? record["name"] : "default",
                  source: record["source"] ? record["source"] : "default",
                  status: record["status"] ? record["status"] : "default",
                  customer_type: record["customerType"]
                    ? record["customerType"]
                    : "default",
                  start: start,
                  bookingProviderId: record["bookingProviderId"]
                    ? record["bookingProviderId"]
                    : record["instance_id"],
                  createdOn: createdOn,
                  modifiedOn: modifiedOn,
                  address_street: address_street,
                  address_unit: address_unit,
                  address_city: address_city,
                  address_state: address_state,
                  address_zip: address_zip,
                  address_country: address_country,
                  business_unit_id: business_unit_id,
                  actual_business_unit_id: actual_business_unit_id,
                  campaign_id: campaign_id,
                  actual_campaign_id: actual_campaign_id,
                  job_details_id: record["jobId"]
                    ? record["jobId"]
                    : record["instance_id"],
                });
              })
          );
        }

        console.log("bookings data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          // await csv_generator(final_data_pool, header_data, table_name);

          do {
            hvac_tables_responses["bookings"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (hvac_tables_responses["bookings"]["status"] != "success");

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET bookings = '${hvac_tables_responses["bookings"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["bookings"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET bookings = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        delete data_lake[api_name];

        break;
      }

      case "customer_details": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool = data_lake[api_name]["crm__customers"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];

          let creation_date = "2000-01-01T00:00:00.00Z";

          if (record["createdOn"]) {
            if (
              new Date(record["createdOn"]) >
              new Date("2000-01-01T00:00:00.00Z")
            ) {
              creation_date = record["createdOn"];
            }
          } else {
            creation_date = "2001-01-01T00:00:00.00Z";
          }

          let address_street = "default";
          let address_unit = "default";
          let address_city = "default";
          let address_state = "default";
          let address_zip = "default";
          if (record["address"]) {
            address_street = record["address"]["street"]
              ? record["address"]["street"]
              : "default";
            address_unit = record["address"]["unit"]
              ? record["address"]["unit"]
              : "default";
            address_city = record["address"]["city"]
              ? record["address"]["city"]
              : "default";
            address_state = record["address"]["state"]
              ? record["address"]["state"]
              : "default";
            address_zip = record["address"]["zip"]
              ? record["address"]["zip"]
              : "default";
          }

          final_data_pool.push({
            id: record["id"],
            name: record["name"] ? record["name"] : "default",
            is_active: record["active"] ? 1 : 0,
            type: record["type"] ? record["type"] : "default",
            creation_date: creation_date,
            address_street: address_street,
            address_unit: address_unit,
            address_city: address_city,
            address_state: address_state,
            address_zip: address_zip,
          });
        });

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   final_data_pool,
        //   header_data,
        //   table_name
        // );

        console.log("customer details data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          // await csv_generator(final_data_pool, header_data, table_name);

          do {
            hvac_tables_responses["customer_details"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (
            hvac_tables_responses["customer_details"]["status"] != "success"
          );

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET customer_details = '${hvac_tables_responses["customer_details"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["customer_details"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET customer_details = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        delete data_lake[api_name];

        break;
      }

      case "location": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool = data_lake[api_name]["crm__locations"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        // console.log("data_pool: ", data_pool);
        // console.log("header_data: ", header_data);

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];

          let address_street = "";
          let address_unit = "";
          let address_city = "";
          let address_state = "";
          let address_zip = 57483;
          let acutal_address_zip = "";
          let address_country = "";
          let latitude = 0.0;
          let longitude = 0.0;
          let city = "Mexico";
          if (record["address"]) {
            address_street = record["address"]["street"]
              ? record["address"]["street"]
              : "";
            address_unit = record["address"]["unit"]
              ? record["address"]["unit"]
              : "";
            address_city = record["address"]["city"]
              ? record["address"]["city"]
              : "";
            address_state = record["address"]["state"]
              ? record["address"]["state"]
              : "";
            address_country = record["address"]["country"]
              ? record["address"]["country"]
              : "";
            acutal_address_zip = record["address"]["zip"]
              ? record["address"]["zip"]
              : "";

            address_zip = record["address"]["zip"]
              ? record["address"]["zip"]
              : 57483;

            latitude = record["address"]["latitude"]
              ? record["address"]["latitude"]
              : 0.0;

            longitude = record["address"]["longitude"]
              ? record["address"]["longitude"]
              : 0.0;

            if (typeof address_zip == "string") {
              const numericValue = Number(address_zip.split("-")[0]);
              if (!isNaN(numericValue)) {
                // If the conversion is successful and numericValue is not NaN, update address_zip
                address_zip = numericValue;
              } else {
                // Handle the case where the conversion fails
                address_zip = 57483;
              }
            }

            if (!unique_us_zip_codes[String(address_zip)]) {
              address_zip = 57483;
            } else {
              city = unique_us_zip_codes[String(address_zip)]["city"];
            }
          }

          const full_address = [
            address_street,
            address_city,
            address_state,
            address_zip,
          ]
            .map((address_data) => {
              if (address_data) {
                return address_data;
              }
            })
            .join(" ");

          final_data_pool.push({
            id: record["id"],
            name: record["name"] ? record["name"] : "default",
            street: address_street,
            unit: address_unit,
            city: address_city,
            state: address_state,
            country: address_country,
            address_zip: address_zip,
            acutal_address_zip: acutal_address_zip,
            latitude: latitude,
            longitude: longitude,
            taxzone: record["taxZoneId"]
              ? record["taxZoneId"]
              : record["instance_id"],
            zone_id: record["zoneId"]
              ? record["zoneId"]
              : record["instance_id"],
            full_address: full_address,
          });
        });

        // console.log("final_data_pool: ", final_data_pool);
        // console.log("header_data: ", header_data);

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   final_data_pool,
        //   header_data,
        //   table_name
        // );

        console.log("location data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          // await csv_generator(final_data_pool, header_data, table_name);

          do {
            hvac_tables_responses["location"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (hvac_tables_responses["location"]["status"] != "success");

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET location = '${hvac_tables_responses["location"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["location"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET location = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        delete data_lake[api_name];

        break;
      }

      case "payrolls": {
        const table_name = "payrolls";
        const data_pool =
          data_lake["cogs_labor"]["payroll__payrolls"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        // console.log("data_pool: ", data_pool);
        // console.log("header_data: ", header_data);

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];

          final_data_pool.push({
            id: record["id"],
            burdenRate: record["burdenRate"] ? record["burdenRate"] : 0,
            employeeId: record["employeeId"]
              ? record["employeeId"]
              : record["instance_id"],
            employeeType: record["employeeType"],
            status: record["status"],
          });
        });

        // console.log("final_data_pool: ", final_data_pool);
        // console.log("header_data: ", header_data);

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   final_data_pool,
        //   header_data,
        //   table_name
        // );

        console.log("payrolls data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          // await csv_generator(final_data_pool, header_data, table_name);

          do {
            hvac_tables_responses["payrolls"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (hvac_tables_responses["payrolls"]["status"] != "success");

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET payrolls = '${hvac_tables_responses["payrolls"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["payrolls"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET payrolls = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        // delete data_lake["cogs_labor"]["payroll__payrolls"];

        break;
      }

      case "job_types": {
        const table_name = "job_types";
        const data_pool =
          data_lake["job_details"]["jpm__job-types"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        // console.log("data_pool: ", data_pool);
        // console.log("header_data: ", header_data);

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];

          final_data_pool.push({
            id: record["id"],
            job_type_name: record["name"],
          });
        });

        // console.log("final_data_pool: ", final_data_pool);
        // console.log("header_data: ", header_data);

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   final_data_pool,
        //   header_data,
        //   table_name
        // );

        console.log("job_types data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          // await csv_generator(final_data_pool, header_data, table_name);

          do {
            hvac_tables_responses["job_types"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (hvac_tables_responses["job_types"]["status"] != "success");

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET job_types = '${hvac_tables_responses["job_types"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["job_types"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET job_types = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        delete data_lake["job_details"]["jpm__job-types"];

        break;
      }

      case "returns": {
        const table_name = "returns";
        const data_pool =
          data_lake["returns"]["inventory__returns"]["data_pool"];

        const header_data = hvac_tables[table_name]["columns"];

        const returns_final_data_pool = [];

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];

          const returnAmount = record["returnAmount"]
            ? parseFloat(record["returnAmount"])
            : 0;

          const taxAmount = record["taxAmount"]
            ? parseFloat(record["taxAmount"])
            : 0;

          returns_final_data_pool.push({
            id: record["id"],
            status: record["status"] ? record["status"] : "default",
            purchaseOrderId: record["purchaseOrderId"]
              ? record["purchaseOrderId"]
              : record["instance_id"],
            jobId: record["jobId"] ? record["jobId"] : record["instance_id"],
            returnAmount: returnAmount,
            taxAmount: taxAmount,
          });
        });

        console.log(
          "returns_final_data_pool order data: ",
          returns_final_data_pool.length
        );

        if (returns_final_data_pool.length > 0) {
          do {
            hvac_tables_responses["returns"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                returns_final_data_pool,
                header_data,
                table_name
              );
          } while (hvac_tables_responses["returns"]["status"] != "success");

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET returns = '${hvac_tables_responses["returns"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["returns"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET returns = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        break;
      }

      case "purchase_order": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const purchase_order_data_pool =
          data_lake[api_name]["inventory__purchase-orders"]["data_pool"];

        const header_data = hvac_tables[table_name]["columns"];

        const final_data_pool = [];

        const batchSize = 100;

        // console.log("purchase_order_data_pool: ", purchase_order_data_pool);

        for (
          let i = 0;
          i < Object.keys(purchase_order_data_pool).length;
          i += batchSize
        ) {
          await Promise.all(
            Object.keys(purchase_order_data_pool)
              .slice(i, i + batchSize)
              .map(async (record_id) => {
                const po_record = purchase_order_data_pool[record_id];

                let job_details_id = po_record["instance_id"];
                let actual_job_details_id = po_record["instance_id"];
                if (po_record["jobId"]) {
                  actual_job_details_id = po_record["jobId"];

                  // checking jobId availlable or not for mapping
                  const is_jobs_available = await sql_request.query(
                    `SELECT id FROM job_details WHERE id='${po_record["jobId"]}'`
                  );

                  if (is_jobs_available["recordset"].length > 0) {
                    job_details_id = po_record["jobId"];
                  }
                }

                let invoice_id = po_record["instance_id"];
                let actual_invoice_id = po_record["instance_id"];
                if (po_record["invoiceId"]) {
                  actual_invoice_id = po_record["invoiceId"];

                  // checking invoice availlable or not for mapping
                  const is_invoice_available = await sql_request.query(
                    `SELECT id FROM invoice WHERE id='${po_record["invoiceId"]}'`
                  );

                  if (is_invoice_available["recordset"].length > 0) {
                    invoice_id = po_record["invoiceId"];
                  }
                }

                let project_id = po_record["instance_id"];
                let actual_project_id = po_record["instance_id"];
                if (po_record["projectId"]) {
                  actual_project_id = po_record["projectId"];
                  // checking projects availlable or not for mapping
                  const is_project_available = await sql_request.query(
                    `SELECT id FROM projects WHERE id='${po_record["projectId"]}'`
                  );

                  if (is_project_available["recordset"].length > 0) {
                    project_id = po_record["projectId"];
                  }
                }

                let vendor_id = po_record["instance_id"];
                let actual_vendor_id = po_record["instance_id"];
                if (po_record["vendorId"]) {
                  actual_vendor_id = po_record["vendorId"];
                  // checking vendor availlable or not for mapping
                  const is_vendor_available = await sql_request.query(
                    `SELECT id FROM vendor WHERE id='${po_record["vendorId"]}'`
                  );

                  if (is_vendor_available["recordset"].length > 0) {
                    vendor_id = po_record["vendorId"];
                  }
                }

                let date = "2000-01-01T00:00:00.00Z";

                if (po_record["date"]) {
                  if (
                    new Date(po_record["date"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    date = po_record["date"];
                  }
                } else {
                  date = "2001-01-01T00:00:00.00Z";
                }

                let requiredOn = "2000-01-01T00:00:00.00Z";

                if (po_record["requiredOn"]) {
                  if (
                    new Date(po_record["requiredOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    requiredOn = po_record["requiredOn"];
                  }
                } else {
                  requiredOn = "2001-01-01T00:00:00.00Z";
                }

                let sentOn = "2000-01-01T00:00:00.00Z";

                if (po_record["sentOn"]) {
                  if (
                    new Date(po_record["sentOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    sentOn = po_record["sentOn"];
                  }
                } else {
                  sentOn = "2001-01-01T00:00:00.00Z";
                }

                let receivedOn = "2000-01-01T00:00:00.00Z";

                if (po_record["receivedOn"]) {
                  if (
                    new Date(po_record["receivedOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    receivedOn = po_record["receivedOn"];
                  }
                } else {
                  receivedOn = "2001-01-01T00:00:00.00Z";
                }

                let createdOn = "2000-01-01T00:00:00.00Z";

                if (po_record["createdOn"]) {
                  if (
                    new Date(po_record["createdOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    createdOn = po_record["createdOn"];
                  }
                } else {
                  createdOn = "2001-01-01T00:00:00.00Z";
                }

                let modifiedOn = "2000-01-01T00:00:00.00Z";

                if (po_record["modifiedOn"]) {
                  if (
                    new Date(po_record["modifiedOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    modifiedOn = po_record["modifiedOn"];
                  }
                } else {
                  modifiedOn = "2001-01-01T00:00:00.00Z";
                }

                final_data_pool.push({
                  id: po_record["id"],
                  status: po_record["status"] ? po_record["status"] : "default",
                  total: po_record["total"] ? po_record["total"] : 0,
                  tax: po_record["tax"] ? po_record["tax"] : 0,
                  date: date,
                  requiredOn: requiredOn,
                  sentOn: sentOn,
                  receivedOn: receivedOn,
                  createdOn: createdOn,
                  modifiedOn: modifiedOn,
                  job_details_id: job_details_id,
                  actual_job_details_id: actual_job_details_id,
                  invoice_id: invoice_id,
                  actual_invoice_id: actual_invoice_id,
                  project_id: project_id,
                  actual_project_id: actual_project_id,
                  vendor_id: vendor_id,
                  actual_vendor_id: actual_vendor_id,
                });
              })
          );
        }

        // console.log("final_data_pool: ", final_data_pool);
        // console.log("header_data: ", header_data);

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   final_data_pool,
        //   header_data,
        //   table_name
        // );

        // console.log("final_data_pool: ", final_data_pool);

        console.log("purchase order data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          // await csv_generator(final_data_pool, header_data, table_name);

          do {
            hvac_tables_responses["purchase_order"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (
            hvac_tables_responses["purchase_order"]["status"] != "success"
          );

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET purchase_order = '${hvac_tables_responses["purchase_order"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["purchase_order"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET purchase_order = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        break;
      }

      case "sales_details": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool = data_lake[api_name]["sales__estimates"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        const batchSize = 100;

        for (let i = 0; i < Object.keys(data_pool).length; i += batchSize) {
          await Promise.all(
            Object.keys(data_pool)
              .slice(i, i + batchSize)
              .map(async (record_id) => {
                const record = data_pool[record_id];

                let project_id = record["instance_id"];
                let actual_project_id = record["projectId"]
                  ? record["projectId"]
                  : record["instance_id"];

                if (record["projectId"]) {
                  // checking projects availlable or not for mapping
                  const is_project_available = await sql_request.query(
                    `SELECT id FROM projects WHERE id='${record["projectId"]}'`
                  );

                  if (is_project_available["recordset"].length > 0) {
                    project_id = record["projectId"];
                  }
                }

                let business_unit_id = record["instance_id"];
                let acutal_business_unit_id = record["businessUnitId"]
                  ? record["businessUnitId"]
                  : record["instance_id"];
                let businessUnitName = record["businessUnitName"]
                  ? record["businessUnitName"]
                  : "default";

                if (record["businessUnitId"]) {
                  // checking business unit availlable or not for mapping
                  const is_business_unit_available = await sql_request.query(
                    `SELECT id FROM business_unit WHERE id='${record["businessUnitId"]}'`
                  );

                  if (is_business_unit_available["recordset"].length > 0) {
                    business_unit_id = record["businessUnitId"];
                  }
                }

                let job_details_id = record["instance_id"];
                let actual_job_details_id = record["jobId"]
                  ? record["jobId"]
                  : record["instance_id"];

                if (record["jobId"]) {
                  // checking jobId availlable or not for mapping
                  const is_jobs_available = await sql_request.query(
                    `SELECT id FROM job_details WHERE id='${record["jobId"]}'`
                  );

                  if (is_jobs_available["recordset"].length > 0) {
                    job_details_id = record["jobId"];
                  }
                }

                let location_id = record["instance_id"];
                let actual_location_id = record["locationId"]
                  ? record["locationId"]
                  : record["instance_id"];

                if (record["locationId"]) {
                  // checking location availlable or not for mapping
                  const is_location_available = await sql_request.query(
                    `SELECT id FROM location WHERE id='${record["locationId"]}'`
                  );

                  if (is_location_available["recordset"].length > 0) {
                    location_id = record["locationId"];
                    actual_location_id = record["locationId"];
                  }
                }

                let customer_details_id = record["instance_id"];
                let actual_customer_details_id = record["customerId"]
                  ? record["customerId"]
                  : record["instance_id"];

                if (record["customerId"]) {
                  // checking customer availlable or not for mapping
                  const is_customer_available = await sql_request.query(
                    `SELECT id FROM customer_details WHERE id='${record["customerId"]}'`
                  );

                  if (is_customer_available["recordset"].length > 0) {
                    customer_details_id = record["customerId"];
                  }
                }

                let soldBy_name = "default";
                let soldBy = record["instance_id"];

                // checking soldBy_name in db
                if (record["soldBy"]) {
                  const is_employee_available = await sql_request.query(
                    `SELECT name,id FROM employees WHERE id='${record["soldBy"]}'`
                  );

                  if (is_employee_available["recordset"].length > 0) {
                    soldBy_name = is_employee_available["recordset"][0]["name"];

                    soldBy = is_employee_available["recordset"][0]["id"];
                  }
                }

                let soldOn = "2000-01-01T00:00:00.00Z";

                if (record["soldOn"]) {
                  if (
                    new Date(record["soldOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    soldOn = record["soldOn"];
                  }
                } else {
                  soldOn = "2001-01-01T00:00:00.00Z";
                }

                let createdOn = "2000-01-01T00:00:00.00Z";
                let estimates_age = 0;
                if (record["createdOn"]) {
                  if (
                    new Date(record["createdOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    createdOn = record["createdOn"];

                    const created_on_date = new Date(record["createdOn"]);
                    const today = new Date();

                    const timeDifference =
                      today.getTime() - created_on_date.getTime();

                    // Convert the difference to days
                    estimates_age = Math.floor(
                      timeDifference / (1000 * 60 * 60 * 24)
                    );
                  }
                } else {
                  createdOn = "2001-01-01T00:00:00.00Z";
                }

                let modifiedOn = "2000-01-01T00:00:00.00Z";
                if (record["modifiedOn"]) {
                  if (
                    new Date(record["modifiedOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    modifiedOn = record["modifiedOn"];
                  }
                } else {
                  modifiedOn = "2001-01-01T00:00:00.00Z";
                }

                let status_value = 0;
                let status_name = "default";
                if (record["status"]) {
                  status_value = record["status"]["value"]
                    ? record["status"]["value"]
                    : 0;
                  status_name = record["status"]["name"]
                    ? record["status"]["name"]
                    : "default";
                }

                let totalCost = 0;
                let budget_hours = 0;
                let estimates_sold_hours = 0;
                record["items"].map((items_record) => {
                  totalCost = totalCost + parseFloat(items_record["totalCost"]);

                  // budget_hours
                  if (items_record["sku"]) {
                    if (items_record["sku"]["name"] == "Labor") {
                      budget_hours += parseFloat(items_record["qty"]);
                    }

                    estimates_sold_hours += parseFloat(
                      items_record["sku"]["soldHours"]
                        ? items_record["sku"]["soldHours"]
                        : 0
                    );
                  }
                });

                // console.log(totalCost, budget_hours);

                final_data_pool.push({
                  id: record["id"],
                  name: record["name"] ? record["name"] : "default",
                  project_id: project_id,
                  actual_project_id: actual_project_id,
                  job_number: record["jobNumber"]
                    ? record["jobNumber"]
                    : "default",
                  soldOn: soldOn,
                  soldBy: soldBy,
                  soldBy_name: soldBy_name,
                  is_active: record["active"] ? 1 : 0,
                  subtotal: record["subtotal"] ? record["subtotal"] : 0,
                  estimates_age: estimates_age,
                  estimates_sold_hours: estimates_sold_hours,
                  budget_expense: totalCost,
                  budget_hours: budget_hours,
                  status_value: status_value,
                  status_name: status_name,
                  createdOn: createdOn,
                  modifiedOn: modifiedOn,
                  business_unit_id: business_unit_id,
                  acutal_business_unit_id: acutal_business_unit_id,
                  businessUnitName: businessUnitName,
                  job_details_id: job_details_id,
                  actual_job_details_id: actual_job_details_id,
                  location_id: location_id,
                  actual_location_id: actual_location_id,
                  customer_details_id: customer_details_id,
                  actual_customer_details_id: actual_customer_details_id,
                });
              })
          );
        }

        console.log("sales_details data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          // await csv_generator(final_data_pool, header_data, table_name);

          do {
            hvac_tables_responses["sales_details"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (
            hvac_tables_responses["sales_details"]["status"] != "success"
          );

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET sales_details = '${hvac_tables_responses["sales_details"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["sales_details"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET sales_details = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        delete data_lake[api_name];

        break;
      }

      case "projects": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool = data_lake[api_name]["jpm__projects"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];
        const project_managers_header_data =
          hvac_tables["project_managers"]["columns"];

        let final_data_pool = [];
        let project_managers_final_data_pool = [];

        batchSize = 5;

        console.log("projects total data: ", Object.keys(data_pool).length);

        for (let i = 0; i < Object.keys(data_pool).length; i = i + batchSize) {
          await Promise.all(
            Object.keys(data_pool)
              .slice(i, i + batchSize)
              .map(async (record_id) => {
                const record = data_pool[record_id];

                let customer_details_id = record["instance_id"];
                let actual_customer_details_id = record["customerId"]
                  ? record["customerId"]
                  : record["instance_id"];

                if (record["customerId"]) {
                  // checking customer availlable or not for mapping
                  const is_customer_available = await sql_request.query(
                    `SELECT id FROM customer_details WHERE id='${record["customerId"]}'`
                  );

                  if (is_customer_available["recordset"].length > 0) {
                    customer_details_id = record["customerId"];
                  }
                }

                let location_id = record["instance_id"];
                let actual_location_id = record["locationId"]
                  ? record["locationId"]
                  : record["instance_id"];

                if (record["locationId"]) {
                  // checking location availlable or not for mapping
                  const is_location_available = await sql_request.query(
                    `SELECT id FROM location WHERE id='${record["locationId"]}'`
                  );

                  if (is_location_available["recordset"].length > 0) {
                    location_id = record["locationId"];
                  }
                }

                let startDate = "2000-01-01T00:00:00.00Z";

                if (record["startDate"]) {
                  if (
                    new Date(record["startDate"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    startDate = record["startDate"];
                  }
                } else {
                  startDate = "2001-01-01T00:00:00.00Z";
                }

                let targetCompletionDate = "2000-01-01T00:00:00.00Z";

                if (record["targetCompletionDate"]) {
                  if (
                    new Date(record["targetCompletionDate"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    targetCompletionDate = record["targetCompletionDate"];
                  }
                } else {
                  targetCompletionDate = "2001-01-01T00:00:00.00Z";
                }

                let actualCompletionDate = "2000-01-01T00:00:00.00Z";

                if (record["actualCompletionDate"]) {
                  if (
                    new Date(record["actualCompletionDate"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    actualCompletionDate = record["actualCompletionDate"];
                  }
                } else {
                  actualCompletionDate = "2001-01-01T00:00:00.00Z";
                }

                let createdOn = "2000-01-01T00:00:00.00Z";

                if (record["createdOn"]) {
                  if (
                    new Date(record["createdOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    createdOn = record["createdOn"];
                  }
                } else {
                  createdOn = "2001-01-01T00:00:00.00Z";
                }

                let modifiedOn = "2000-01-01T00:00:00.00Z";
                if (record["modifiedOn"]) {
                  if (
                    new Date(record["modifiedOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    modifiedOn = record["modifiedOn"];
                  }
                } else {
                  modifiedOn = "2001-01-01T00:00:00.00Z";
                }

                // project_manager id deletion
                const project_managers_deleting_query = await sql_request.query(
                  `DELETE FROM project_managers WHERE id = '${record["id"]}'`
                );

                // project_manager id insertion
                await Promise.all(
                  record["projectManagerIds"].map(async (manager_id) => {
                    let manager_ID = record["instance_id"];
                    let actual_manager_id = manager_id;

                    // checking customer availlable or not for mapping
                    const is_employee_available = await sql_request.query(
                      `SELECT id FROM employees WHERE id='${manager_id}'`
                    );

                    if (is_employee_available["recordset"].length > 0) {
                      manager_ID = manager_id;
                    }

                    project_managers_final_data_pool.push({
                      id: record["id"],
                      manager_id: manager_ID,
                      actual_manager_id: actual_manager_id,
                    });
                  })
                );

                final_data_pool.push({
                  id: record["id"],
                  number: record["number"] ? record["number"] : "default",
                  name: record["name"] ? record["name"] : `${record["id"]}`,
                  status: record["status"] ? record["status"] : "No Status",
                  customer_details_id: customer_details_id,
                  actual_customer_details_id: actual_customer_details_id,
                  location_id: location_id,
                  actual_location_id: actual_location_id,
                  startDate: startDate,
                  targetCompletionDate: targetCompletionDate,
                  actualCompletionDate: actualCompletionDate,
                  createdOn: createdOn,
                  modifiedOn: modifiedOn,
                  legal_entity_id: record["instance_id"],
                });
              })
          );
        }

        // console.log("final_data_pool: ", final_data_pool);
        // console.log("header_data: ", header_data);

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   final_data_pool,
        //   header_data,
        //   table_name
        // );

        console.log("projects data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          // await csv_generator(final_data_pool, header_data, table_name);

          do {
            hvac_tables_responses["projects"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (hvac_tables_responses["projects"]["status"] != "success");

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET projects = '${hvac_tables_responses["projects"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["projects"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET projects = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        if (project_managers_final_data_pool.length > 0) {
          do {
            hvac_tables_responses["project_managers"]["status"] =
              await hvac_data_insertion(
                sql_request,
                project_managers_final_data_pool,
                project_managers_header_data,
                "project_managers"
              );
          } while (
            hvac_tables_responses["project_managers"]["status"] != "success"
          );

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET project_managers = '${hvac_tables_responses["project_managers"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["project_managers"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET project_managers = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        delete data_lake[api_name];
        delete data_lake["purchase_order"];

        break;
      }

      case "call_details": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const telecom_calls_data_pool =
          data_lake[table_name]["telecom__calls"]["data_pool"];

        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        // console.log("telecom_calls_data_pool: ", telecom_calls_data_pool);
        // console.log("header_data: ", header_data);

        const batchSize = 25; // Set your desired batch size

        console.log(
          "Object.keys(telecom_calls_data_pool).length: ",
          Object.keys(telecom_calls_data_pool).length
        );

        for (
          let i = 0;
          i < Object.keys(telecom_calls_data_pool).length;
          i += batchSize
        ) {
          console.log(
            "call details: ",
            i,
            "/",
            Object.keys(telecom_calls_data_pool).length
          );
          await Promise.all(
            Object.keys(telecom_calls_data_pool)
              .slice(i, i + batchSize)
              .map(async (record_id) => {
                const record = telecom_calls_data_pool[record_id];

                let createdOn = "2000-01-01T00:00:00.00Z";

                if (record["leadCall"]["createdOn"]) {
                  if (
                    new Date(record["leadCall"]["createdOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    createdOn = record["leadCall"]["createdOn"];
                  }
                } else {
                  createdOn = "2001-01-01T00:00:00.00Z";
                }

                let modifiedOn = "2000-01-01T00:00:00.00Z";

                if (record["leadCall"]["modifiedOn"]) {
                  if (
                    new Date(record["leadCall"]["modifiedOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    modifiedOn = record["leadCall"]["modifiedOn"];
                  }
                } else {
                  modifiedOn = "2001-01-01T00:00:00.00Z";
                }

                let receivedOn = "2000-01-01T00:00:00.00Z";

                if (record["leadCall"]["receivedOn"]) {
                  if (
                    new Date(record["leadCall"]["receivedOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    receivedOn = record["leadCall"]["receivedOn"];
                  }
                } else {
                  receivedOn = "2001-01-01T00:00:00.00Z";
                }

                let project_id = record["instance_id"];
                let actual_project_id = record["projectId"]
                  ? record["projectId"]
                  : record["instance_id"];

                if (record["projectId"]) {
                  // checking projects availlable or not for mapping
                  const is_project_available = await sql_request.query(
                    `SELECT id FROM projects WHERE id='${record["projectId"]}'`
                  );

                  if (is_project_available["recordset"].length > 0) {
                    project_id = record["projectId"];
                  }
                }

                let business_unit_id = record["instance_id"];
                let actual_business_unit_id = record["instance_id"];
                let business_unit_active = 0;
                let business_unit_name = "default";
                let business_unit_official_name = "default";
                if (record["businessUnit"]) {
                  actual_business_unit_id = record["businessUnit"]["id"]
                    ? record["businessUnit"]["id"]
                    : record["instance_id"];
                  business_unit_active = record["businessUnit"]["active"]
                    ? 1
                    : 0;
                  business_unit_name = record["businessUnit"]["name"]
                    ? record["businessUnit"]["name"]
                    : "default";
                  business_unit_official_name = record["businessUnit"][
                    "officialName"
                  ]
                    ? record["businessUnit"]["officialName"]
                    : "default";

                  if (record["businessUnit"]["id"]) {
                    // checking business unit availlable or not for mapping
                    const is_business_unit_available = await sql_request.query(
                      `SELECT id FROM business_unit WHERE id='${record["businessUnit"]["id"]}'`
                    );

                    if (is_business_unit_available["recordset"].length > 0) {
                      business_unit_id = record["businessUnit"]["id"];
                    }
                  }
                }

                let customer_details_id = record["instance_id"];
                let actual_customer_details_id = record["instance_id"];
                let customer_name = "default";
                let is_customer_active = 0;
                let street_address = "default";
                let street = "default";
                let unit = "default";
                let city = "default";
                let state = "default";
                let country = "default";
                let zip = "default";
                let latitude = 0.0;
                let longitude = 0.0;
                let customer_import_id = "default";
                let customer_type = "Others";
                if (record["leadCall"]["customer"]) {
                  actual_customer_details_id =
                    record["leadCall"]["customer"]["id"];
                  customer_name = record["leadCall"]["customer"]["name"]
                    ? record["leadCall"]["customer"]["name"]
                    : "default";
                  is_customer_active = record["leadCall"]["customer"]["active"]
                    ? 1
                    : 0;

                  if (record["leadCall"]["customer"]["address"]) {
                    street_address = record["leadCall"]["customer"]["address"][
                      "streetAddress"
                    ]
                      ? record["leadCall"]["customer"]["address"][
                          "streetAddress"
                        ]
                      : "default";
                    street = record["leadCall"]["customer"]["address"]["street"]
                      ? record["leadCall"]["customer"]["address"]["street"]
                      : "default";
                    unit = record["leadCall"]["customer"]["address"]["unit"]
                      ? record["leadCall"]["customer"]["address"]["unit"]
                      : "default";
                    city = record["leadCall"]["customer"]["address"]["city"]
                      ? record["leadCall"]["customer"]["address"]["city"]
                      : "default";
                    state = record["leadCall"]["customer"]["address"]["state"]
                      ? record["leadCall"]["customer"]["address"]["state"]
                      : "default";
                    country = record["leadCall"]["customer"]["address"][
                      "country"
                    ]
                      ? record["leadCall"]["customer"]["address"]["country"]
                      : "default";
                    zip = record["leadCall"]["customer"]["address"]["zip"]
                      ? record["leadCall"]["customer"]["address"]["zip"]
                      : "default";
                    latitude = record["leadCall"]["customer"]["address"][
                      "latitude"
                    ]
                      ? record["leadCall"]["customer"]["address"]["latitude"]
                      : 0.0;
                    longitude = record["leadCall"]["customer"]["address"][
                      "longitude"
                    ]
                      ? record["leadCall"]["customer"]["address"]["longitude"]
                      : 0.0;
                  }

                  customer_import_id = record["leadCall"]["customer"][
                    "importId"
                  ]
                    ? record["leadCall"]["customer"]["importId"]
                    : "default";
                  customer_type = record["leadCall"]["customer"]["type"]
                    ? record["leadCall"]["customer"]["type"]
                    : "Others";

                  if (record["leadCall"]["customer"]["id"]) {
                    // checking customer availlable or not for mapping
                    const is_customer_available = await sql_request.query(
                      `SELECT id FROM customer_details WHERE id='${record["leadCall"]["customer"]["id"]}'`
                    );

                    if (is_customer_available["recordset"].length > 0) {
                      customer_details_id =
                        record["leadCall"]["customer"]["id"];
                    }
                  }
                }

                let campaign_id = record["instance_id"];
                let campaign_category = "default";
                let campaign_source = "default";
                let campaign_medium = "default";
                let campaign_dnis = "default";
                let campaign_name = "default";
                let campaign_createdOn = "2000-01-01T00:00:00.00Z";
                let campaign_modifiedOn = "2000-01-01T00:00:00.00Z";
                let is_campaign_active = 0;
                if (record["leadCall"]["campaign"]) {
                  if (record["leadCall"]["campaign"]["createdOn"]) {
                    if (
                      new Date(record["leadCall"]["campaign"]["createdOn"]) >
                      new Date("2000-01-01T00:00:00.00Z")
                    ) {
                      campaign_createdOn =
                        record["leadCall"]["campaign"]["createdOn"];
                    }
                  } else {
                    campaign_createdOn = "2001-01-01T00:00:00.00Z";
                  }

                  if (record["leadCall"]["campaign"]["modifiedOn"]) {
                    if (
                      new Date(record["leadCall"]["campaign"]["modifiedOn"]) >
                      new Date("2000-01-01T00:00:00.00Z")
                    ) {
                      campaign_modifiedOn =
                        record["leadCall"]["campaign"]["modifiedOn"];
                    }
                  } else {
                    campaign_modifiedOn = "2001-01-01T00:00:00.00Z";
                  }

                  campaign_id = record["leadCall"]["campaign"]["id"]
                    ? record["leadCall"]["campaign"]["id"]
                    : record["instance_id"];

                  if (record["leadCall"]["campaign"]["category"]) {
                    campaign_category = record["leadCall"]["campaign"][
                      "category"
                    ]["name"]
                      ? record["leadCall"]["campaign"]["category"]["name"]
                      : "default";
                  }

                  campaign_source = record["leadCall"]["campaign"]["source"]
                    ? record["leadCall"]["campaign"]["source"]
                    : "default";
                  campaign_medium = record["leadCall"]["campaign"]["medium"]
                    ? record["leadCall"]["campaign"]["medium"]
                    : "default";
                  campaign_dnis = record["leadCall"]["campaign"]["dnis"]
                    ? record["leadCall"]["campaign"]["dnis"]
                    : "default";
                  campaign_name = record["leadCall"]["campaign"]["name"]
                    ? record["leadCall"]["campaign"]["name"]
                    : "default";
                  is_campaign_active = record["leadCall"]["campaign"]["active"]
                    ? 1
                    : 0;
                }

                let agent_id = record["instance_id"];
                let agent_externalId = record["instance_id"];
                let agent_name = "default";
                if (record["leadCall"]["agent"]) {
                  agent_id = record["leadCall"]["agent"]["id"]
                    ? record["leadCall"]["agent"]["id"]
                    : record["instance_id"];
                  agent_externalId = record["leadCall"]["agent"]["externalId"]
                    ? record["leadCall"]["agent"]["externalId"]
                    : record["instance_id"];
                  agent_name = record["leadCall"]["agent"]["name"]
                    ? record["leadCall"]["agent"]["name"]
                    : "default";
                }

                let type_id = record["instance_id"];
                let type_name = "default";
                let type_modifiedOn = "2000-01-01T00:00:00.00Z";
                if (record["type"]) {
                  type_id = record["type"]["id"]
                    ? record["type"]["id"]
                    : record["instance_id"];
                  type_name = record["type"]["name"]
                    ? record["type"]["name"]
                    : "default";
                  if (record["type"]["modifiedOn"]) {
                    if (
                      new Date(record["type"]["modifiedOn"]) >
                      new Date("2000-01-01T00:00:00.00Z")
                    ) {
                      type_modifiedOn = record["type"]["modifiedOn"];
                    }
                  } else {
                    type_modifiedOn = "2001-01-01T00:00:00.00Z";
                  }
                }

                final_data_pool.push({
                  id: record["leadCall"]["id"],
                  instance_id: record["instance_id"],
                  job_number: record["jobNumber"]
                    ? record["jobNumber"]
                    : "default",
                  project_id: project_id,
                  actual_project_id: actual_project_id,
                  createdOn: createdOn,
                  modifiedOn: modifiedOn,
                  receivedOn: receivedOn,
                  duration: record["leadCall"]["duration"]
                    ? record["leadCall"]["duration"]
                    : "00:00:00",
                  from: record["leadCall"]["from"]
                    ? record["leadCall"]["from"]
                    : "default",
                  to: record["leadCall"]["to"]
                    ? record["leadCall"]["to"]
                    : "default",
                  direction: record["leadCall"]["direction"]
                    ? record["leadCall"]["direction"]
                    : "Others",
                  call_type: record["leadCall"]["callType"]
                    ? record["leadCall"]["callType"]
                    : "Others",
                  customer_details_id: customer_details_id,
                  actual_customer_details_id: actual_customer_details_id,
                  is_customer_active: is_customer_active,
                  customer_name: customer_name,
                  street_address: street_address,
                  street: street,
                  unit: unit,
                  city: city,
                  state: state,
                  country: country,
                  zip: zip,
                  latitude: latitude,
                  longitude: longitude,
                  customer_import_id: customer_import_id,
                  customer_type: customer_type,
                  campaign_id: campaign_id,
                  campaign_category: campaign_category,
                  campaign_source: campaign_source,
                  campaign_medium: campaign_medium,
                  campaign_dnis: campaign_dnis,
                  campaign_name: campaign_name,
                  campaign_createdOn: campaign_createdOn,
                  campaign_modifiedOn: campaign_modifiedOn,
                  is_campaign_active: is_campaign_active,
                  agent_id: agent_id,
                  agent_externalId: agent_externalId,
                  agent_name: agent_name,
                  business_unit_id: business_unit_id,
                  actual_business_unit_id: actual_business_unit_id,
                  business_unit_active: business_unit_active,
                  business_unit_name: business_unit_name,
                  business_unit_official_name: business_unit_official_name,
                  type_id: type_id,
                  type_name: type_name,
                  type_modifiedOn: type_modifiedOn,
                });
              })
          );
        }

        // console.log("final_data_pool: ", final_data_pool);
        // console.log("header_data: ", header_data);

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   final_data_pool,
        //   Object.keys(header_data),
        //   table_name
        // );

        console.log("telecom_calls data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          do {
            hvac_tables_responses["call_details"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (
            hvac_tables_responses["call_details"]["status"] != "success"
          );

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET call_details = '${hvac_tables_responses["call_details"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["call_details"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET call_details = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        delete data_lake[api_name];

        break;
      }

      case "job_details": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const jobs_data_pool = data_lake[api_name]["jpm__jobs"]["data_pool"];

        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        // console.log("jobs_data_pool: ", jobs_data_pool);
        // console.log("header_data: ", header_data);

        const batchSize = 100;

        for (
          let i = 0;
          i < Object.keys(jobs_data_pool).length;
          i += batchSize
        ) {
          await Promise.all(
            Object.keys(jobs_data_pool)
              .slice(i, i + batchSize)
              .map(async (record_id) => {
                const record = jobs_data_pool[record_id];
                let business_unit_id = record["instance_id"];
                let actual_business_unit_id = record["businessUnitId"]
                  ? record["businessUnitId"]
                  : record["instance_id"];
                let customer_details_id = record["instance_id"];
                let actual_customer_details_id = record["customerId"]
                  ? record["customerId"]
                  : record["instance_id"];
                let project_id = record["instance_id"];
                let actual_project_id = record["projectId"]
                  ? record["projectId"]
                  : record["instance_id"];
                let location_id = record["instance_id"];
                let actual_location_id = record["locationId"]
                  ? record["locationId"]
                  : record["instance_id"];
                let lead_call_id = record["instance_id"];
                let actual_lead_call_id = record["leadCallId"]
                  ? record["leadCallId"]
                  : record["instance_id"];
                let campaign_id = record["instance_id"];
                let actual_campaign_id = record["campaignId"]
                  ? record["campaignId"]
                  : record["instance_id"];
                let booking_id = record["instance_id"];
                let actual_booking_id = record["bookingId"]
                  ? record["bookingId"]
                  : record["instance_id"];
                let job_type_id = record["instance_id"];
                let actual_job_type_id = record["jobTypeId"]
                  ? record["jobTypeId"]
                  : record["instance_id"];

                if (record["businessUnitId"]) {
                  // checking business unit availlable or not for mapping
                  const is_business_unit_available = await sql_request.query(
                    `SELECT id FROM business_unit WHERE id='${record["businessUnitId"]}'`
                  );

                  if (is_business_unit_available["recordset"].length > 0) {
                    business_unit_id = record["businessUnitId"];
                  }
                }

                if (record["customerId"]) {
                  // checking customer availlable or not for mapping
                  const is_customer_available = await sql_request.query(
                    `SELECT id FROM customer_details WHERE id='${record["customerId"]}'`
                  );

                  if (is_customer_available["recordset"].length > 0) {
                    customer_details_id = record["customerId"];
                  }
                }

                if (record["projectId"]) {
                  // checking projects availlable or not for mapping
                  const is_project_available = await sql_request.query(
                    `SELECT id FROM projects WHERE id='${record["projectId"]}'`
                  );

                  if (is_project_available["recordset"].length > 0) {
                    project_id = record["projectId"];
                  }
                }

                if (record["locationId"]) {
                  // checking location availlable or not for mapping
                  const is_location_available = await sql_request.query(
                    `SELECT id FROM location WHERE id='${record["locationId"]}'`
                  );

                  if (is_location_available["recordset"].length > 0) {
                    location_id = record["locationId"];
                  }
                }

                if (record["leadCallId"]) {
                  // checking call availlable or not for mapping
                  const is_call_available = await sql_request.query(
                    `SELECT id FROM call_details WHERE id='${record["leadCallId"]}'`
                  );

                  if (is_call_available["recordset"].length > 0) {
                    lead_call_id = record["leadCallId"];
                  }
                }

                if (record["campaignId"]) {
                  // checking campaignId availlable or not for mapping
                  const is_campaigns_available = await sql_request.query(
                    `SELECT id FROM campaigns WHERE id='${record["campaignId"]}'`
                  );

                  if (is_campaigns_available["recordset"].length > 0) {
                    campaign_id = record["campaignId"];
                  }
                }

                if (record["bookingId"]) {
                  // checking bookings id availlable or not for mapping
                  const is_bookings_available = await sql_request.query(
                    `SELECT id FROM bookings WHERE id='${record["bookingId"]}'`
                  );

                  if (is_bookings_available["recordset"].length > 0) {
                    booking_id = record["bookingId"];
                  }
                }

                if (record["jobTypeId"]) {
                  // checking job_type_id availlable or not for mapping
                  const is_job_type_available = await sql_request.query(
                    `SELECT id FROM job_types WHERE id='${record["jobTypeId"]}'`
                  );

                  if (is_job_type_available["recordset"].length > 0) {
                    job_type_id = record["jobTypeId"];
                  }
                }

                let createdOn = "2000-01-01T00:00:00.00Z";

                if (record["createdOn"]) {
                  if (
                    new Date(record["createdOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    createdOn = record["createdOn"];
                  }
                } else {
                  createdOn = "2001-01-01T00:00:00.00Z";
                }

                let modifiedOn = "2000-01-01T00:00:00.00Z";
                if (record["modifiedOn"]) {
                  if (
                    new Date(record["modifiedOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    modifiedOn = record["modifiedOn"];
                  }
                } else {
                  modifiedOn = "2001-01-01T00:00:00.00Z";
                }

                let job_completion_time = "2000-01-01T00:00:00.00Z";

                if (record["completedOn"]) {
                  if (
                    new Date(record["completedOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    job_completion_time = record["completedOn"];
                  }
                } else {
                  job_completion_time = "2001-01-01T00:00:00.00Z";
                }

                final_data_pool.push({
                  id: record["id"],
                  job_type_id: job_type_id,
                  actual_job_type_id: actual_job_type_id,
                  job_number: record["jobNumber"]
                    ? record["jobNumber"]
                    : "default",
                  job_status: record["jobStatus"],
                  job_completion_time: job_completion_time,
                  business_unit_id: business_unit_id,
                  actual_business_unit_id: actual_business_unit_id,
                  location_id: location_id,
                  actual_location_id: actual_location_id,
                  customer_details_id: customer_details_id,
                  actual_customer_details_id: actual_customer_details_id,
                  project_id: project_id,
                  actual_project_id: actual_project_id,
                  campaign_id: campaign_id,
                  actual_campaign_id: actual_campaign_id,
                  createdOn: createdOn,
                  modifiedOn: modifiedOn,
                  created_by_id: record["createdById"]
                    ? record["createdById"]
                    : record["instance_id"],
                  lead_call_id: lead_call_id,
                  actual_lead_call_id: actual_lead_call_id,
                  booking_id: booking_id,
                  actual_booking_id: actual_booking_id,
                  sold_by_id: record["soldById"]
                    ? record["soldById"]
                    : record["instance_id"],
                });
              })
          );
        }

        // console.log("final_data_pool: ", final_data_pool);
        // console.log("header_data: ", header_data);

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   final_data_pool,
        //   Object.keys(header_data),
        //   table_name
        // );

        console.log("job details data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          // await csv_generator(final_data_pool, header_data, table_name);

          do {
            hvac_tables_responses["job_details"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (hvac_tables_responses["job_details"]["status"] != "success");

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET job_details = '${hvac_tables_responses["job_details"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["job_details"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET job_details = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        delete data_lake[api_name];

        break;
      }

      case "appointments": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool = data_lake[api_name]["jpm__appointments"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        const batchSize = 100;

        for (let i = 0; i < Object.keys(data_pool).length; i += batchSize) {
          await Promise.all(
            Object.keys(data_pool)
              .slice(i, i + batchSize)
              .map(async (record_id) => {
                const record = data_pool[record_id];

                let job_details_id = record["instance_id"];
                let actual_job_details_id = record["jobId"]
                  ? record["jobId"]
                  : record["instance_id"];

                if (record["jobId"]) {
                  // checking jobId availlable or not for mapping
                  const is_jobs_available = await sql_request.query(
                    `SELECT id FROM job_details WHERE id='${record["jobId"]}'`
                  );

                  if (is_jobs_available["recordset"].length > 0) {
                    job_details_id = record["jobId"];
                  }
                }

                let customer_details_id = record["instance_id"];
                let actual_customer_details_id = record["customerId"]
                  ? record["customerId"]
                  : record["instance_id"];

                if (record["customerId"]) {
                  // checking customer availlable or not for mapping
                  const is_customer_available = await sql_request.query(
                    `SELECT id FROM customer_details WHERE id='${record["customerId"]}'`
                  );

                  if (is_customer_available["recordset"].length > 0) {
                    customer_details_id = record["customerId"];
                  }
                }

                let start = "2000-01-01T00:00:00.00Z";

                if (record["start"]) {
                  if (
                    new Date(record["start"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    start = record["start"];
                  }
                } else {
                  start = "2001-01-01T00:00:00.00Z";
                }

                let end = "2000-01-01T00:00:00.00Z";
                if (record["end"]) {
                  if (
                    new Date(record["end"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    end = record["end"];
                  }
                } else {
                  end = "2001-01-01T00:00:00.00Z";
                }

                let arrivalWindowStart = "2000-01-01T00:00:00.00Z";

                if (record["arrivalWindowStart"]) {
                  if (
                    new Date(record["arrivalWindowStart"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    arrivalWindowStart = record["arrivalWindowStart"];
                  }
                } else {
                  arrivalWindowStart = "2001-01-01T00:00:00.00Z";
                }

                let arrivalWindowEnd = "2000-01-01T00:00:00.00Z";
                if (record["arrivalWindowEnd"]) {
                  if (
                    new Date(record["arrivalWindowEnd"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    arrivalWindowEnd = record["arrivalWindowEnd"];
                  }
                } else {
                  arrivalWindowEnd = "2001-01-01T00:00:00.00Z";
                }

                let createdOn = "2000-01-01T00:00:00.00Z";

                if (record["createdOn"]) {
                  if (
                    new Date(record["createdOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    createdOn = record["createdOn"];
                  }
                } else {
                  createdOn = "2001-01-01T00:00:00.00Z";
                }

                let modifiedOn = "2000-01-01T00:00:00.00Z";
                if (record["modifiedOn"]) {
                  if (
                    new Date(record["modifiedOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    modifiedOn = record["modifiedOn"];
                  }
                } else {
                  modifiedOn = "2001-01-01T00:00:00.00Z";
                }

                final_data_pool.push({
                  id: record["id"],
                  job_details_id: job_details_id,
                  actual_job_details_id: actual_job_details_id,
                  appointmentNumber: record["appointmentNumber"]
                    ? record["appointmentNumber"]
                    : "default",
                  start: start,
                  end: end,
                  arrivalWindowStart: arrivalWindowStart,
                  arrivalWindowEnd: arrivalWindowEnd,
                  status: record["status"] ? record["status"] : "Not Known",
                  createdOn: createdOn,
                  modifiedOn: modifiedOn,
                  customer_details_id: customer_details_id,
                  actual_customer_details_id: actual_customer_details_id,
                });
              })
          );
        }

        console.log("appointments data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          // await csv_generator(final_data_pool, header_data, table_name);

          do {
            hvac_tables_responses["appointments"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (
            hvac_tables_responses["appointments"]["status"] != "success"
          );

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET appointments = '${hvac_tables_responses["appointments"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["appointments"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET appointments = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        delete data_lake[api_name];

        break;
      }

      case "vendor": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool =
          data_lake[api_name]["inventory__vendors"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        // console.log("data_pool: ", data_pool);
        // console.log("header_data: ", header_data);

        const batchSize = 100;

        for (let i = 0; i < Object.keys(data_pool).length; i += batchSize) {
          await Promise.all(
            Object.keys(data_pool)
              .slice(i, i + batchSize)
              .map((record_id) => {
                const record = data_pool[record_id];
                final_data_pool.push({
                  id: record["id"],
                  name: record["name"] ? record["name"] : "default_vendor",
                  is_active: record["active"] ? 1 : 0,
                });
              })
          );
        }

        // console.log("final_data_pool: ", final_data_pool);
        // console.log("header_data: ", header_data);

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   final_data_pool,
        //   header_data,
        //   table_name
        // );

        console.log("vendor data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          // await csv_generator(final_data_pool, header_data, table_name);

          do {
            hvac_tables_responses["vendor"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (hvac_tables_responses["vendor"]["status"] != "success");

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET vendor = '${hvac_tables_responses["vendor"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["vendor"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET vendor = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        delete data_lake[api_name];

        break;
      }

      case "inventory_bills": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool =
          data_lake[api_name]["accounting__export/inventory-bills"][
            "data_pool"
          ];

        const header_data = hvac_tables[table_name]["columns"];
        let final_data_pool = [];

        const batchSize = 100;

        for (let i = 0; i < Object.keys(data_pool).length; i++) {
          await Promise.all(
            Object.keys(data_pool)
              .slice(i, i + batchSize)
              .map(async (record_id) => {
                const record = data_pool[record_id];

                let billDate = "2000-01-01T00:00:00.00Z";

                if (record["billDate"]) {
                  if (
                    new Date(record["billDate"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    billDate = record["billDate"];
                  }
                } else {
                  billDate = "2001-01-01T00:00:00.00Z";
                }

                if (record["createdOn"]) {
                  if (
                    new Date(record["createdOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    createdOn = record["createdOn"];
                  }
                } else {
                  createdOn = "2001-01-01T00:00:00.00Z";
                }

                let dueDate = "2000-01-01T00:00:00.00Z";

                if (record["dueDate"]) {
                  if (
                    new Date(record["dueDate"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    dueDate = record["dueDate"];
                  }
                } else {
                  dueDate = "2001-01-01T00:00:00.00Z";
                }

                let purchase_order_id = record["instance_id"];
                let actual_purchase_order_id = record["purchaseOrderId"]
                  ? record["purchaseOrderId"]
                  : record["instance_id"];

                if (record["purchaseOrderId"]) {
                  // checking purchaseOrderId availlable or not for mapping
                  const is_po_available = await sql_request.query(
                    `SELECT id FROM purchase_order WHERE id='${record["purchaseOrderId"]}'`
                  );

                  if (is_po_available["recordset"].length > 0) {
                    purchase_order_id = record["purchaseOrderId"];
                  }
                }

                let business_unit_id = record["instance_id"];
                let actual_business_unit_id = record["instance_id"];

                if (record["businessUnit"]) {
                  actual_business_unit_id = record["businessUnit"]["id"]
                    ? record["businessUnit"]["id"]
                    : record["instance_id"];

                  if (record["businessUnit"]["id"]) {
                    // checking business unit availlable or not for mapping
                    const is_business_unit_available = await sql_request.query(
                      `SELECT id FROM business_unit WHERE id='${record["businessUnit"]["id"]}'`
                    );

                    if (is_business_unit_available["recordset"].length > 0) {
                      business_unit_id = record["businessUnit"]["id"];
                    }
                  }
                }

                let vendor_id = record["instance_id"];
                let actual_vendor_id = record["instance_id"];
                if (record["vendor"]) {
                  actual_vendor_id = record["vendor"]["id"]
                    ? record["vendor"]["id"]
                    : record["instance_id"];

                  if (record["vendor"]["id"]) {
                    // checking vendor availlable or not for mapping
                    const is_vendor_available = await sql_request.query(
                      `SELECT id FROM vendor WHERE id='${record["vendor"]["id"]}'`
                    );

                    if (is_vendor_available["recordset"].length > 0) {
                      vendor_id = record["vendor"]["id"];
                    }
                  }
                }

                let job_details_id = record["instance_id"];
                let actual_job_details_id = record["jobId"]
                  ? record["jobId"]
                  : record["instance_id"];

                if (record["jobId"]) {
                  // checking jobId availlable or not for mapping
                  const is_jobs_available = await sql_request.query(
                    `SELECT id FROM job_details WHERE id='${record["jobId"]}'`
                  );

                  if (is_jobs_available["recordset"].length > 0) {
                    job_details_id = record["jobId"];
                  }
                }

                final_data_pool.push({
                  id: record["id"],
                  purchase_order_id: purchase_order_id,
                  actual_purchase_order_id: actual_purchase_order_id,
                  syncStatus: record["syncStatus"]
                    ? record["syncStatus"]
                    : "default",
                  referenceNumber: record["referenceNumber"]
                    ? record["referenceNumber"]
                    : "default",
                  vendorNumber: record["vendorNumber"],
                  billDate: billDate,
                  billAmount: parseFloat(record["billAmount"]),
                  taxAmount: parseFloat(record["taxAmount"]),
                  shippingAmount: parseFloat(record["shippingAmount"]),
                  createdOn: createdOn,
                  dueDate: dueDate,
                  business_unit_id: business_unit_id,
                  actual_business_unit_id: actual_business_unit_id,
                  vendor_id: vendor_id,
                  actual_vendor_id: actual_vendor_id,
                  job_details_id: job_details_id,
                  actual_job_details_id: actual_job_details_id,
                  job_number: record["jobNumber"],
                });
              })
          );
        }

        if (final_data_pool.length > 0) {
          // await csv_generator(final_data_pool, header_data, table_name);

          do {
            hvac_tables_responses["inventory_bills"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                final_data_pool,
                header_data,
                "inventory_bills"
              );
          } while (
            hvac_tables_responses["inventory_bills"]["status"] != "success"
          );

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET inventory_bills = '${hvac_tables_responses["inventory_bills"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["inventory_bills"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET inventory_bills = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        break;
      }

      case "technician": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool =
          data_lake["technician"]["settings__technicians"]["data_pool"];

        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        const batchSize = 100;

        for (let i = 0; i < Object.keys(data_pool).length; i += batchSize) {
          await Promise.all(
            Object.keys(data_pool)
              .slice(i, i + batchSize)
              .map(async (record_id) => {
                const record = data_pool[record_id];
                let acutal_business_unit_id = record["businessUnitId"]
                  ? record["businessUnitId"]
                  : record["instance_id"];

                let business_unit_id = record["instance_id"];

                if (record["businessUnitId"]) {
                  // checking business unit availlable or not for mapping
                  const is_business_unit_available = await sql_request.query(
                    `SELECT id FROM business_unit WHERE id='${record["businessUnitId"]}'`
                  );

                  if (is_business_unit_available["recordset"].length > 0) {
                    business_unit_id = record["businessUnitId"];
                  }
                }

                final_data_pool.push({
                  id: record["id"],
                  name: record["name"] ? record["name"] : "default_technician",
                  is_active: 0,
                  team: record["team"] ? record["team"] : "default_team",
                  business_unit_id: business_unit_id,
                  acutal_business_unit_id: acutal_business_unit_id,
                });
              })
          );
        }

        console.log("techician data: ", final_data_pool.length);
        // console.log("technician data: ", final_data_pool);
        // console.log("header_data: ", header_data);

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   final_data_pool,
        //   header_data,
        //   table_name
        // );

        if (final_data_pool.length > 0) {
          // await csv_generator(final_data_pool, header_data, table_name);

          do {
            hvac_tables_responses["technician"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (hvac_tables_responses["technician"]["status"] != "success");

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET technician = '${hvac_tables_responses["technician"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["technician"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET technician = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        delete data_lake[api_name];

        break;
      }

      case "sku_details": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const materials_data_pool =
          data_lake[api_name]["pricebook__materials"]["data_pool"];
        const equipment_data_pool =
          data_lake[api_name]["pricebook__equipment"]["data_pool"];
        const services_data_pool =
          data_lake[api_name]["pricebook__services"]["data_pool"];

        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        // console.log("data_pool: ", materials_data_pool);
        // console.log("data_pool: ", equipment_data_pool);
        // console.log("header_data: ", header_data);

        const batchSize = 100;

        for (
          let i = 0;
          i < Object.keys(materials_data_pool).length;
          i += batchSize
        ) {
          await Promise.all(
            Object.keys(materials_data_pool)
              .slice(i, i + batchSize)
              .map(async (record_id) => {
                const record = materials_data_pool[record_id];
                let vendor_id = record["instance_id"];
                let actual_vendor_id = record["instance_id"];
                if (record["primaryVendor"]) {
                  actual_vendor_id = record["primaryVendor"]["vendorId"]
                    ? record["primaryVendor"]["vendorId"]
                    : record["instance_id"];

                  if (record["primaryVendor"]["vendorId"]) {
                    // checking vendor availlable or not for mapping
                    const is_vendor_available = await sql_request.query(
                      `SELECT id FROM vendor WHERE id='${record["primaryVendor"]["vendorId"]}'`
                    );

                    if (is_vendor_available["recordset"].length > 0) {
                      vendor_id = record["primaryVendor"]["vendorId"];
                    }
                  }
                }

                final_data_pool.push({
                  id: record["id"],
                  sku_name: record["code"],
                  sku_type: "Material",
                  sku_unit_price: record["cost"]
                    ? parseFloat(record["cost"])
                    : 0,
                  vendor_id: vendor_id,
                  actual_vendor_id: actual_vendor_id,
                });
              })
          );
        }

        for (
          let i = 0;
          i < Object.keys(equipment_data_pool).length;
          i += batchSize
        ) {
          await Promise.all(
            Object.keys(equipment_data_pool)
              .slice(i, i + batchSize)
              .map(async (record_id) => {
                const record = equipment_data_pool[record_id];
                let vendor_id = record["instance_id"];
                let actual_vendor_id = record["instance_id"];
                if (record["primaryVendor"]) {
                  actual_vendor_id = record["primaryVendor"]["vendorId"]
                    ? record["primaryVendor"]["vendorId"]
                    : record["instance_id"];

                  if (record["primaryVendor"]["vendorId"]) {
                    // checking vendor availlable or not for mapping
                    const is_vendor_available = await sql_request.query(
                      `SELECT id FROM vendor WHERE id='${record["primaryVendor"]["vendorId"]}'`
                    );

                    if (is_vendor_available["recordset"].length > 0) {
                      vendor_id = record["primaryVendor"]["vendorId"];
                    }
                  }
                }
                final_data_pool.push({
                  id: record["id"],
                  sku_name: record["code"] ? record["code"] : "default",
                  sku_type: "Equipment",
                  sku_unit_price: record["cost"] ? record["cost"] : 0,
                  vendor_id: vendor_id,
                  actual_vendor_id: actual_vendor_id,
                });
              })
          );
        }

        for (
          let i = 0;
          i < Object.keys(services_data_pool).length;
          i += batchSize
        ) {
          Object.keys(services_data_pool)
            .slice(i, i + batchSize)
            .map((record_id) => {
              const record = services_data_pool[record_id];

              final_data_pool.push({
                id: record["id"],
                sku_name: record["code"] ? record["code"] : "default",
                sku_type: "Service",
                sku_unit_price: record["price"] ? record["price"] : 0,
                vendor_id: record["instance_id"],
                actual_vendor_id: record["instance_id"],
              });
            });
        }

        // console.log("final_data_pool: ", final_data_pool);
        // console.log("header_data: ", header_data);

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   final_data_pool,
        //   header_data,
        //   table_name
        // );

        console.log("sku_details data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          // await csv_generator(final_data_pool, header_data, table_name);

          do {
            hvac_tables_responses["sku_details"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (hvac_tables_responses["sku_details"]["status"] != "success");

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET sku_details = '${hvac_tables_responses["sku_details"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["sku_details"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET sku_details = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        delete data_lake[api_name];

        break;
      }

      case "invoice": {
        const table_name = main_api_list[api_name][0]["table_name"];

        const invoice_header_data = hvac_tables["invoice"]["columns"];
        const invoice_data_pool =
          data_lake["invoice"]["accounting__invoices"]["data_pool"];
        const cogs_material_header_data =
          hvac_tables["cogs_material"]["columns"];
        const cogs_equipment_header_data =
          hvac_tables["cogs_equipment"]["columns"];
        const cogs_service_header_data = hvac_tables["cogs_service"]["columns"];

        let invoice_final_data_pool = [];
        let cogs_material_final_data_pool = [];
        let cogs_equipment_final_data_pool = [];
        let cogs_services_final_data_pool = [];

        let batchSize = 20;

        console.log(
          "invoice total data: ",
          Object.keys(invoice_data_pool).length
        );

        for (
          let i = 0;
          i < Object.keys(invoice_data_pool).length;
          i = i + batchSize
        ) {
          console.log("invoice", i, "/", Object.keys(invoice_data_pool).length);
          await Promise.all(
            Object.keys(invoice_data_pool)
              .slice(i, i + batchSize)
              .map(async (record_id) => {
                const record = invoice_data_pool[record_id];

                let job_details_id = record["instance_id"];
                let actual_job_details_id = record["instance_id"];
                if (record["job"]) {
                  actual_job_details_id = record["job"]["id"];

                  if (record["job"]["id"]) {
                    // checking jobId availlable or not for mapping
                    const is_jobs_available = await sql_request.query(
                      `SELECT id FROM job_details WHERE id='${record["job"]["id"]}'`
                    );

                    if (is_jobs_available["recordset"].length > 0) {
                      job_details_id = record["job"]["id"];
                    }
                  }
                }

                let project_id = record["instance_id"];
                let actual_project_id = record["projectId"]
                  ? record["projectId"]
                  : record["instance_id"];

                // checking projects availlable or not for mapping
                if (record["projectId"]) {
                  const is_project_available = await sql_request.query(
                    `SELECT id FROM projects WHERE id='${record["projectId"]}'`
                  );

                  if (is_project_available["recordset"].length > 0) {
                    project_id = record["projectId"];
                  }
                }

                let business_unit_id = record["instance_id"];
                let actual_business_unit_id = record["instance_id"];
                if (record["businessUnit"]) {
                  actual_business_unit_id = record["businessUnit"]["id"];

                  if (record["businessUnit"]["id"]) {
                    // checking business unit availlable or not for mapping
                    const is_business_unit_available = await sql_request.query(
                      `SELECT id FROM business_unit WHERE id='${record["businessUnit"]["id"]}'`
                    );

                    if (is_business_unit_available["recordset"].length > 0) {
                      business_unit_id = record["businessUnit"]["id"];
                    }
                  }
                }

                let location_id = record["instance_id"];
                let actual_location_id = record["instance_id"];
                if (record["location"]) {
                  actual_location_id = record["location"]["id"];

                  if (record["location"]["id"]) {
                    // checking location availlable or not for mapping
                    const is_location_available = await sql_request.query(
                      `SELECT id FROM location WHERE id='${record["location"]["id"]}'`
                    );

                    if (is_location_available["recordset"].length > 0) {
                      location_id = record["location"]["id"];
                    }
                  }
                }

                let customer_id = record["instance_id"];
                let actual_customer_id = record["instance_id"];
                let customer_name = "default";
                if (record["customer"]) {
                  actual_customer_id = record["customer"]["id"];
                  customer_name = record["customer"]["name"]
                    ? record["customer"]["name"]
                    : "default";

                  if (record["customer"]["id"]) {
                    // checking customer availlable or not for mapping
                    const is_customer_available = await sql_request.query(
                      `SELECT id FROM customer_details WHERE id='${record["customer"]["id"]}'`
                    );

                    if (is_customer_available["recordset"].length > 0) {
                      customer_id = record["customer"]["id"];
                    }
                  }
                }

                let address_street = "";
                let address_unit = "";
                let address_city = "";
                let address_state = "";
                let address_zip = 57483;
                let acutal_address_zip = "";
                let address_country = "";
                let city = "Mexico";
                if (record["locationAddress"]) {
                  address_street = record["locationAddress"]["street"]
                    ? record["locationAddress"]["street"]
                    : "";
                  address_unit = record["locationAddress"]["unit"]
                    ? record["locationAddress"]["unit"]
                    : "";
                  address_city = record["locationAddress"]["city"]
                    ? record["locationAddress"]["city"]
                    : "";
                  address_state = record["locationAddress"]["state"]
                    ? record["locationAddress"]["state"]
                    : "";
                  address_country = record["locationAddress"]["country"]
                    ? record["locationAddress"]["country"]
                    : "";
                  acutal_address_zip = record["locationAddress"]["zip"]
                    ? record["locationAddress"]["zip"]
                    : "";

                  address_zip = record["locationAddress"]["zip"]
                    ? record["locationAddress"]["zip"]
                    : 57483;

                  if (typeof address_zip == "string") {
                    const numericValue = Number(address_zip.split("-")[0]);
                    if (!isNaN(numericValue)) {
                      // If the conversion is successful and numericValue is not NaN, update address_zip
                      address_zip = numericValue;
                    } else {
                      // Handle the case where the conversion fails
                      address_zip = 57483;
                    }
                  }

                  if (!unique_us_zip_codes[String(address_zip)]) {
                    address_zip = 57483;
                  } else {
                    city = unique_us_zip_codes[String(address_zip)]["city"];
                  }
                }

                let invoice_date = "2000-01-01T00:00:00.00Z";

                if (record["invoiceDate"]) {
                  if (
                    new Date(record["invoiceDate"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    invoice_date = record["invoiceDate"];
                  }
                } else {
                  invoice_date = "2001-01-01T00:00:00.00Z";
                }

                let dueDate = "2000-01-01T00:00:00.00Z";

                if (record["dueDate"]) {
                  if (
                    new Date(record["dueDate"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    dueDate = record["dueDate"];
                  }
                } else {
                  dueDate = "2001-01-01T00:00:00.00Z";
                }

                let depositedOn = "2000-01-01T00:00:00.00Z";

                if (record["depositedOn"]) {
                  if (
                    new Date(record["depositedOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    depositedOn = record["depositedOn"];
                  }
                } else {
                  depositedOn = "2001-01-01T00:00:00.00Z";
                }

                let createdOn = "2000-01-01T00:00:00.00Z";

                if (record["createdOn"]) {
                  if (
                    new Date(record["createdOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    createdOn = record["createdOn"];
                  }
                } else {
                  createdOn = "2001-01-01T00:00:00.00Z";
                }

                let modifiedOn = "2000-01-01T00:00:00.00Z";

                if (record["modifiedOn"]) {
                  if (
                    new Date(record["modifiedOn"]) >
                    new Date("2000-01-01T00:00:00.00Z")
                  ) {
                    modifiedOn = record["modifiedOn"];
                  }
                } else {
                  modifiedOn = "2001-01-01T00:00:00.00Z";
                }

                const js_date = new Date(invoice_date);

                const current_date = new Date();

                if (js_date > current_date) {
                  invoice_date = "2002-01-01T00:00:00.00Z";
                }

                let invoice_type_id = record["instance_id"];
                let invoice_type_name = "default_invoice";
                if (record["invoiceType"]) {
                  invoice_type_id = record["invoiceType"]["id"];
                  invoice_type_name = record["invoiceType"]["name"];
                }

                invoice_final_data_pool.push({
                  id: record["id"],
                  syncStatus: record["syncStatus"]
                    ? record["syncStatus"]
                    : "default",
                  date: invoice_date,
                  dueDate: dueDate,
                  subtotal: record["subTotal"] ? record["subTotal"] : 0,
                  tax: record["salesTax"] ? record["salesTax"] : 0,
                  total: record["total"] ? record["total"] : 0,
                  balance: record["balance"] ? record["balance"] : 0,
                  depositedOn: depositedOn,
                  createdOn: createdOn,
                  modifiedOn: modifiedOn,
                  invoice_type_id: invoice_type_id,
                  invoice_type_name: invoice_type_name,
                  job_details_id: job_details_id,
                  actual_job_details_id: actual_job_details_id,
                  project_id: project_id,
                  actual_project_id: actual_project_id,
                  business_unit_id: business_unit_id,
                  actual_business_unit_id: actual_business_unit_id,
                  location_id: location_id,
                  actual_location_id: actual_location_id,
                  address_street: address_street,
                  address_unit: address_unit,
                  address_city: address_city,
                  address_state: address_state,
                  address_country: address_country,
                  address_zip: address_zip,
                  acutal_address_zip: acutal_address_zip,
                  customer_id: customer_id,
                  actual_customer_id: actual_customer_id,
                  customer_name: customer_name,
                });

                if (record["items"]) {
                  // deleting cogs equipment
                  const cogs_equipment_query = await sql_request.query(
                    `DELETE FROM cogs_equipment WHERE invoice_id = '${record["id"]}'`
                  );

                  // deleting cogs material
                  const cogs_material_query = await sql_request.query(
                    `DELETE FROM cogs_material WHERE invoice_id = '${record["id"]}'`
                  );

                  // deleting cogs service
                  const cogs_service_query = await sql_request.query(
                    `DELETE FROM cogs_service WHERE invoice_id = '${record["id"]}'`
                  );

                  await Promise.all(
                    record["items"].map(async (items_record) => {
                      let generalLedgerAccountid = 0;
                      let generalLedgerAccountname = "default";
                      let generalLedgerAccountnumber = 0;
                      let generalLedgerAccounttype = "default";
                      let generalLedgerAccountdetailType = "default";

                      if (items_record["generalLedgerAccount"]) {
                        generalLedgerAccountid =
                          items_record["generalLedgerAccount"]["id"];
                        generalLedgerAccountname =
                          items_record["generalLedgerAccount"]["name"];
                        generalLedgerAccountnumber =
                          items_record["generalLedgerAccount"]["number"];
                        generalLedgerAccounttype =
                          items_record["generalLedgerAccount"]["type"];
                        generalLedgerAccountdetailType =
                          items_record["generalLedgerAccount"]["detailType"];
                      }

                      if (items_record["type"] == "Material") {
                        let sku_details_id = record["instance_id"];
                        let actual_sku_details_id = items_record["skuId"]
                          ? items_record["skuId"]
                          : record["instance_id"];

                        if (items_record["skuId"]) {
                          actual_sku_details_id = items_record["skuId"];

                          // checking sku availlable or not for mapping
                          const is_sku_available = await sql_request.query(
                            `SELECT id FROM sku_details WHERE id='${items_record["skuId"]}'`
                          );

                          if (is_sku_available["recordset"].length > 0) {
                            sku_details_id = items_record["skuId"];
                          }
                        }

                        cogs_material_final_data_pool.push({
                          quantity: items_record["quantity"]
                            ? items_record["quantity"]
                            : 0,
                          cost: items_record["cost"] ? items_record["cost"] : 0,
                          total_cost: items_record["totalCost"]
                            ? items_record["totalCost"]
                            : 0,
                          price: items_record["price"]
                            ? items_record["price"]
                            : 0,
                          sku_name: items_record["skuName"]
                            ? items_record["skuName"]
                            : "default",
                          sku_total: items_record["total"]
                            ? items_record["total"]
                            : 0,
                          generalLedgerAccountid: generalLedgerAccountid,
                          generalLedgerAccountname: generalLedgerAccountname,
                          generalLedgerAccountnumber:
                            generalLedgerAccountnumber,
                          generalLedgerAccounttype: generalLedgerAccounttype,
                          generalLedgerAccountdetailType:
                            generalLedgerAccountdetailType,
                          job_details_id: job_details_id,
                          actual_job_details_id: actual_job_details_id,
                          project_id: project_id,
                          actual_project_id: actual_project_id,
                          invoice_id: record["id"],
                          sku_details_id: sku_details_id,
                          actual_sku_details_id: actual_sku_details_id,
                        });
                      }

                      if (items_record["type"] == "Equipment") {
                        let sku_details_id = record["instance_id"] + 3;
                        let actual_sku_details_id = items_record["skuId"]
                          ? items_record["skuId"]
                          : record["instance_id"];

                        if (items_record["skuId"]) {
                          actual_sku_details_id = items_record["skuId"];

                          // checking sku availlable or not for mapping
                          const is_sku_available = await sql_request.query(
                            `SELECT id FROM sku_details WHERE id='${items_record["skuId"]}'`
                          );

                          if (is_sku_available["recordset"].length > 0) {
                            sku_details_id = items_record["skuId"];
                          }
                        }

                        cogs_equipment_final_data_pool.push({
                          quantity: items_record["quantity"]
                            ? items_record["quantity"]
                            : 0,
                          cost: items_record["cost"] ? items_record["cost"] : 0,
                          total_cost: items_record["totalCost"]
                            ? items_record["totalCost"]
                            : 0,
                          price: items_record["price"]
                            ? items_record["price"]
                            : 0,
                          sku_name: items_record["skuName"]
                            ? items_record["skuName"]
                            : "default",
                          sku_total: items_record["total"]
                            ? items_record["total"]
                            : 0,
                          generalLedgerAccountid: generalLedgerAccountid,
                          generalLedgerAccountname: generalLedgerAccountname,
                          generalLedgerAccountnumber:
                            generalLedgerAccountnumber,
                          generalLedgerAccounttype: generalLedgerAccounttype,
                          generalLedgerAccountdetailType:
                            generalLedgerAccountdetailType,
                          job_details_id: job_details_id,
                          actual_job_details_id: actual_job_details_id,
                          project_id: project_id,
                          actual_project_id: actual_project_id,
                          invoice_id: record["id"],
                          sku_details_id: sku_details_id,
                          actual_sku_details_id: actual_sku_details_id,
                        });
                      }

                      if (items_record["type"] == "Service") {
                        let sku_details_id = record["instance_id"] + 6;
                        let actual_sku_details_id = items_record["skuId"]
                          ? items_record["skuId"]
                          : record["instance_id"];

                        if (items_record["skuId"]) {
                          actual_sku_details_id = items_record["skuId"];

                          // checking sku availlable or not for mapping
                          const is_sku_available = await sql_request.query(
                            `SELECT id FROM sku_details WHERE id='${items_record["skuId"]}'`
                          );

                          if (is_sku_available["recordset"].length > 0) {
                            sku_details_id = items_record["skuId"];
                          }
                        }

                        cogs_services_final_data_pool.push({
                          quantity: items_record["quantity"]
                            ? items_record["quantity"]
                            : 0,
                          cost: items_record["cost"] ? items_record["cost"] : 0,
                          total_cost: items_record["totalCost"]
                            ? items_record["totalCost"]
                            : 0,
                          price: items_record["price"]
                            ? items_record["price"]
                            : 0,
                          sku_name: items_record["skuName"]
                            ? items_record["skuName"]
                            : "default",
                          sku_total: items_record["total"]
                            ? items_record["total"]
                            : 0,
                          generalLedgerAccountid: generalLedgerAccountid,
                          generalLedgerAccountname: generalLedgerAccountname,
                          generalLedgerAccountnumber:
                            generalLedgerAccountnumber,
                          generalLedgerAccounttype: generalLedgerAccounttype,
                          generalLedgerAccountdetailType:
                            generalLedgerAccountdetailType,
                          job_details_id: job_details_id,
                          actual_job_details_id: actual_job_details_id,
                          project_id: project_id,
                          actual_project_id: actual_project_id,
                          invoice_id: record["id"],
                          sku_details_id: sku_details_id,
                          actual_sku_details_id: actual_sku_details_id,
                        });
                      }
                    })
                  );
                }
              })
          );
        }

        console.log("invoice ended");

        console.log("invoice data: ", invoice_final_data_pool.length);
        if (invoice_final_data_pool.length > 0) {
          // await csv_generator(
          invoice_final_data_pool, invoice_header_data, "invoice";
          // );

          do {
            hvac_tables_responses["invoice"]["status"] =
              await hvac_merge_insertion(
                sql_request,
                invoice_final_data_pool,
                invoice_header_data,
                "invoice"
              );
          } while (hvac_tables_responses["invoice"]["status"] != "success");

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET invoice = '${hvac_tables_responses["invoice"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["invoice"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET invoice = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        console.log(
          "cogs_material data: ",
          cogs_material_final_data_pool.length
        );
        if (cogs_material_final_data_pool.length > 0) {
          // await csv_generator(
          cogs_material_final_data_pool,
            cogs_material_header_data,
            "cogs_material";
          // );

          do {
            hvac_tables_responses["cogs_material"]["status"] =
              await hvac_data_insertion(
                sql_request,
                cogs_material_final_data_pool,
                cogs_material_header_data,
                "cogs_material"
              );
          } while (
            hvac_tables_responses["cogs_material"]["status"] != "success"
          );

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET cogs_material = '${hvac_tables_responses["cogs_material"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["cogs_material"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET cogs_material = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        console.log(
          "cogs_equipment data: ",
          cogs_equipment_final_data_pool.length
        );
        if (cogs_equipment_final_data_pool.length > 0) {
          // await csv_generator(
          cogs_equipment_final_data_pool,
            cogs_equipment_header_data,
            "cogs_equipment";
          // );

          do {
            hvac_tables_responses["cogs_equipment"]["status"] =
              await hvac_data_insertion(
                sql_request,
                cogs_equipment_final_data_pool,
                cogs_equipment_header_data,
                "cogs_equipment"
              );
          } while (
            hvac_tables_responses["cogs_equipment"]["status"] != "success"
          );

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET cogs_equipment = '${hvac_tables_responses["cogs_equipment"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["cogs_equipment"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET cogs_equipment = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        console.log(
          "cogs_service data: ",
          cogs_services_final_data_pool.length
        );
        if (cogs_services_final_data_pool.length > 0) {
          // await csv_generator(
          // cogs_services_final_data_pool,
          //   cogs_service_header_data,
          //   "cogs_service";
          // );

          do {
            hvac_tables_responses["cogs_service"]["status"] =
              await hvac_data_insertion(
                sql_request,
                cogs_services_final_data_pool,
                cogs_service_header_data,
                "cogs_service"
              );
          } while (
            hvac_tables_responses["cogs_service"]["status"] != "success"
          );

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET cogs_service = '${hvac_tables_responses["cogs_service"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        } else {
          hvac_tables_responses["cogs_service"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET cogs_service = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        break;
      }

      case "cogs_labor": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const gross_pay_items_data_pool =
          data_lake[table_name]["payroll__gross-pay-items"]["data_pool"];
        const payrolls_data_pool =
          data_lake[table_name]["payroll__payrolls"]["data_pool"];

        const header_data = hvac_tables[table_name]["columns"];

        console.log("cogs_labor: ", gross_pay_items_data_pool.length);

        const batchSize = 300;

        const payroll_ids = [];

        gross_pay_items_data_pool.map((record) => {
          if (record["payrollId"]) {
            payroll_ids.push(record["payrollId"]);
          }
        });

        const unique_payroll_ids = Array.from(new Set(payroll_ids));

        // deleting all records of payroll ids
        const delete_payroll_rows = await sql_request.query(
          `DELETE FROM cogs_labor WHERE payrollId IN ('${unique_payroll_ids.join(
            `','`
          )}');`
        );

        let final_data_pool = [];

        let access_token_list = [];

        await Promise.all(
          unique_payroll_ids.map(async (current_payroll_id) => {
            // fetch the particular payroll id's records from service titan

            let gross_pay_data = [];

            const params_header_temp = JSON.parse(
              JSON.stringify(params_header)
            );

            let [payroll_id, payroll_instance_id] =
              current_payroll_id.split("_");

            payroll_instance_id = parseInt(payroll_instance_id);

            params_header_temp["payrollIds"] = String(payroll_id);
            params_header_temp["modifiedOnOrAfter"] = "";
            params_header_temp["modifiedBefore"] = "";

            const instance_name =
              instance_details[payroll_instance_id - 1]["instance_name"];
            const tenant_id =
              instance_details[payroll_instance_id - 1]["tenant_id"];
            const app_key =
              instance_details[payroll_instance_id - 1]["app_key"];
            const client_id =
              instance_details[payroll_instance_id - 1]["client_id"];
            const client_secret =
              instance_details[payroll_instance_id - 1]["client_secret"];

            // console.log("payroll_instance_id: ", payroll_instance_id);
            // console.log("instance_name: ", instance_name);
            // console.log("tenant_id: ", tenant_id);
            // console.log("app_key: ", app_key);
            // console.log("client_id: ", client_id);
            // console.log("client_secret: ", client_secret);

            if (!access_token_list[payroll_instance_id - 1]) {
              // signing a new access token in Service Titan's API
              let access_token = "";

              do {
                access_token = await getAccessToken(client_id, client_secret);
              } while (!access_token);

              access_token_list[payroll_instance_id - 1] = access_token;
            }

            // continuously fetching whole api data
            let data_pool_object = {};
            let data_pool = [];
            let page_count = 0;
            let has_error_occured = false;

            do {
              ({ data_pool_object, data_pool, page_count, has_error_occured } =
                await getAPIWholeData(
                  access_token_list[payroll_instance_id - 1],
                  app_key,
                  instance_name,
                  tenant_id,
                  "payroll",
                  "gross-pay-items",
                  params_header_temp,
                  data_pool_object,
                  data_pool,
                  page_count
                ));

              gross_pay_data = [...gross_pay_data, ...data_pool];
            } while (has_error_occured);

            for (let i = 0; i < gross_pay_data.length; i += batchSize) {
              await Promise.all(
                gross_pay_data.slice(i, i + batchSize).map(async (record) => {
                  let job_details_id = record["instance_id"];
                  let actual_job_details_id = record["instance_id"];
                  if (record["jobId"]) {
                    actual_job_details_id = record["jobId"];
                    // checking jobId availlable or not for mapping
                    const is_jobs_available = await sql_request.query(
                      `SELECT id FROM job_details WHERE id='${record["jobId"]}'`
                    );

                    if (is_jobs_available["recordset"].length > 0) {
                      job_details_id = record["jobId"];
                    }
                  }

                  let invoice_id = record["instance_id"];
                  let actual_invoice_id = record["instance_id"];
                  if (record["invoiceId"]) {
                    actual_invoice_id = record["invoiceId"];

                    // checking invoice availlable or not for mapping
                    const is_invoice_available = await sql_request.query(
                      `SELECT id FROM invoice WHERE id='${record["invoiceId"]}'`
                    );

                    if (is_invoice_available["recordset"].length > 0) {
                      invoice_id = record["invoiceId"];
                    }
                  }

                  let project_id = record["instance_id"];
                  let actual_project_id = record["projectId"]
                    ? record["projectId"]
                    : record["instance_id"];

                  // checking projects availlable or not for mapping
                  if (record["projectId"]) {
                    const is_project_available = await sql_request.query(
                      `SELECT id FROM projects WHERE id='${record["projectId"]}'`
                    );

                    if (is_project_available["recordset"].length > 0) {
                      project_id = record["projectId"];
                    }
                  }

                  let technician_id = record["instance_id"];
                  let actual_technician_id = record["employeeId"]
                    ? record["employeeId"]
                    : record["instance_id"];

                  if (record["employeeId"]) {
                    // checking technician availlable or not for mapping
                    const is_technnician_available = await sql_request.query(
                      `SELECT id FROM technician WHERE id='${record["employeeId"]}'`
                    );

                    if (is_technnician_available["recordset"].length > 0) {
                      technician_id = record["employeeId"];
                    }
                  }

                  let date = "2000-01-01T00:00:00.00Z";

                  if (record["date"]) {
                    if (
                      new Date(record["date"]) >
                      new Date("2000-01-01T00:00:00.00Z")
                    ) {
                      date = record["date"];
                    }
                  } else {
                    date = "2001-01-01T00:00:00.00Z";
                  }

                  let startedOn = "2000-01-01T00:00:00.00Z";

                  if (record["startedOn"]) {
                    if (
                      new Date(record["startedOn"]) >
                      new Date("2000-01-01T00:00:00.00Z")
                    ) {
                      startedOn = record["startedOn"];
                    }
                  } else {
                    startedOn = "2001-01-01T00:00:00.00Z";
                  }

                  let endedOn = "2000-01-01T00:00:00.00Z";

                  if (record["endedOn"]) {
                    if (
                      new Date(record["endedOn"]) >
                      new Date("2000-01-01T00:00:00.00Z")
                    ) {
                      endedOn = record["endedOn"];
                    }
                  } else {
                    endedOn = "2001-01-01T00:00:00.00Z";
                  }

                  final_data_pool.push({
                    paid_duration: record["paidDurationHours"]
                      ? record["paidDurationHours"]
                      : 0,
                    labor_cost: record["amount"] ? record["amount"] : 0,
                    activity: record["activity"]
                      ? record["activity"]
                      : "default",
                    paid_time_type: record["paidTimeType"]
                      ? record["paidTimeType"]
                      : "default",
                    date: date,
                    startedOn: startedOn,
                    endedOn: endedOn,
                    isPrevailingWageJob: record["isPrevailingWageJob"] ? 1 : 0,
                    job_details_id: job_details_id,
                    actual_job_details_id: actual_job_details_id,
                    invoice_id: invoice_id,
                    actual_invoice_id: actual_invoice_id,
                    project_id: project_id,
                    actual_project_id: actual_project_id,
                    payrollId: record["payrollId"],
                    acutal_payrollId: record["payrollId"],
                    technician_id: technician_id,
                    actual_technician_id: actual_technician_id,
                  });
                })
              );
            }
          })
        );

        console.log("cogs_labor data: ", final_data_pool.length);
        if (final_data_pool.length > 0) {
          // await csv_generator(final_data_pool, header_data, table_name);

          do {
            hvac_tables_responses["cogs_labor"]["status"] =
              await hvac_data_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (hvac_tables_responses["cogs_labor"]["status"] != "success");
        } else {
          hvac_tables_responses["cogs_labor"]["status"] = "success";

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET cogs_labor = 'no changes' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        delete data_lake[api_name];

        break;
      }

      default: {
        console.log("default");
      }
    }
  }

  await post_insertion(sql_request);
}

async function post_insertion(sql_request) {
  end_time = new Date();

  end_time.setHours(end_time.getHours() + timezoneOffsetHours);
  end_time.setMinutes(end_time.getMinutes() + timezoneOffsetMinutes);

  const timeDifferenceInMilliseconds = end_time - start_time;

  // Convert the time difference to minutes
  const timeDifferenceInMinutes = timeDifferenceInMilliseconds / (1000 * 60);

  let is_all_table_updated = "success";

  const failure_tables = [];

  // entry into auto_update table
  try {
    Object.keys(hvac_tables_responses).map((table) => {
      if (hvac_tables_responses[table]["status"] != "success") {
        failure_tables.push(table);
        is_all_table_updated = "failure";
      }
    });

    const auto_update_query = `UPDATE auto_update SET end_time = '${end_time.toISOString()}', total_minutes=${timeDifferenceInMinutes}, overall_status = '${is_all_table_updated}'  WHERE id=${lastInsertedId}`;

    await sql_request.query(auto_update_query);

    console.log("final Auto_Update log created ");
  } catch (err) {
    console.log("Error while inserting into auto_update", err);
  }

  if (is_all_table_updated == "failure") {
    // it will never get executed.. bcoz if table insertion failed, then & there we re-inserting table. so is_all_table_updated will alwys be true
    console.log("Pushing failed tables again.", failure_tables);
    console.log("==================================");
    console.log("==================================");
    console.log("==================================");
    console.log("==================================");
    console.log("==================================");
    console.log("==================================");
    console.log("==================================");

    // await flush_hvac_data(sql_request);
    // await data_processor(data_lake, sql_request, Object.keys(data_lake));
  } else {
    // free previous batch data lake and call next iteration
    data_lake = {};

    console.log("==================================");
    console.log("goint to enter auto_update");
    console.log("==================================");

    // await auto_update();
  }
}

// for automatic mass ETL
async function start_pipeline() {
  should_auto_update = false;

  start_time = new Date();

  start_time.setHours(start_time.getHours() + timezoneOffsetHours);
  start_time.setMinutes(start_time.getMinutes() + timezoneOffsetMinutes);

  // fetching all data from Service Titan's API
  const stop1 = startStopwatch("data fetching");
  await fetch_main_data(
    data_lake,
    instance_details,
    main_api_list,
    hvac_tables
  ); // taking 3 mins to fetch all data

  console.log("data_lake: ", data_lake);

  console.log("Time taken for fetching data: ", stop1());

  await azure_sql_operations(data_lake, Object.keys(hvac_tables));
}

async function flush_data_pool(is_initial_execute) {
  const sql_request = await create_sql_connection();
  await flush_hvac_schema(sql_request, is_initial_execute);
  await sql.close();
}

async function orchestrate() {
  // Step 1: Call start_pipeline
  await start_pipeline();

  do {
    // finding the next batch time
    params_header["modifiedOnOrAfter"] = params_header["modifiedBefore"];

    const next_batch_time = new Date(params_header["modifiedOnOrAfter"]);

    next_batch_time.setDate(next_batch_time.getDate() + 1);
    next_batch_time.setUTCHours(7, 0, 0, 0);

    console.log("finished batch: ", params_header["modifiedOnOrAfter"]);
    console.log("next batch: ", next_batch_time);

    const now = new Date();

    // Check if it's the next day
    // now < next_batch_time
    if (now < next_batch_time) {
      // Schedule the next call after an day
      const timeUntilNextBatch = next_batch_time - now; // Calculate milliseconds until the next day
      console.log("timer funtion entering", timeUntilNextBatch);

      await new Promise((resolve) => setTimeout(resolve, timeUntilNextBatch));
    } else {
      console.log("next batch initiated");

      now.setUTCHours(7, 0, 0, 0);

      params_header["modifiedBefore"] = now.toISOString();
      console.log("params_header: ", params_header);

      // Step 1: Call start_pipeline
      await start_pipeline();
    }

    should_auto_update = true;
  } while (should_auto_update);
}

orchestrate();
