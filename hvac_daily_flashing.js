const sql = require("mssql");
const fs = require("fs");

// modules
const create_sql_connection = require("./modules/create_sql_connection");
const create_sql_pool = require("./modules/create_sql_pool");
const getAccessToken = require("./modules/get_access_token");
const getAPIWholeData = require("./modules/get_api_whole_data");
const hvac_data_insertion = require("./modules/hvac_data_insertion");
const hvac_flat_data_insertion = require("./modules/hvac_flat_data_insertion");
const find_lenghthiest_header = require("./modules/find_lengthiest_header");
const create_hvac_schema = require("./modules/create_hvac_schema");
const flush_hvac_schema = require("./modules/flush_hvac_schema");
const flush_hvac_data = require("./modules/flush_hvac_data");
const kpi_data = require("./modules/business_units_details");

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

let createdBeforeTime = new Date();

createdBeforeTime.setUTCHours(7, 30, 0, 0);

const params_header = {
  createdOnOrAfter: "", // 2023-08-01T00:00:00.00Z
  createdBefore: createdBeforeTime.toISOString(),
  includeTotal: true,
  pageSize: 2000,
  active: "any",
};

// let cuurent_time = new Date();
// cuurent_time.setHours(cuurent_time.getHours() + timezoneOffsetHours);

// console.log("cuurent_time: ", cuurent_time);
console.log("params_header: ", params_header);

let initial_execute = true;
let lastInsertedId = 0;

let data_lake = {};
let should_auto_update = false;

const hvac_tables = {
  legal_entity: {
    // manual entry
    columns: {
      id: {
        data_type: "INT",
        constraint: { primary: true, nullable: false },
      },
      legal_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
    },
  },
  business_unit: {
    // settings business units
    columns: {
      id: {
        data_type: "INT",
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
      revenue_type: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      account_type: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      legal_entity_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
    },
  },
  campaigns: {
    columns: {
      id: {
        data_type: "INT",
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
        data_type: "INT",
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
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_business_unit_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
    },
  },
  bookings: {
    columns: {
      id: {
        data_type: "INT",
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
        data_type: "INT",
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
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_business_unit_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      campaign_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_campaign_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      job_details_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
    },
  },
  customer_details: {
    columns: {
      id: {
        data_type: "INT",
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
        data_type: "INT",
        constraint: { primary: true, nullable: false },
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
      zip: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      taxzone: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      zone_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
    },
  },
  projects: {
    columns: {
      id: {
        data_type: "INT",
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
      subStatus: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      customer_details_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_customer_details_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      location_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_location_id: {
        data_type: "INT",
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
    },
  },
  call_details: {
    columns: {
      lead_call_id: {
        data_type: "INT",
        constraint: { primary: true, nullable: false },
      },
      id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      instance_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      job_number: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      status: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      project_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_project_id: {
        data_type: "INT",
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
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_customer_details_id: {
        data_type: "INT",
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
        data_type: "INT",
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
        data_type: "INT",
        constraint: { nullable: true },
      },
      agent_externalId: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      agent_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      business_unit_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_business_unit_id: {
        data_type: "INT",
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
        data_type: "INT",
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
        data_type: "INT",
        constraint: { primary: true, nullable: false },
      },
      job_type_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      job_type_name: {
        data_type: "NVARCHAR",
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
      project_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_project_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      job_completion_time: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      business_unit_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_business_unit_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      location_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_location_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      customer_details_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_customer_details_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      campaign_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_campaign_id: {
        data_type: "INT",
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
        data_type: "INT",
        constraint: { nullable: true },
      },
      lead_call_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_lead_call_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      booking_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_booking_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      sold_by_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
    },
  },
  appointments: {
    columns: {
      id: {
        data_type: "INT",
        constraint: { primary: true, nullable: false },
      },
      job_details_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_job_details_id: {
        data_type: "INT",
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
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_customer_details_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
    },
  },
  sales_details: {
    columns: {
      id: {
        data_type: "INT",
        constraint: { primary: true, nullable: false },
      },
      name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      project_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_project_id: {
        data_type: "INT",
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
        data_type: "INT",
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
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_business_unit_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      businessUnitName: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      job_details_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_job_details_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      location_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_location_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      customer_details_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_customer_details_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
    },
  },
  vendor: {
    columns: {
      id: {
        data_type: "INT",
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
  technician: {
    columns: {
      id: {
        data_type: "INT",
        constraint: { primary: true, nullable: false },
      },
      name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      business_unit_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_business_unit_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
    },
  },
  appointment_assignments: {
    columns: {
      id: {
        data_type: "INT",
        constraint: { primary: true, nullable: false },
      },
      technician_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_technician_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      technician_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      assigned_by_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      assignedOn: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      status: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      is_paused: {
        data_type: "TINYINT",
        constraint: { nullable: true },
      },
      job_details_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_job_details_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      appointment_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_appointment_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
    },
  },
  non_job_appointments: {
    columns: {
      id: {
        data_type: "INT",
        constraint: { primary: true, nullable: false },
      },
      technician_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_technician_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      start: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
      name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      duration: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      timesheetCodeId: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      clearDispatchBoard: {
        data_type: "TINYINT",
        constraint: { nullable: true },
      },
      clearTechnicianView: {
        data_type: "TINYINT",
        constraint: { nullable: true },
      },
      removeTechnicianFromCapacityPlanning: {
        data_type: "TINYINT",
        constraint: { nullable: true },
      },
      is_all_day: {
        data_type: "TINYINT",
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
      created_by_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
    },
  },
  sku_details: {
    columns: {
      id: {
        data_type: "INT",
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
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_vendor_id: {
        data_type: "INT",
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
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_job_details_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      invoice_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      sku_details_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_sku_details_id: {
        data_type: "INT",
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
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_job_details_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      invoice_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      sku_details_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_sku_details_id: {
        data_type: "INT",
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
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_job_details_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      invoice_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      sku_details_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_sku_details_id: {
        data_type: "INT",
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
      burden_rate: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      labor_cost: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      burden_cost: {
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
      job_details_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_job_details_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      invoice_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_invoice_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      technician_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_technician_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
    },
  },
  purchase_order: {
    columns: {
      id: {
        data_type: "INT",
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
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_job_details_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      invoice_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_invoice_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      vendor_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_vendor_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
    },
  },
  invoice: {
    columns: {
      id: {
        data_type: "INT",
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
        data_type: "INT",
        constraint: { nullable: true },
      },
      invoice_type_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      job_details_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_job_details_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      business_unit_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_business_unit_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      location_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_location_id: {
        data_type: "INT",
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
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      customer_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_customer_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      customer_name: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
    },
  },
  gross_profit: {
    columns: {
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
      default: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      total: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      po_cost: {
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
      burden: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      // gross_profit: {
      //   data_type: "DECIMAL",
      //   constraint: { nullable: true },
      // },
      // gross_margin: {
      //   data_type: "DECIMAL",
      //   constraint: { nullable: true },
      // },
      units: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      labor_hours: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      invoice_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
    },
  },
};

const hvac_tables_responses = {
  legal_entity: {
    status: "",
  },
  business_unit: {
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
  projects: {
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
  appointment_assignments: {
    status: "",
  },
  non_job_appointments: {
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
  gross_profit: {
    status: "",
  },
  purchase_order: {
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
  business_unit: [
    {
      api_group: "settings",
      api_name: "business-units",
      table_name: "business_unit",
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
  appointments: [
    {
      api_group: "jpm",
      api_name: "appointments",
      table_name: "appointments",
    },
  ],
  sales_details: [
    {
      api_group: "sales",
      api_name: "estimates",
      table_name: "sales_details",
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
  appointment_assignments: [
    {
      api_group: "dispatch",
      api_name: "appointment-assignments",
      table_name: "appointment_assignments",
    },
  ],
  non_job_appointments: [
    {
      api_group: "dispatch",
      api_name: "non-job-appointments",
      table_name: "non_job_appointments",
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
      table_name: ["business_unit", "cogs_equipment", "cogs_material"],
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
                1: { id: 1, legal_name: "Expert Heating and Cooling" },
                2: { id: 2, legal_name: "Parket-Arntz Plumbing and Heating" },
                3: { id: 3, legal_name: "Family Heating and Cooling" },
              },
            };
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

  let sql_pool = "";
  do {
    sql_pool = await create_sql_pool();
  } while (!sql_pool);

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
      business_unit,
      campaigns,
      bookings,
      customer_details,
      call_details,
      [location],
      projects,
      job_details,
      appointments,
      sales_details,
      vendor,
      technician,
      appointment_assignments,
      non_job_appointments,
      sku_details,
      invoice,
      cogs_material,
      cogs_equipment,
      cogs_service,
      cogs_labor,
      purchase_order,
      gross_profit,
      overall_status)
      OUTPUT INSERTED.id -- Return the inserted ID
      VALUES ('${
        params_header["createdBefore"]
      }','${start_time.toISOString()}','${end_time}','${timeDifferenceInMinutes}','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated', 'not yet updated')`;

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
  await data_processor(data_lake, sql_request, table_list);
  console.log("Time Taken for pushing all data: ", pushing_time());

  // Close the connection pool
  await sql.close();
}

async function data_processor(data_lake, sql_request, table_list) {
  for (let api_count = 0; api_count < table_list.length; api_count++) {
    // Object.keys(data_lake).length
    const api_name = table_list[api_count];

    console.log("table_name: ", api_name);

    switch (api_name) {
      case "legal_entity": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool = data_lake[api_name]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   Object.values(data_pool),
        //   header_data,
        //   table_name
        // );

        if (initial_execute) {
          do {
            hvac_tables_responses["legal_entity"]["status"] =
              await hvac_data_insertion(
                sql_request,
                Object.values(data_pool),
                header_data,
                table_name
              );
          } while (
            hvac_tables_responses["legal_entity"]["status"] != "success"
          );

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET legal_entity = '${hvac_tables_responses["legal_entity"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        break;
      }

      case "business_unit": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool =
          data_lake[api_name]["settings__business-units"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        // [
        //   "account_type",
        //   "business_unit_name",
        //   "business_unit_official_name",
        //   "id",
        //   "revenue_type",
        //   "trade_type",
        //   "legal_entity_id",
        // ],

        let final_data_pool = [];

        if (initial_execute) {
          final_data_pool.push({
            id: 1,
            business_unit_name: "default_business_1",
            business_unit_official_name: "default_business_1",
            trade_type: "OTHR",
            revenue_type: "OTHR",
            account_type: "OTHR",
            legal_entity_id: 1,
          });

          final_data_pool.push({
            id: 2,
            business_unit_name: "default_business_2",
            business_unit_official_name: "default_business_2",
            trade_type: "OTHR",
            revenue_type: "OTHR",
            account_type: "OTHR",
            legal_entity_id: 2,
          });

          final_data_pool.push({
            id: 3,
            business_unit_name: "default_business_3",
            business_unit_official_name: "default_business_3",
            trade_type: "OTHR",
            revenue_type: "OTHR",
            account_type: "OTHR",
            legal_entity_id: 3,
          });

          // MANUAL ENTRY

          // final_data_pool.push({
          //   id: 108709,
          //   business_unit_name: "Imported Default Businessunit",
          //   business_unit_official_name:
          //     "Expert Imported Default Business Unit",
          //   trade_type: "HIS",
          //   revenue_type: "HIS",
          //   account_type: "HIS",
          //   legal_entity_id: 1,
          // });

          // final_data_pool.push({
          //   id: 1000004,
          //   business_unit_name: "Imported Businessunit",
          //   business_unit_official_name:
          //     "Expert Imported Default Business Unit",
          //   trade_type: "HIS",
          //   revenue_type: "HIS",
          //   account_type: "HIS",
          //   legal_entity_id: 1,
          // });

          // final_data_pool.push({
          //   id: 166181,
          //   business_unit_name: "Imported Default Businessunit",
          //   business_unit_official_name:
          //     "Family Imported Default Business Unit",
          //   trade_type: "HIS",
          //   revenue_type: "HIS",
          //   account_type: "HIS",
          //   legal_entity_id: 3,
          // });
        }

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];

          // console.log("id: ", record["id"]);
          // console.log("Acc type", kpi_data[record["id"]]["Account Type"]);
          // console.log("Trade type", kpi_data[record["id"]]["Trade Type"]);

          let trade_type = "";
          let revenue_type = "";
          let account_type = "";
          try {
            trade_type = kpi_data[record["id"]]["Trade Type"]
              ? kpi_data[record["id"]]["Trade Type"]
              : "";
            revenue_type = kpi_data[record["id"]]["Revenue Type"]
              ? kpi_data[record["id"]]["Revenue Type"]
              : "";
            account_type = kpi_data[record["id"]]["Account Type"]
              ? kpi_data[record["id"]]["Account Type"]
              : "";
          } catch (err) {}

          final_data_pool.push({
            id: record["id"],
            business_unit_name: record["name"]
              ? record["name"]
              : "default_business",
            business_unit_official_name: record["officialName"]
              ? record["officialName"]
              : "default_business",
            trade_type: trade_type,
            revenue_type: revenue_type,
            account_type: account_type,
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
          do {
            hvac_tables_responses["business_unit"]["status"] =
              await hvac_data_insertion(
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
        }

        break;
      }

      case "campaigns": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool =
          data_lake[api_name]["marketing__campaigns"]["data_pool"];
        const business_unit_data_pool =
          data_lake["business_unit"]["settings__business-units"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        const final_data_pool = [];

        if (initial_execute) {
          final_data_pool.push({
            id: 1,
            name: "default",
            is_active: 0,
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            category_id: 1,
            category_name: "default",
            is_category_active: 0,
            source: "default",
            medium: "default",
            business_unit_id: 1,
            actual_business_unit_id: 1,
          });

          final_data_pool.push({
            id: 2,
            name: "default",
            is_active: 0,
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            category_id: 2,
            category_name: "default",
            is_category_active: 0,
            source: "default",
            medium: "default",
            business_unit_id: 2,
            actual_business_unit_id: 2,
          });

          final_data_pool.push({
            id: 3,
            name: "default",
            is_active: 0,
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            category_id: 3,
            category_name: "default",
            is_category_active: 3,
            source: "default",
            medium: "default",
            business_unit_id: 3,
            actual_business_unit_id: 3,
          });
        }

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];
          let createdOn = "2000-01-01T00:00:00.00Z";
          let modifiedOn = "2000-01-01T00:00:00.00Z";

          // console.log("id: ", record["id"]);
          // console.log("Acc type", kpi_data[record["id"]]["Account Type"]);
          // console.log("Trade type", kpi_data[record["id"]]["Trade Type"]);

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

          let category_id = 0;
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
          if (record["businessUnit"]) {
            actual_business_unit_id = record["businessUnit"]["id"]
              ? record["businessUnit"]["id"]
              : record["instance_id"];
            if (business_unit_data_pool[record["businessUnit"]["id"]]) {
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
        });

        console.log("campaigns data: ", final_data_pool.length);

        // console.log("final data pool", final_data_pool);
        // await hvac_flat_data_insertion(
        //   sql_request,
        //   final_data_pool,
        //   header_data,
        //   table_name
        // );

        if (final_data_pool.length > 0) {
          do {
            hvac_tables_responses["campaigns"]["status"] =
              await hvac_data_insertion(
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
        }

        break;
      }

      case "bookings": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool = data_lake[api_name]["crm__bookings"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        const business_unit_data_pool =
          data_lake["business_unit"]["settings__business-units"]["data_pool"];
        const campaigns_data_pool =
          data_lake["campaigns"]["marketing__campaigns"]["data_pool"];

        let final_data_pool = [];

        if (initial_execute) {
          final_data_pool.push({
            id: 1,
            name: "default",
            source: "default",
            status: "default",
            customer_type: "default",
            start: "1999-01-01T00:00:00.00Z",
            bookingProviderId: 1,
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            address_street: "default",
            address_unit: "default",
            address_city: "default",
            address_state: "default",
            address_zip: "default",
            address_country: "default",
            business_unit_id: 1,
            actual_business_unit_id: 1,
            campaign_id: 1,
            actual_campaign_id: 1,
            job_details_id: 1,
          });

          final_data_pool.push({
            id: 2,
            name: "default",
            source: "default",
            status: "default",
            customer_type: "default",
            start: "1999-01-01T00:00:00.00Z",
            bookingProviderId: 2,
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            address_street: "default",
            address_unit: "default",
            address_city: "default",
            address_state: "default",
            address_zip: "default",
            address_country: "default",
            business_unit_id: 2,
            actual_business_unit_id: 2,
            campaign_id: 2,
            actual_campaign_id: 2,
            job_details_id: 2,
          });

          final_data_pool.push({
            id: 3,
            name: "default",
            source: "default",
            status: "default",
            customer_type: "default",
            start: "1999-01-01T00:00:00.00Z",
            bookingProviderId: 3,
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            address_street: "default",
            address_unit: "default",
            address_city: "default",
            address_state: "default",
            address_zip: "default",
            address_country: "default",
            business_unit_id: 3,
            actual_business_unit_id: 3,
            campaign_id: 3,
            actual_campaign_id: 3,
            job_details_id: 3,
          });
        }

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];

          let start = "2000-01-01T00:00:00.00Z";

          if (record["start"]) {
            if (
              new Date(record["start"]) > new Date("2000-01-01T00:00:00.00Z")
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
          if (business_unit_data_pool[record["businessUnitId"]]) {
            business_unit_id = record["businessUnitId"];
          }

          let campaign_id = record["instance_id"];
          let actual_campaign_id = record["campaignId"]
            ? record["campaignId"]
            : record["campaignId"];
          if (campaigns_data_pool[record["campaignId"]]) {
            campaign_id = record["campaignId"];
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
        });

        console.log("bookings data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          do {
            hvac_tables_responses["bookings"]["status"] =
              await hvac_data_insertion(
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
        }

        break;
      }

      case "customer_details": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool = data_lake[api_name]["crm__customers"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        if (initial_execute) {
          final_data_pool.push({
            id: 1,
            name: "default_customer_1",
            is_active: 1,
            type: "default_type",
            creation_date: "1999-01-01T00:00:00.00Z",
            address_street: "default",
            address_unit: "default",
            address_city: "default",
            address_state: "default",
            address_zip: "default",
          });

          final_data_pool.push({
            id: 2,
            name: "default_customer_2",
            is_active: 1,
            type: "default_type",
            creation_date: "1999-01-01T00:00:00.00Z",
            address_street: "default",
            address_unit: "default",
            address_city: "default",
            address_state: "default",
            address_zip: "default",
          });

          final_data_pool.push({
            id: 3,
            name: "default_customer_3",
            is_active: 1,
            type: "default_type",
            creation_date: "1999-01-01T00:00:00.00Z",
            address_street: "default",
            address_unit: "default",
            address_city: "default",
            address_state: "default",
            address_zip: "default",
          });
        }

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
          do {
            hvac_tables_responses["customer_details"]["status"] =
              await hvac_data_insertion(
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
        }

        break;
      }

      case "location": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool = data_lake[api_name]["crm__locations"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        // console.log("data_pool: ", data_pool);
        // console.log("header_data: ", header_data);

        if (initial_execute) {
          final_data_pool.push({
            id: 1,
            street: "default",
            unit: "default",
            city: "default",
            state: "default",
            zip: "default",
            taxzone: 0,
            zone_id: 0,
          });

          final_data_pool.push({
            id: 2,
            street: "default",
            unit: "default",
            city: "default",
            state: "default",
            zip: "default",
            taxzone: 0,
            zone_id: 0,
          });

          final_data_pool.push({
            id: 3,
            street: "default",
            unit: "default",
            city: "default",
            state: "default",
            zip: "default",
            taxzone: 0,
            zone_id: 0,
          });
        }

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];
          final_data_pool.push({
            id: record["id"],
            street: record["address"]["street"]
              ? record["address"]["street"]
              : "default",
            unit: record["address"]["unit"]
              ? record["address"]["unit"]
              : "default",
            city: record["address"]["city"]
              ? record["address"]["city"]
              : "default",
            state: record["address"]["state"]
              ? record["address"]["state"]
              : "default",
            zip: record["address"]["zip"]
              ? record["address"]["zip"]
              : "default",
            taxzone: record["taxZoneId"] ? record["taxZoneId"] : 0,
            zone_id: record["zoneId"] ? record["zoneId"] : 0,
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
          do {
            hvac_tables_responses["location"]["status"] =
              await hvac_data_insertion(
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
        }

        break;
      }

      case "projects": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool = data_lake[api_name]["jpm__projects"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        const customer_data_pool =
          data_lake["customer_details"]["crm__customers"]["data_pool"];
        const location_data_pool =
          data_lake["location"]["crm__locations"]["data_pool"];

        let final_data_pool = [];

        // console.log("data_pool: ", data_pool);
        // console.log("header_data: ", header_data);

        if (initial_execute) {
          final_data_pool.push({
            id: 1,
            number: "default",
            name: "default",
            status: "default",
            subStatus: "default",
            customer_details_id: 1,
            actual_customer_details_id: 1,
            location_id: 1,
            actual_location_id: 1,
            startDate: "1999-01-01T00:00:00.00Z",
            targetCompletionDate: "1999-01-01T00:00:00.00Z",
            actualCompletionDate: "1999-01-01T00:00:00.00Z",
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
          });

          final_data_pool.push({
            id: 2,
            number: "default",
            name: "default",
            status: "default",
            subStatus: "default",
            customer_details_id: 2,
            actual_customer_details_id: 2,
            location_id: 2,
            actual_location_id: 2,
            startDate: "1999-01-01T00:00:00.00Z",
            targetCompletionDate: "1999-01-01T00:00:00.00Z",
            actualCompletionDate: "1999-01-01T00:00:00.00Z",
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
          });

          final_data_pool.push({
            id: 3,
            number: "default",
            name: "default",
            status: "default",
            subStatus: "default",
            customer_details_id: 3,
            actual_customer_details_id: 3,
            location_id: 3,
            actual_location_id: 3,
            startDate: "1999-01-01T00:00:00.00Z",
            targetCompletionDate: "1999-01-01T00:00:00.00Z",
            actualCompletionDate: "1999-01-01T00:00:00.00Z",
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
          });
        }

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];

          let customer_details_id = record["instance_id"];
          let actual_customer_details_id = record["customerId"]
            ? record["customerId"]
            : record["instance_id"];
          if (customer_data_pool[record["customerId"]]) {
            customer_details_id = record["customerId"];
          }

          let location_id = record["instance_id"];
          let actual_location_id = record["locationId"]
            ? record["locationId"]
            : record["instance_id"];
          if (location_data_pool[record["locationId"]]) {
            location_id = record["locationId"];
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

          final_data_pool.push({
            id: record["id"],
            number: record["number"] ? record["number"] : "default",
            name: record["name"] ? record["name"] : "default",
            status: record["status"] ? record["status"] : "default",
            subStatus: record["subStatus"] ? record["subStatus"] : "default",
            customer_details_id: customer_details_id,
            actual_customer_details_id: actual_customer_details_id,
            location_id: location_id,
            actual_location_id: actual_location_id,
            startDate: startDate,
            targetCompletionDate: targetCompletionDate,
            actualCompletionDate: actualCompletionDate,
            createdOn: createdOn,
            modifiedOn: modifiedOn,
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

        console.log("projects data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          do {
            hvac_tables_responses["projects"]["status"] =
              await hvac_data_insertion(
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
        }

        break;
      }

      case "call_details": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const telecom_calls_data_pool =
          data_lake[table_name]["telecom__calls"]["data_pool"];
        const business_unit_data_pool =
          data_lake["business_unit"]["settings__business-units"]["data_pool"];
        const customer_data_pool =
          data_lake["customer_details"]["crm__customers"]["data_pool"];
        const appointments_data_pool =
          data_lake["appointments"]["jpm__appointments"]["data_pool"];
        const projects_data_pool =
          data_lake["projects"]["jpm__projects"]["data_pool"];

        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        // console.log("telecom_calls_data_pool: ", telecom_calls_data_pool);
        // console.log("header_data: ", header_data);

        if (initial_execute) {
          final_data_pool.push({
            lead_call_id: 1,
            id: 1,
            instance_id: 1,
            job_number: "default",
            status: "Not Known",
            project_id: 1,
            actual_project_id: 1,
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            receivedOn: "1999-01-01T00:00:00.00Z",
            duration: "00:00:00",
            from: "default",
            to: "default",
            direction: "default",
            call_type: "default",
            customer_details_id: 1,
            actual_customer_details_id: 1,
            is_customer_active: 0,
            customer_name: "default",
            street_address: "default",
            street: "default",
            unit: "default",
            city: "default",
            state: "default",
            country: "default",
            zip: "default",
            latitude: 0.0,
            longitude: 0.0,
            customer_import_id: "default",
            customer_type: "default",
            campaign_id: 1,
            campaign_category: "default",
            campaign_source: "default",
            campaign_medium: "default",
            campaign_dnis: "default",
            campaign_name: "default",
            campaign_createdOn: "1999-01-01T00:00:00.00Z",
            campaign_modifiedOn: "1999-01-01T00:00:00.00Z",
            is_campaign_active: 0,
            agent_id: 1,
            agent_externalId: 1,
            agent_name: "default",
            business_unit_id: 1,
            actual_business_unit_id: 1,
            business_unit_active: 0,
            business_unit_name: "default",
            business_unit_official_name: "default",
            type_id: 1,
            type_name: "default",
            type_modifiedOn: "1999-01-01T00:00:00.00Z",
          });

          final_data_pool.push({
            lead_call_id: 2,
            id: 2,
            instance_id: 2,
            job_number: "default",
            status: "Not Known",
            project_id: 2,
            actual_project_id: 2,
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            receivedOn: "1999-01-01T00:00:00.00Z",
            duration: "00:00:00",
            from: "default",
            to: "default",
            direction: "default",
            call_type: "default",
            customer_details_id: 2,
            actual_customer_details_id: 2,
            is_customer_active: 0,
            customer_name: "default",
            street_address: "default",
            street: "default",
            unit: "default",
            city: "default",
            state: "default",
            country: "default",
            zip: "default",
            latitude: 0.0,
            longitude: 0.0,
            customer_import_id: "default",
            customer_type: "default",
            campaign_id: 2,
            campaign_category: "default",
            campaign_source: "default",
            campaign_medium: "default",
            campaign_dnis: "default",
            campaign_name: "default",
            campaign_createdOn: "1999-01-01T00:00:00.00Z",
            campaign_modifiedOn: "1999-01-01T00:00:00.00Z",
            is_campaign_active: 0,
            agent_id: 2,
            agent_externalId: 2,
            agent_name: "default",
            business_unit_id: 2,
            actual_business_unit_id: 2,
            business_unit_active: 0,
            business_unit_name: "default",
            business_unit_official_name: "default",
            type_id: 2,
            type_name: "default",
            type_modifiedOn: "1999-01-01T00:00:00.00Z",
          });

          final_data_pool.push({
            lead_call_id: 3,
            id: 3,
            instance_id: 3,
            job_number: "default",
            status: "Not Known",
            project_id: 1,
            actual_project_id: 3,
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            receivedOn: "1999-01-01T00:00:00.00Z",
            duration: "00:00:00",
            from: "default",
            to: "default",
            direction: "default",
            call_type: "default",
            customer_details_id: 3,
            actual_customer_details_id: 3,
            is_customer_active: 0,
            customer_name: "default",
            street_address: "default",
            street: "default",
            unit: "default",
            city: "default",
            state: "default",
            country: "default",
            zip: "default",
            latitude: 0.0,
            longitude: 0.0,
            customer_import_id: "default",
            customer_type: "default",
            campaign_id: 3,
            campaign_category: "default",
            campaign_source: "default",
            campaign_medium: "default",
            campaign_dnis: "default",
            campaign_name: "default",
            campaign_createdOn: "1999-01-01T00:00:00.00Z",
            campaign_modifiedOn: "1999-01-01T00:00:00.00Z",
            is_campaign_active: 0,
            agent_id: 3,
            agent_externalId: 3,
            agent_name: "default",
            business_unit_id: 3,
            actual_business_unit_id: 3,
            business_unit_active: 0,
            business_unit_name: "default",
            business_unit_official_name: "default",
            type_id: 3,
            type_name: "default",
            type_modifiedOn: "1999-01-01T00:00:00.00Z",
          });
        }

        let lead_call_status = {};

        Object.keys(appointments_data_pool).map((record_id) => {
          const record = appointments_data_pool[record_id];

          if (record["jobId"]) {
            lead_call_status[record["jobId"]] = record["status"];
          }
        });

        Object.keys(telecom_calls_data_pool).map((record_id) => {
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
          if (projects_data_pool[record["projectId"]]) {
            project_id = record["projectId"];
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
            business_unit_active = record["businessUnit"]["active"] ? 1 : 0;
            business_unit_name = record["businessUnit"]["name"]
              ? record["businessUnit"]["name"]
              : "default";
            business_unit_official_name = record["businessUnit"]["officialName"]
              ? record["businessUnit"]["officialName"]
              : "default";
            if (business_unit_data_pool[record["businessUnit"]["id"]]) {
              business_unit_id = record["businessUnit"]["id"];
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
          let customer_type = "default";
          if (record["leadCall"]["customer"]) {
            actual_customer_details_id = record["leadCall"]["customer"]["id"];
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
                ? record["leadCall"]["customer"]["address"]["streetAddress"]
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
              country = record["leadCall"]["customer"]["address"]["country"]
                ? record["leadCall"]["customer"]["address"]["country"]
                : "default";
              zip = record["leadCall"]["customer"]["address"]["zip"]
                ? record["leadCall"]["customer"]["address"]["zip"]
                : "default";
              latitude = record["leadCall"]["customer"]["address"]["latitude"]
                ? record["leadCall"]["customer"]["address"]["latitude"]
                : 0.0;
              longitude = record["leadCall"]["customer"]["address"]["longitude"]
                ? record["leadCall"]["customer"]["address"]["longitude"]
                : 0.0;
            }

            customer_import_id = record["leadCall"]["customer"]["importId"]
              ? record["leadCall"]["customer"]["importId"]
              : "default";
            customer_type = record["leadCall"]["customer"]["type"]
              ? record["leadCall"]["customer"]["type"]
              : "default";

            if (customer_data_pool[record["leadCall"]["customer"]["id"]]) {
              customer_details_id = record["leadCall"]["customer"]["id"];
            }
          }

          let campaign_id = 0;
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
              : 0;

            if (record["leadCall"]["campaign"]["category"]) {
              campaign_category = record["leadCall"]["campaign"]["category"][
                "name"
              ]
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

          let agent_id = 0;
          let agent_externalId = 0;
          let agent_name = "default";
          if (record["leadCall"]["agent"]) {
            agent_id = record["leadCall"]["agent"]["id"]
              ? record["leadCall"]["agent"]["id"]
              : 0;
            agent_externalId = record["leadCall"]["agent"]["externalId"]
              ? record["leadCall"]["agent"]["externalId"]
              : 0;
            agent_name = record["leadCall"]["agent"]["name"]
              ? record["leadCall"]["agent"]["name"]
              : "default";
          }

          let type_id = 0;
          let type_name = "default";
          let type_modifiedOn = "2000-01-01T00:00:00.00Z";
          if (record["type"]) {
            type_id = record["type"]["id"] ? record["type"]["id"] : 0;
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

          let status = "Not Known";
          if (lead_call_status[record["jobNumber"]]) {
            status = lead_call_status[record["jobNumber"]];
          }

          final_data_pool.push({
            lead_call_id: record["leadCall"]["id"],
            id: record["id"],
            instance_id: record["instance_id"],
            job_number: record["jobNumber"] ? record["jobNumber"] : "default",
            status: status,
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
            to: record["leadCall"]["to"] ? record["leadCall"]["to"] : "default",
            direction: record["leadCall"]["direction"]
              ? record["leadCall"]["direction"]
              : "default",
            call_type: record["leadCall"]["callType"]
              ? record["leadCall"]["callType"]
              : "default",
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
        });

        // console.log("final_data_pool: ", final_data_pool);
        // console.log("header_data: ", header_data);

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   final_data_pool,
        //   Object.keys(header_data),
        //   table_name
        // );

        console.log("telecom_calls data: ", final_data_pool.length);
        // console.log("telecom_calls data: ", final_data_pool);

        if (final_data_pool.length > 0) {
          do {
            hvac_tables_responses["call_details"]["status"] =
              await hvac_data_insertion(
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
        }

        break;
      }

      case "job_details": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const jobs_data_pool = data_lake[api_name]["jpm__jobs"]["data_pool"];
        const job_types_data_pool =
          data_lake[api_name]["jpm__job-types"]["data_pool"];
        const projects_data_pool =
          data_lake["projects"]["jpm__projects"]["data_pool"];
        const business_unit_data_pool =
          data_lake["business_unit"]["settings__business-units"]["data_pool"];
        const customer_data_pool =
          data_lake["customer_details"]["crm__customers"]["data_pool"];
        const location_data_pool =
          data_lake["location"]["crm__locations"]["data_pool"];
        const call_details_data_pool =
          data_lake["call_details"]["telecom__calls"]["data_pool"];
        const campaigns_data_pool =
          data_lake["campaigns"]["marketing__campaigns"]["data_pool"];
        const bookings_data_pool =
          data_lake["bookings"]["crm__bookings"]["data_pool"];

        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        if (initial_execute) {
          final_data_pool.push({
            id: 1,
            job_type_id: 1,
            job_type_name: "default_job_1",
            job_number: "1",
            job_status: "default",
            project_id: 1,
            actual_project_id: 1,
            job_completion_time: "1999-01-01T00:00:00.00Z",
            business_unit_id: 1,
            actual_business_unit_id: 1,
            location_id: 1,
            actual_location_id: 1,
            customer_details_id: 1,
            actual_customer_details_id: 1,
            campaign_id: 1,
            actual_campaign_id: 1,
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            created_by_id: 0,
            lead_call_id: 1,
            actual_lead_call_id: 1,
            booking_id: 0,
            sold_by_id: 0,
          });

          final_data_pool.push({
            id: 2,
            job_type_id: 2,
            job_type_name: "default_job_2",
            job_number: "2",
            job_status: "default",
            project_id: 2,
            actual_project_id: 2,
            job_completion_time: "1999-01-01T00:00:00.00Z",
            business_unit_id: 2,
            actual_business_unit_id: 2,
            location_id: 2,
            actual_location_id: 2,
            customer_details_id: 2,
            actual_customer_details_id: 2,
            campaign_id: 2,
            actual_campaign_id: 2,
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            created_by_id: 0,
            lead_call_id: 2,
            actual_lead_call_id: 2,
            booking_id: 0,
            sold_by_id: 0,
          });

          final_data_pool.push({
            id: 3,
            job_type_id: 3,
            job_type_name: "default_job_3",
            job_number: "3",
            job_status: "default",
            project_id: 3,
            actual_project_id: 3,
            job_completion_time: "1999-01-01T00:00:00.00Z",
            business_unit_id: 3,
            actual_business_unit_id: 3,
            location_id: 3,
            actual_location_id: 3,
            customer_details_id: 3,
            actual_customer_details_id: 3,
            campaign_id: 3,
            actual_campaign_id: 3,
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            created_by_id: 0,
            lead_call_id: 3,
            actual_lead_call_id: 3,
            booking_id: 0,
            sold_by_id: 0,
          });
        }
        // console.log("jobs_data_pool: ", jobs_data_pool);
        // console.log("header_data: ", header_data);

        Object.keys(jobs_data_pool).map((record_id) => {
          const record = jobs_data_pool[record_id];
          let project_id = record["instance_id"];
          let actual_project_id = record["projectId"]
            ? record["projectId"]
            : record["instance_id"];
          let business_unit_id = record["instance_id"];
          let actual_business_unit_id = record["businessUnitId"]
            ? record["businessUnitId"]
            : record["instance_id"];
          let customer_details_id = record["instance_id"];
          let actual_customer_details_id = record["customerId"]
            ? record["customerId"]
            : record["instance_id"];
          let location_id = record["instance_id"];
          let actual_location_id = record["locationId"]
            ? record["locationId"]
            : record["instance_id"];
          let job_type_name = "default";
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

          if (
            business_unit_data_pool[record["businessUnitId"]]
            // ||
            // record["businessUnitId"] == 108709 ||
            // record["businessUnitId"] == 1000004 ||
            // record["businessUnitId"] == 166181
          ) {
            business_unit_id = record["businessUnitId"];
          }

          if (customer_data_pool[record["customerId"]]) {
            customer_details_id = record["customerId"];
          }

          if (location_data_pool[record["locationId"]]) {
            location_id = record["locationId"];
          }

          if (projects_data_pool[record["projectId"]]) {
            project_id = record["projectId"];
          }

          if (call_details_data_pool[record["leadCallId"]]) {
            lead_call_id = record["leadCallId"];
          }

          if (campaigns_data_pool[record["campaignId"]]) {
            campaign_id = record["campaignId"];
          }

          if (bookings_data_pool[record["bookingId"]]) {
            campaign_id = record["bookingId"];
          }

          if (job_types_data_pool[record["jobTypeId"]]) {
            job_type_name = job_types_data_pool[record["jobTypeId"]]["name"];
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
            job_type_id: record["jobTypeId"] ? record["jobTypeId"] : 0,
            job_type_name: job_type_name ? job_type_name : "default_job",
            job_number: record["jobNumber"] ? record["jobNumber"] : "default",
            job_status: record["jobStatus"],
            project_id: project_id,
            actual_project_id: actual_project_id,
            job_completion_time: job_completion_time,
            business_unit_id: business_unit_id,
            actual_business_unit_id: actual_business_unit_id,
            location_id: location_id,
            actual_location_id: actual_location_id,
            customer_details_id: customer_details_id,
            actual_customer_details_id: actual_customer_details_id,
            campaign_id: campaign_id,
            actual_campaign_id: actual_campaign_id,
            createdOn: createdOn,
            modifiedOn: modifiedOn,
            created_by_id: record["createdById"] ? record["createdById"] : 0,
            lead_call_id: lead_call_id,
            actual_lead_call_id: actual_lead_call_id,
            booking_id: booking_id,
            actual_booking_id: actual_booking_id,
            sold_by_id: record["soldById"] ? record["soldById"] : 0,
          });
        });

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
          do {
            hvac_tables_responses["job_details"]["status"] =
              await hvac_data_insertion(
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
        }

        delete data_lake["call_details"]["telecom__calls"];
        delete data_lake["campaigns"]["marketing__campaigns"];
        delete data_lake["bookings"]["crm__bookings"];

        break;
      }

      case "appointments": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool = data_lake[api_name]["jpm__appointments"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];
        const jobs_data_pool =
          data_lake["job_details"]["jpm__jobs"]["data_pool"];
        const customer_data_pool =
          data_lake["customer_details"]["crm__customers"]["data_pool"];

        let final_data_pool = [];

        if (initial_execute) {
          final_data_pool.push({
            id: 1,
            job_details_id: 1,
            actual_job_details_id: 1,
            appointmentNumber: "default",
            start: "1999-01-01T00:00:00.00Z",
            end: "1999-01-01T00:00:00.00Z",
            arrivalWindowStart: "1999-01-01T00:00:00.00Z",
            arrivalWindowEnd: "1999-01-01T00:00:00.00Z",
            status: "Not Known",
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            customer_details_id: 1,
            actual_customer_details_id: 1,
          });

          final_data_pool.push({
            id: 2,
            job_details_id: 2,
            actual_job_details_id: 2,
            appointmentNumber: "default",
            start: "1999-01-01T00:00:00.00Z",
            end: "1999-01-01T00:00:00.00Z",
            arrivalWindowStart: "1999-01-01T00:00:00.00Z",
            arrivalWindowEnd: "1999-01-01T00:00:00.00Z",
            status: "Not Known",
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            customer_details_id: 2,
            actual_customer_details_id: 2,
          });

          final_data_pool.push({
            id: 3,
            job_details_id: 3,
            actual_job_details_id: 3,
            appointmentNumber: "default",
            start: "1999-01-01T00:00:00.00Z",
            end: "1999-01-01T00:00:00.00Z",
            arrivalWindowStart: "1999-01-01T00:00:00.00Z",
            arrivalWindowEnd: "1999-01-01T00:00:00.00Z",
            status: "Not Known",
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            customer_details_id: 3,
            actual_customer_details_id: 3,
          });
        }

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];

          let job_details_id = record["instance_id"];
          let actual_job_details_id = record["jobId"]
            ? record["jobId"]
            : record["instance_id"];
          if (jobs_data_pool[record["jobId"]]) {
            job_details_id = record["jobId"];
          }

          let customer_details_id = record["instance_id"];
          let actual_customer_details_id = record["customerId"]
            ? record["customerId"]
            : record["instance_id"];

          if (customer_data_pool[record["customerId"]]) {
            customer_details_id = record["customerId"];
          }

          let start = "2000-01-01T00:00:00.00Z";

          if (record["start"]) {
            if (
              new Date(record["start"]) > new Date("2000-01-01T00:00:00.00Z")
            ) {
              start = record["start"];
            }
          } else {
            start = "2001-01-01T00:00:00.00Z";
          }

          let end = "2000-01-01T00:00:00.00Z";
          if (record["end"]) {
            if (new Date(record["end"]) > new Date("2000-01-01T00:00:00.00Z")) {
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
        });

        console.log("appointments data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          do {
            hvac_tables_responses["appointments"]["status"] =
              await hvac_data_insertion(
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
        }

        break;
      }

      case "sales_details": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool = data_lake[api_name]["sales__estimates"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];
        const business_unit_data_pool =
          data_lake["business_unit"]["settings__business-units"]["data_pool"];
        const jobs_data_pool =
          data_lake["job_details"]["jpm__jobs"]["data_pool"];
        const customer_data_pool =
          data_lake["customer_details"]["crm__customers"]["data_pool"];
        const location_data_pool =
          data_lake["location"]["crm__locations"]["data_pool"];
        const projects_data_pool =
          data_lake["projects"]["jpm__projects"]["data_pool"];

        let final_data_pool = [];

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];

          let project_id = record["instance_id"];
          let actual_project_id = record["projectId"]
            ? record["projectId"]
            : record["instance_id"];
          if (projects_data_pool[record["projectId"]]) {
            project_id = record["projectId"];
          }

          let business_unit_id = record["instance_id"];
          let acutal_business_unit_id = record["instance_id"];
          let businessUnitName = record["businessUnitName"]
            ? record["businessUnitName"]
            : "default";
          if (business_unit_data_pool[record["businessUnitId"]]) {
            acutal_business_unit_id = record["businessUnitId"];
            business_unit_id = record["businessUnitId"];
          }

          let job_details_id = record["instance_id"];
          let actual_job_details_id = record["jobId"]
            ? record["jobId"]
            : record["instance_id"];
          if (jobs_data_pool[record["jobId"]]) {
            job_details_id = record["jobId"];
          }

          let location_id = record["instance_id"];
          let actual_location_id = record["instance_id"];
          if (location_data_pool[record["locationId"]]) {
            location_id = record["locationId"];
            actual_location_id = record["locationId"];
          }

          let customer_details_id = record["instance_id"];
          let actual_customer_details_id = record["customerId"]
            ? record["customerId"]
            : record["instance_id"];
          if (customer_data_pool[record["customerId"]]) {
            customer_details_id = record["customerId"];
          }

          let soldOn = "2000-01-01T00:00:00.00Z";

          if (record["soldOn"]) {
            if (
              new Date(record["soldOn"]) > new Date("2000-01-01T00:00:00.00Z")
            ) {
              soldOn = record["soldOn"];
            }
          } else {
            soldOn = "2001-01-01T00:00:00.00Z";
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

          final_data_pool.push({
            id: record["id"],
            name: record["name"] ? record["name"] : "default",
            project_id: project_id,
            actual_project_id: actual_project_id,
            job_number: record["jobNumber"] ? record["jobNumber"] : "default",
            soldOn: soldOn,
            soldBy: record["soldBy"] ? record["soldBy"] : 0,
            is_active: record["active"] ? 1 : 0,
            subtotal: record["subtotal"] ? record["subtotal"] : 0,
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
        });

        console.log("sales_details data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          do {
            hvac_tables_responses["sales_details"]["status"] =
              await hvac_data_insertion(
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
        }

        delete data_lake[table_name]["sales__estimates"];
        delete data_lake["projects"]["jpm__projects"];

        break;
      }

      case "vendor": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool =
          data_lake[api_name]["inventory__vendors"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        if (initial_execute) {
          final_data_pool.push({
            id: 1,
            name: "default_vendor_1",
            is_active: 1,
          });

          final_data_pool.push({
            id: 2,
            name: "default_vendor_2",
            is_active: 1,
          });

          final_data_pool.push({
            id: 3,
            name: "default_vendor_3",
            is_active: 1,
          });
        }

        // console.log("data_pool: ", data_pool);
        // console.log("header_data: ", header_data);

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];
          final_data_pool.push({
            id: record["id"],
            name: record["name"] ? record["name"] : "default_vendor",
            is_active: record["active"] ? 1 : 0,
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

        console.log("vendor data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          do {
            hvac_tables_responses["vendor"]["status"] =
              await hvac_data_insertion(
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
        }

        break;
      }

      case "technician": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool =
          data_lake[api_name]["settings__technicians"]["data_pool"];
        const business_unit_data_pool =
          data_lake["business_unit"]["settings__business-units"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        if (initial_execute) {
          final_data_pool.push({
            id: 1,
            name: "default_technician_1",
            business_unit_id: 1,
            acutal_business_unit_id: 1,
          });

          final_data_pool.push({
            id: 2,
            name: "default_technician_2",
            business_unit_id: 2,
            acutal_business_unit_id: 2,
          });

          final_data_pool.push({
            id: 3,
            name: "default_technician_3",
            business_unit_id: 3,
            acutal_business_unit_id: 3,
          });
        }

        // console.log("data_pool: ", data_pool);
        // console.log("header_data: ", header_data);

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];
          let acutal_business_unit_id = record["instance_id"];
          let business_unit_id = record["instance_id"];

          if (
            business_unit_data_pool[record["businessUnitId"]]
            //  ||
            // record["businessUnitId"] == 108709 ||
            // record["businessUnitId"] == 1000004 ||
            // record["businessUnitId"] == 166181
          ) {
            business_unit_id = record["businessUnitId"];
            acutal_business_unit_id = record["businessUnitId"];
          }

          if (record["businessUnitId"]) {
            acutal_business_unit_id = record["businessUnitId"];
          }
          final_data_pool.push({
            id: record["id"],
            name: record["name"] ? record["name"] : "default_technician",
            business_unit_id: business_unit_id,
            acutal_business_unit_id: acutal_business_unit_id,
          });
        });

        console.log("techician data: ", final_data_pool.length);
        // console.log("header_data: ", header_data);

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   final_data_pool,
        //   header_data,
        //   table_name
        // );

        if (final_data_pool.length > 0) {
          do {
            hvac_tables_responses["technician"]["status"] =
              await hvac_data_insertion(
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
        }

        break;
      }

      case "appointment_assignments": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool =
          data_lake[api_name]["dispatch__appointment-assignments"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        const technician_data_pool =
          data_lake["technician"]["settings__technicians"]["data_pool"];
        const jobs_data_pool =
          data_lake["job_details"]["jpm__jobs"]["data_pool"];
        const appointments_data_pool =
          data_lake["appointments"]["jpm__appointments"]["data_pool"];

        let final_data_pool = [];

        // console.log("data_pool: ", data_pool);
        // console.log("header_data: ", header_data);

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];

          let technician_id = record["instance_id"];
          let actual_technician_id = record["technicianId"]
            ? record["technicianId"]
            : record["instance_id"];
          if (technician_data_pool[record["technicianId"]]) {
            technician_id = record["technicianId"];
          }

          let assignedOn = "2000-01-01T00:00:00.00Z";

          if (record["assignedOn"]) {
            if (
              new Date(record["assignedOn"]) >
              new Date("2000-01-01T00:00:00.00Z")
            ) {
              assignedOn = record["assignedOn"];
            }
          } else {
            assignedOn = "2001-01-01T00:00:00.00Z";
          }

          let job_details_id = record["instance_id"];
          let actual_job_details_id = record["jobId"]
            ? record["jobId"]
            : record["instance_id"];
          if (jobs_data_pool[record["jobId"]]) {
            job_details_id = record["jobId"];
          }

          let appointment_id = record["instance_id"];
          let actual_appointment_id = record["appointmentId"]
            ? record["appointmentId"]
            : record["instance_id"];
          if (appointments_data_pool[record["appointmentId"]]) {
            appointment_id = record["appointmentId"];
          }

          final_data_pool.push({
            id: record["id"],
            technician_id: technician_id,
            actual_technician_id: actual_technician_id,
            technician_name: record["technicianName"]
              ? record["technicianName"]
              : "default",
            assigned_by_id: record["assignedById"] ? record["assignedById"] : 0,
            assignedOn: assignedOn,
            status: record["status"] ? record["status"] : "default",
            is_paused: record["isPaused"] ? record["isPaused"] : 0,
            job_details_id: job_details_id,
            actual_job_details_id: actual_job_details_id,
            appointment_id: appointment_id,
            actual_appointment_id: actual_appointment_id,
          });
        });

        console.log("appointment_assignments data: ", final_data_pool.length);
        // console.log("header_data: ", header_data);

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   final_data_pool,
        //   header_data,
        //   table_name
        // );

        if (final_data_pool.length > 0) {
          do {
            hvac_tables_responses["appointment_assignments"]["status"] =
              await hvac_data_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (
            hvac_tables_responses["appointment_assignments"]["status"] !=
            "success"
          );

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET appointment_assignments = '${hvac_tables_responses["appointment_assignments"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        delete data_lake[api_name]["dispatch__appointment-assignments"];
        delete data_lake[table_name]["jpm__appointments"];

        break;
      }

      case "non_job_appointments": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool =
          data_lake[api_name]["dispatch__non-job-appointments"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        const technician_data_pool =
          data_lake["technician"]["settings__technicians"]["data_pool"];

        let final_data_pool = [];

        // console.log("data_pool: ", data_pool);
        // console.log("header_data: ", header_data);

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];

          let technician_id = record["instance_id"];
          let actual_technician_id = record["technicianId"]
            ? record["technicianId"]
            : record["instance_id"];
          if (technician_data_pool[record["technicianId"]]) {
            technician_id = record["technicianId"];
          }

          let start = "2000-01-01T00:00:00.00Z";

          if (record["start"]) {
            if (
              new Date(record["start"]) > new Date("2000-01-01T00:00:00.00Z")
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

          final_data_pool.push({
            id: record["id"],
            technician_id: technician_id,
            actual_technician_id: actual_technician_id,
            start: start,
            name: record["name"] ? record["name"] : "default",
            duration: record["duration"] ? record["duration"] : "00:00:00",
            timesheetCodeId: record["timesheetCodeId"]
              ? record["timesheetCodeId"]
              : 0,
            clearDispatchBoard: record["clearDispatchBoard"]
              ? record["clearDispatchBoard"]
              : 0,
            clearTechnicianView: record["clearTechnicianView"]
              ? record["clearTechnicianView"]
              : 0,
            removeTechnicianFromCapacityPlanning: record[
              "removeTechnicianFromCapacityPlanning"
            ]
              ? record["removeTechnicianFromCapacityPlanning"]
              : 0,
            is_all_day: record["allDay"] ? record["allDay"] : 0,
            is_active: record["active"] ? record["active"] : 0,
            createdOn: createdOn,
            created_by_id: record["createdById"] ? record["createdById"] : 0,
          });
        });

        console.log("non_job_appointments data: ", final_data_pool.length);
        // console.log("header_data: ", header_data);

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   final_data_pool,
        //   header_data,
        //   table_name
        // );

        if (final_data_pool.length > 0) {
          do {
            hvac_tables_responses["non_job_appointments"]["status"] =
              await hvac_data_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (
            hvac_tables_responses["non_job_appointments"]["status"] != "success"
          );

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET non_job_appointments = '${hvac_tables_responses["non_job_appointments"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        delete data_lake[api_name]["dispatch__non-job-appointments"];
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
        if (initial_execute) {
          final_data_pool.push({
            id: 1,
            sku_name: "default_material_1",
            sku_type: "Material",
            sku_unit_price: 0,
            vendor_id: 1,
            actual_vendor_id: 1,
          });

          final_data_pool.push({
            id: 2,
            sku_name: "default_material_2",
            sku_type: "Material",
            sku_unit_price: 0,
            vendor_id: 2,
            actual_vendor_id: 2,
          });

          final_data_pool.push({
            id: 3,
            sku_name: "default_material_3",
            sku_type: "Material",
            sku_unit_price: 0,
            vendor_id: 3,
            actual_vendor_id: 3,
          });

          final_data_pool.push({
            id: 4,
            sku_name: "default_equipment_1",
            sku_type: "Equipment",
            sku_unit_price: 0,
            vendor_id: 1,
            actual_vendor_id: 1,
          });

          final_data_pool.push({
            id: 5,
            sku_name: "default_equipment_2",
            sku_type: "Equipment",
            sku_unit_price: 0,
            vendor_id: 2,
            actual_vendor_id: 2,
          });

          final_data_pool.push({
            id: 6,
            sku_name: "default_equipment_3",
            sku_type: "Equipment",
            sku_unit_price: 0,
            vendor_id: 3,
            actual_vendor_id: 3,
          });

          final_data_pool.push({
            id: 7,
            sku_name: "default_service_1",
            sku_type: "Service",
            sku_unit_price: 0,
            vendor_id: 1,
            actual_vendor_id: 1,
          });

          final_data_pool.push({
            id: 8,
            sku_name: "default_service_2",
            sku_type: "Service",
            sku_unit_price: 0,
            vendor_id: 2,
            actual_vendor_id: 2,
          });

          final_data_pool.push({
            id: 9,
            sku_name: "default_service_3",
            sku_type: "Service",
            sku_unit_price: 0,
            vendor_id: 3,
            actual_vendor_id: 3,
          });
        }

        Object.keys(materials_data_pool).map((record_id) => {
          const record = materials_data_pool[record_id];
          let vendor_id = record["instance_id"];
          let actual_vendor_id = record["instance_id"];
          if (record["primaryVendor"]) {
            actual_vendor_id = record["primaryVendor"]["vendorId"]
              ? record["primaryVendor"]["vendorId"]
              : record["instance_id"];
            vendor_id = record["primaryVendor"]["vendorId"]
              ? record["primaryVendor"]["vendorId"]
              : record["instance_id"];
          }
          final_data_pool.push({
            id: record["id"],
            sku_name: record["code"],
            sku_type: "Material",
            sku_unit_price: record["cost"] ? parseFloat(record["cost"]) : 0,
            vendor_id: vendor_id,
            actual_vendor_id: actual_vendor_id,
          });
        });

        Object.keys(equipment_data_pool).map((record_id) => {
          const record = equipment_data_pool[record_id];
          let vendor_id = record["instance_id"];
          let actual_vendor_id = record["instance_id"];
          if (record["primaryVendor"]) {
            actual_vendor_id = record["primaryVendor"]["vendorId"]
              ? record["primaryVendor"]["vendorId"]
              : record["instance_id"];
            vendor_id = record["primaryVendor"]["vendorId"]
              ? record["primaryVendor"]["vendorId"]
              : record["instance_id"];
          }
          final_data_pool.push({
            id: record["id"],
            sku_name: record["code"] ? record["code"] : "default",
            sku_type: "Equipment",
            sku_unit_price: record["cost"] ? record["cost"] : 0,
            vendor_id: vendor_id,
            actual_vendor_id: actual_vendor_id,
          });
        });

        Object.keys(services_data_pool).map((record_id) => {
          const record = services_data_pool[record_id];

          final_data_pool.push({
            id: record["id"],
            sku_name: record["code"] ? record["code"] : "default",
            sku_type: "Service",
            sku_unit_price: record["price"] ? record["price"] : 0,
            vendor_id: 0,
            actual_vendor_id: 0,
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

        console.log("sku_details data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          do {
            hvac_tables_responses["sku_details"]["status"] =
              await hvac_data_insertion(
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
        }

        break;
      }

      case "invoice": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const invoice_data_pool =
          data_lake[api_name]["accounting__invoices"]["data_pool"];
        const jobs_data_pool =
          data_lake["job_details"]["jpm__jobs"]["data_pool"];
        const business_unit_data_pool =
          data_lake["business_unit"]["settings__business-units"]["data_pool"];
        const customer_data_pool =
          data_lake["customer_details"]["crm__customers"]["data_pool"];
        const location_data_pool =
          data_lake["location"]["crm__locations"]["data_pool"];
        const gross_pay_items_data_pool = JSON.parse(
          JSON.stringify(
            data_lake["cogs_labor"]["payroll__gross-pay-items"]["data_pool"]
          )
        );
        const payrolls_data_pool =
          data_lake["cogs_labor"]["payroll__payrolls"]["data_pool"];
        const purchase_order_data_pool = JSON.parse(
          JSON.stringify(
            data_lake["purchase_order"]["inventory__purchase-orders"][
              "data_pool"
            ]
          )
        );
        const sku_details_data_pool = {
          ...data_lake["sku_details"]["pricebook__materials"]["data_pool"],
          ...data_lake["sku_details"]["pricebook__equipment"]["data_pool"],
          ...data_lake["sku_details"]["pricebook__services"]["data_pool"],
        };

        const invoice_header_data = hvac_tables["invoice"]["columns"];
        const cogs_material_header_data =
          hvac_tables["cogs_material"]["columns"];
        const cogs_equipment_header_data =
          hvac_tables["cogs_equipment"]["columns"];
        const cogs_service_header_data = hvac_tables["cogs_service"]["columns"];
        const gross_profit_header_data = hvac_tables["gross_profit"]["columns"];
        cogs_service_header_data;

        let invoice_final_data_pool = [];
        let cogs_material_final_data_pool = [];
        let cogs_equipment_final_data_pool = [];
        let cogs_services_final_data_pool = [];
        let gross_profit_final_data_pool = [];

        // console.log("header_data: ", header_data);

        const po_and_gpi_data = {};

        const dummy_values = {
          po_cost: {
            1: 0,
            2: 0,
            3: 0,
          },
          labor_cost: {
            1: 0,
            2: 0,
            3: 0,
          },
          labor_hours: {
            1: 0,
            2: 0,
            3: 0,
          },
          burden_cost: {
            1: 0,
            2: 0,
            3: 0,
          },
        };

        // deleting purchase order_records, where jobId = null (:- for reducing time complexity )
        Object.keys(purchase_order_data_pool).map((po_record_id) => {
          const po_record = purchase_order_data_pool[po_record_id];

          if (po_record["invoiceId"] != null) {
            if (!po_and_gpi_data[po_record["invoiceId"]]) {
              po_and_gpi_data[po_record["invoiceId"]] = {
                po_total: 0,
                labor_cost: 0,
                labor_hours: 0,
                burden: 0,
              };
            }

            if (po_record["status"] != "Canceled") {
              if (!invoice_data_pool[po_record["invoiceId"]]) {
                dummy_values["po_cost"][po_record["instance_id"]] +=
                  po_record["total"];
              } else {
                po_and_gpi_data[po_record["invoiceId"]]["po_total"] +=
                  po_record["total"];
              }
            }
          } else {
            if (po_record["status"] != "Canceled") {
              dummy_values["po_cost"][po_record["instance_id"]] +=
                po_record["total"];
            }
          }
        });

        // console.log("po_and_gpi_data: ", Object.keys(po_and_gpi_data).length);

        Object.keys(gross_pay_items_data_pool).map((gpi_record_id) => {
          const gpi_record = gross_pay_items_data_pool[gpi_record_id];
          if (gpi_record["invoiceId"] != null) {
            if (!po_and_gpi_data[gpi_record["invoiceId"]]) {
              po_and_gpi_data[gpi_record["invoiceId"]] = {
                po_total: 0,
                labor_cost: 0,
                labor_hours: 0,
                burden: 0,
              };
            }

            if (!invoice_data_pool[gpi_record["invoiceId"]]) {
              dummy_values["labor_cost"][gpi_record["instance_id"]] +=
                gpi_record["amount"] ? gpi_record["amount"] : 0;

              dummy_values["labor_hours"][gpi_record["invoiceId"]] +=
                gpi_record["paidDurationHours"]
                  ? gpi_record["paidDurationHours"]
                  : 0;

              dummy_values["burden_cost"][gpi_record["instance_id"]] +=
                (gpi_record["paidDurationHours"]
                  ? gpi_record["paidDurationHours"]
                  : 0) *
                (payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
                  ? payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
                  : 0);
            } else {
              po_and_gpi_data[gpi_record["invoiceId"]]["labor_cost"] +=
                gpi_record["amount"] ? gpi_record["amount"] : 0;

              po_and_gpi_data[gpi_record["invoiceId"]]["labor_hours"] +=
                gpi_record["paidDurationHours"]
                  ? gpi_record["paidDurationHours"]
                  : 0;

              po_and_gpi_data[gpi_record["invoiceId"]]["burden"] +=
                (gpi_record["paidDurationHours"]
                  ? gpi_record["paidDurationHours"]
                  : 0) *
                (payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
                  ? payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
                  : 0);
            }
          } else {
            dummy_values["labor_cost"][gpi_record["instance_id"]] += gpi_record[
              "amount"
            ]
              ? gpi_record["amount"]
              : 0;

            dummy_values["labor_hours"][gpi_record["invoiceId"]] += gpi_record[
              "paidDurationHours"
            ]
              ? gpi_record["paidDurationHours"]
              : 0;

            dummy_values["burden_cost"][gpi_record["instance_id"]] +=
              (gpi_record["paidDurationHours"]
                ? gpi_record["paidDurationHours"]
                : 0) *
              (payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
                ? payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
                : 0);
          }
        });

        if (initial_execute) {
          invoice_final_data_pool.push({
            id: 1,
            syncStatus: "default",
            date: "1999-01-01T00:00:00.00Z",
            dueDate: "1999-01-01T00:00:00.00Z",
            subtotal: 0,
            tax: 0,
            total: 0,
            balance: 0,
            depositedOn: "1999-01-01T00:00:00.00Z",
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            invoice_type_id: 0,
            invoice_type_name: "default_invoice",
            job_details_id: 1,
            actual_job_details_id: 1,
            business_unit_id: 1,
            actual_business_unit_id: 1,
            location_id: 1,
            actual_location_id: 1,
            address_street: "default",
            address_unit: "default",
            address_city: "default",
            address_state: "default",
            address_country: "default",
            address_zip: "default",
            customer_id: 1,
            actual_customer_id: 1,
            customer_name: "default",
          });

          invoice_final_data_pool.push({
            id: 2,
            syncStatus: "default",
            date: "1999-01-01T00:00:00.00Z",
            dueDate: "1999-01-01T00:00:00.00Z",
            subtotal: 0,
            tax: 0,
            total: 0,
            balance: 0,
            depositedOn: "1999-01-01T00:00:00.00Z",
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            invoice_type_id: 0,
            invoice_type_name: "default_invoice",
            job_details_id: 2,
            actual_job_details_id: 2,
            business_unit_id: 2,
            actual_business_unit_id: 2,
            location_id: 2,
            actual_location_id: 2,
            address_street: "default",
            address_unit: "default",
            address_city: "default",
            address_state: "default",
            address_country: "default",
            address_zip: "default",
            customer_id: 2,
            actual_customer_id: 2,
            customer_name: "default",
          });

          invoice_final_data_pool.push({
            id: 3,
            syncStatus: "default",
            date: "1999-01-01T00:00:00.00Z",
            dueDate: "1999-01-01T00:00:00.00Z",
            subtotal: 0,
            tax: 0,
            total: 0,
            balance: 0,
            depositedOn: "1999-01-01T00:00:00.00Z",
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            invoice_type_id: 0,
            invoice_type_name: "default_invoice",
            job_details_id: 3,
            actual_job_details_id: 3,
            business_unit_id: 3,
            actual_business_unit_id: 3,
            location_id: 3,
            actual_location_id: 3,
            address_street: "default",
            address_unit: "default",
            address_city: "default",
            address_state: "default",
            address_country: "default",
            address_zip: "default",
            customer_id: 3,
            actual_customer_id: 3,
            customer_name: "default",
          });

          cogs_material_final_data_pool.push({
            quantity: 0,
            cost: 0,
            total_cost: 0,
            price: 0,
            sku_name: "default",
            sku_total: 0,
            generalLedgerAccountid: 1,
            generalLedgerAccountname: "default",
            generalLedgerAccountnumber: 1,
            generalLedgerAccounttype: "default",
            generalLedgerAccountdetailType: "default",
            job_details_id: 1,
            actual_job_details_id: 1,
            invoice_id: 1,
            sku_details_id: 1,
            actual_sku_details_id: 1,
          });

          cogs_material_final_data_pool.push({
            quantity: 0,
            cost: 0,
            total_cost: 0,
            price: 0,
            sku_name: "default",
            sku_total: 0,
            generalLedgerAccountid: 2,
            generalLedgerAccountname: "default",
            generalLedgerAccountnumber: 2,
            generalLedgerAccounttype: "default",
            generalLedgerAccountdetailType: "default",
            job_details_id: 2,
            actual_job_details_id: 2,
            invoice_id: 2,
            sku_details_id: 2,
            actual_sku_details_id: 2,
          });

          cogs_material_final_data_pool.push({
            quantity: 0,
            cost: 0,
            total_cost: 0,
            price: 0,
            sku_name: "default",
            sku_total: 0,
            generalLedgerAccountid: 3,
            generalLedgerAccountname: "default",
            generalLedgerAccountnumber: 3,
            generalLedgerAccounttype: "default",
            generalLedgerAccountdetailType: "default",
            job_details_id: 3,
            actual_job_details_id: 3,
            invoice_id: 3,
            sku_details_id: 3,
            actual_sku_details_id: 3,
          });

          cogs_equipment_final_data_pool.push({
            quantity: 0,
            cost: 0,
            total_cost: 0,
            price: 0,
            sku_name: "default",
            sku_total: 0,
            generalLedgerAccountid: 1,
            generalLedgerAccountname: "default",
            generalLedgerAccountnumber: 1,
            generalLedgerAccounttype: "default",
            generalLedgerAccountdetailType: "default",
            job_details_id: 1,
            actual_job_details_id: 1,
            invoice_id: 1,
            sku_details_id: 1,
            actual_sku_details_id: 1,
          });

          cogs_equipment_final_data_pool.push({
            quantity: 0,
            cost: 0,
            total_cost: 0,
            price: 0,
            sku_name: "default",
            sku_total: 0,
            generalLedgerAccountid: 2,
            generalLedgerAccountname: "default",
            generalLedgerAccountnumber: 2,
            generalLedgerAccounttype: "default",
            generalLedgerAccountdetailType: "default",
            job_details_id: 2,
            actual_job_details_id: 2,
            invoice_id: 2,
            sku_details_id: 2,
            actual_sku_details_id: 2,
          });

          cogs_equipment_final_data_pool.push({
            quantity: 0,
            cost: 0,
            total_cost: 0,
            price: 0,
            sku_name: "default",
            sku_total: 0,
            generalLedgerAccountid: 3,
            generalLedgerAccountname: "default",
            generalLedgerAccountnumber: 3,
            generalLedgerAccounttype: "default",
            generalLedgerAccountdetailType: "default",
            job_details_id: 3,
            actual_job_details_id: 3,
            invoice_id: 3,
            sku_details_id: 3,
            actual_sku_details_id: 3,
          });

          cogs_services_final_data_pool.push({
            quantity: 0,
            cost: 0,
            total_cost: 0,
            price: 0,
            sku_name: "default",
            sku_total: 0,
            generalLedgerAccountid: 1,
            generalLedgerAccountname: "default",
            generalLedgerAccountnumber: 1,
            generalLedgerAccounttype: "default",
            generalLedgerAccountdetailType: "default",
            job_details_id: 1,
            actual_job_details_id: 1,
            invoice_id: 1,
            sku_details_id: 1,
            actual_sku_details_id: 1,
          });

          cogs_services_final_data_pool.push({
            quantity: 0,
            cost: 0,
            total_cost: 0,
            price: 0,
            sku_name: "default",
            sku_total: 0,
            generalLedgerAccountid: 2,
            generalLedgerAccountname: "default",
            generalLedgerAccountnumber: 2,
            generalLedgerAccounttype: "default",
            generalLedgerAccountdetailType: "default",
            job_details_id: 2,
            actual_job_details_id: 2,
            invoice_id: 2,
            sku_details_id: 2,
            actual_sku_details_id: 2,
          });

          cogs_services_final_data_pool.push({
            quantity: 0,
            cost: 0,
            total_cost: 0,
            price: 0,
            sku_name: "default",
            sku_total: 0,
            generalLedgerAccountid: 3,
            generalLedgerAccountname: "default",
            generalLedgerAccountnumber: 3,
            generalLedgerAccounttype: "default",
            generalLedgerAccountdetailType: "default",
            job_details_id: 3,
            actual_job_details_id: 3,
            invoice_id: 3,
            sku_details_id: 3,
            actual_sku_details_id: 3,
          });

          gross_profit_final_data_pool.push({
            accounts_receivable: 0,
            expense: 0,
            income: 0,
            current_liability: 0,
            membership_liability: 0,
            default: 0,
            total: 0,
            po_cost: dummy_values["po_cost"][1], // purchase orders
            equipment_cost: 0, //
            material_cost: 0, //
            labor_cost: dummy_values["labor_cost"][1], // cogs_labor burden cost, labor cost, paid duration
            burden: dummy_values["burden_cost"][1], // cogs_labor
            // gross_profit: gross_profit, // invoice[total] - po - equi - mater - labor - burden
            // gross_margin: gross_margin, // gross_profit / invoice['total'] * 100 %
            units: 1, //  currently for 1
            labor_hours: dummy_values["labor_hours"][1], // cogs_labor paid duration
            invoice_id: 1,
          });

          gross_profit_final_data_pool.push({
            accounts_receivable: 0,
            expense: 0,
            income: 0,
            current_liability: 0,
            membership_liability: 0,
            default: 0,
            total: 0,
            po_cost: dummy_values["po_cost"][2], // purchase orders
            equipment_cost: 0, //
            material_cost: 0, //
            labor_cost: dummy_values["labor_cost"][2], // cogs_labor burden cost, labor cost, paid duration
            burden: dummy_values["burden_cost"][2], // cogs_labor
            // gross_profit: gross_profit, // invoice[total] - po - equi - mater - labor - burden
            // gross_margin: gross_margin, // gross_profit / invoice['total'] * 100 %
            units: 1, //  currently for 1
            labor_hours: dummy_values["labor_hours"][2], // cogs_labor paid duration
            invoice_id: 2,
          });

          gross_profit_final_data_pool.push({
            accounts_receivable: 0,
            expense: 0,
            income: 0,
            current_liability: 0,
            membership_liability: 0,
            default: 0,
            total: 0,
            po_cost: dummy_values["po_cost"][3], // purchase orders
            equipment_cost: 0, //
            material_cost: 0, //
            labor_cost: dummy_values["labor_cost"][3], // cogs_labor burden cost, labor cost, paid duration
            burden: dummy_values["burden_cost"][3], // cogs_labor
            // gross_profit: gross_profit, // invoice[total] - po - equi - mater - labor - burden
            // gross_margin: gross_margin, // gross_profit / invoice['total'] * 100 %
            units: 1, //  currently for 1
            labor_hours: dummy_values["labor_hours"][3], // cogs_labor paid duration
            invoice_id: 3,
          });
        }

        Object.keys(invoice_data_pool).map((record_id) => {
          const record = invoice_data_pool[record_id];

          let job_details_id = record["instance_id"];
          let actual_job_details_id = record["instance_id"];
          if (record["job"]) {
            actual_job_details_id = record["job"]["id"];
            if (jobs_data_pool[record["job"]["id"]]) {
              job_details_id = record["job"]["id"];
            }
          }

          let business_unit_id = record["instance_id"];
          let actual_business_unit_id = record["instance_id"];
          if (record["businessUnit"]) {
            actual_business_unit_id = record["businessUnit"]["id"];
            if (business_unit_data_pool[record["businessUnit"]["id"]]) {
              business_unit_id = record["businessUnit"]["id"];
            }
          }

          let location_id = record["instance_id"];
          let actual_location_id = record["instance_id"];
          if (record["location"]) {
            actual_location_id = record["location"]["id"];
            if (location_data_pool[record["location"]["id"]]) {
              location_id = record["location"]["id"];
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
            if (customer_data_pool[record["customer"]["id"]]) {
              customer_id = record["customer"]["id"];
            }
          }

          let address_street = "default";
          let address_unit = "default";
          let address_city = "default";
          let address_state = "default";
          let address_zip = "default";
          let address_country = "default";
          if (record["locationAddress"]) {
            address_street = record["locationAddress"]["street"]
              ? record["locationAddress"]["street"]
              : "default";
            address_unit = record["locationAddress"]["unit"]
              ? record["locationAddress"]["unit"]
              : "default";
            address_city = record["locationAddress"]["city"]
              ? record["locationAddress"]["city"]
              : "default";
            address_state = record["locationAddress"]["state"]
              ? record["locationAddress"]["state"]
              : "default";
            address_country = record["locationAddress"]["country"]
              ? record["locationAddress"]["country"]
              : "default";
            address_zip = record["locationAddress"]["zip"]
              ? record["locationAddress"]["zip"]
              : "default";
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
              new Date(record["dueDate"]) > new Date("2000-01-01T00:00:00.00Z")
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

          let invoice_type_id = 0;
          let invoice_type_name = "default_invoice";
          if (record["invoiceType"]) {
            invoice_type_id = record["invoiceType"]["id"];
            invoice_type_name = record["invoiceType"]["name"];
          }

          invoice_final_data_pool.push({
            id: record["id"],
            syncStatus: record["syncStatus"] ? record["syncStatus"] : "default",
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
            customer_id: customer_id,
            actual_customer_id: actual_customer_id,
            customer_name: customer_name,
          });

          let po_cost = 0;
          let labor_cost = 0;
          let labor_hours = 0;
          let burden = 0;

          try {
            po_cost = po_and_gpi_data[record["id"]]["po_total"];
            labor_cost = po_and_gpi_data[record["id"]]["labor_cost"];
            labor_hours = po_and_gpi_data[record["id"]]["labor_hours"];
            burden = po_and_gpi_data[record["id"]]["burden"];
          } catch (err) {
            // console.log("job_details_id: ", job_details_id);
          }

          let material_cost = 0;
          let equipment_cost = 0;

          let accounts_receivable = 0;
          let expense = 0;
          let income = 0;
          let current_liability = 0;
          let membership_liability = 0;
          let default_val = 0;

          if (record["items"]) {
            record["items"].map((items_record) => {
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
                material_cost =
                  material_cost + parseFloat(items_record["totalCost"]);

                let sku_details_id = record["instance_id"];
                let actual_sku_details_id = items_record["skuId"]
                  ? items_record["skuId"]
                  : record["instance_id"];
                if (sku_details_data_pool[items_record["skuId"]]) {
                  sku_details_id = items_record["skuId"];
                }

                cogs_material_final_data_pool.push({
                  quantity: items_record["quantity"]
                    ? items_record["quantity"]
                    : 0,
                  cost: items_record["cost"] ? items_record["cost"] : 0,
                  total_cost: items_record["totalCost"]
                    ? items_record["totalCost"]
                    : 0,
                  price: items_record["price"] ? items_record["price"] : 0,
                  sku_name: items_record["skuName"]
                    ? items_record["skuName"]
                    : "default",
                  sku_total: items_record["total"] ? items_record["total"] : 0,
                  generalLedgerAccountid: generalLedgerAccountid,
                  generalLedgerAccountname: generalLedgerAccountname,
                  generalLedgerAccountnumber: generalLedgerAccountnumber,
                  generalLedgerAccounttype: generalLedgerAccounttype,
                  generalLedgerAccountdetailType:
                    generalLedgerAccountdetailType,
                  job_details_id: job_details_id,
                  actual_job_details_id: actual_job_details_id,
                  invoice_id: record["id"],
                  sku_details_id: sku_details_id,
                  actual_sku_details_id: actual_sku_details_id,
                });
              }

              if (items_record["type"] == "Equipment") {
                equipment_cost =
                  equipment_cost + parseFloat(items_record["totalCost"]);

                let sku_details_id = record["instance_id"] + 3;
                let actual_sku_details_id = items_record["skuId"]
                  ? items_record["skuId"]
                  : record["instance_id"];
                if (sku_details_data_pool[items_record["skuId"]]) {
                  sku_details_id = items_record["skuId"];
                }

                cogs_equipment_final_data_pool.push({
                  quantity: items_record["quantity"]
                    ? items_record["quantity"]
                    : 0,
                  cost: items_record["cost"] ? items_record["cost"] : 0,
                  total_cost: items_record["totalCost"]
                    ? items_record["totalCost"]
                    : 0,
                  price: items_record["price"] ? items_record["price"] : 0,
                  sku_name: items_record["skuName"]
                    ? items_record["skuName"]
                    : "default",
                  sku_total: items_record["total"] ? items_record["total"] : 0,
                  generalLedgerAccountid: generalLedgerAccountid,
                  generalLedgerAccountname: generalLedgerAccountname,
                  generalLedgerAccountnumber: generalLedgerAccountnumber,
                  generalLedgerAccounttype: generalLedgerAccounttype,
                  generalLedgerAccountdetailType:
                    generalLedgerAccountdetailType,
                  job_details_id: job_details_id,
                  actual_job_details_id: actual_job_details_id,
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
                if (sku_details_data_pool[items_record["skuId"]]) {
                  sku_details_id = items_record["skuId"];
                }

                // for gross profit
                switch (generalLedgerAccounttype) {
                  case "Accounts Receivable": {
                    accounts_receivable += items_record["total"]
                      ? parseFloat(items_record["total"])
                      : 0;

                    break;
                  }
                  case "Current Liability": {
                    current_liability += items_record["total"]
                      ? parseFloat(items_record["total"])
                      : 0;

                    break;
                  }
                  case "Membership Liability": {
                    membership_liability += items_record["total"]
                      ? parseFloat(items_record["total"])
                      : 0;

                    break;
                  }
                  case "default": {
                    default_val += items_record["total"]
                      ? parseFloat(items_record["total"])
                      : 0;

                    break;
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
                  price: items_record["price"] ? items_record["price"] : 0,
                  sku_name: items_record["skuName"]
                    ? items_record["skuName"]
                    : "default",
                  sku_total: items_record["total"] ? items_record["total"] : 0,
                  generalLedgerAccountid: generalLedgerAccountid,
                  generalLedgerAccountname: generalLedgerAccountname,
                  generalLedgerAccountnumber: generalLedgerAccountnumber,
                  generalLedgerAccounttype: generalLedgerAccounttype,
                  generalLedgerAccountdetailType:
                    generalLedgerAccountdetailType,
                  job_details_id: job_details_id,
                  actual_job_details_id: actual_job_details_id,
                  invoice_id: record["id"],
                  sku_details_id: sku_details_id,
                  actual_sku_details_id: actual_sku_details_id,
                });
              }

              // for gross profit
              switch (generalLedgerAccounttype) {
                case "Expense": {
                  expense += items_record["total"]
                    ? parseFloat(items_record["total"])
                    : 0;

                  break;
                }
                case "Income": {
                  income += items_record["total"]
                    ? parseFloat(items_record["total"])
                    : 0;

                  break;
                }
              }
            });
          }

          let revenue = parseFloat(record["total"])
            ? parseFloat(record["total"])
            : 0;

          // let gross_profit =
          //   revenue -
          //   po_cost -
          //   equipment_cost -
          //   material_cost -
          //   labor_cost -
          //   burden;

          // let gross_margin = (Number(gross_profit) / Number(revenue)) * 100;

          // if (isNaN(gross_margin) || !isFinite(gross_margin)) {
          //   gross_margin = 0;
          // }

          if (js_date <= current_date) {
            gross_profit_final_data_pool.push({
              accounts_receivable: accounts_receivable,
              expense: expense,
              income: income,
              current_liability: current_liability,
              membership_liability: membership_liability,
              default: default_val,
              total: record["total"] ? parseFloat(record["total"]) : 0,
              po_cost: po_cost, // purchase orders
              equipment_cost: equipment_cost, //
              material_cost: material_cost, //
              labor_cost: labor_cost, // cogs_labor burden cost, labor cost, paid duration
              burden: burden, // cogs_labor
              // gross_profit: gross_profit, // invoice[total] - po - equi - mater - labor - burden
              // gross_margin: gross_margin, // gross_profit / invoice['total'] * 100 %
              units: 1, //  currently for 1
              labor_hours: labor_hours, // cogs_labor paid duration
              invoice_id: record["id"],
            });
          }
        });

        // console.log("invoice_final_data_pool: ", invoice_final_data_pool);
        // console.log(
        //   "cogs_material_final_data_pool: ",
        //   cogs_material_final_data_pool
        // );
        // console.log(
        //   "cogs_equipment_final_data_pool: ",
        //   cogs_equipment_final_data_pool
        // );
        // console.log(
        //   "gross_profit_final_data_pool: ",
        //   gross_profit_final_data_pool
        // );

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   invoice_final_data_pool,
        //   invoice_header_data,
        //   "invoice"
        // );

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   cogs_material_final_data_pool,
        //   cogs_material_header_data,
        //   "cogs_material"
        // );

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   cogs_equipment_final_data_pool,
        //   cogs_equipment_header_data,
        //   "cogs_equipment"
        // );

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   gross_profit_final_data_pool,
        //   gross_profit_header_data,
        //   "gross_profit"
        // );

        console.log("invoice data: ", invoice_final_data_pool.length);
        if (invoice_final_data_pool.length > 0) {
          do {
            hvac_tables_responses["invoice"]["status"] =
              await hvac_data_insertion(
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
        }

        console.log(
          "cogs_material data: ",
          cogs_material_final_data_pool.length
        );
        if (cogs_material_final_data_pool.length > 0) {
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
        }

        console.log(
          "cogs_equipment data: ",
          cogs_equipment_final_data_pool.length
        );
        if (cogs_equipment_final_data_pool.length > 0) {
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
        }

        console.log(
          "cogs_service data: ",
          cogs_services_final_data_pool.length
        );
        if (cogs_services_final_data_pool.length > 0) {
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
        }

        console.log("gross_profit data: ", gross_profit_final_data_pool.length);
        if (gross_profit_final_data_pool.length > 0) {
          do {
            hvac_tables_responses["gross_profit"]["status"] =
              await hvac_data_insertion(
                sql_request,
                gross_profit_final_data_pool,
                gross_profit_header_data,
                "gross_profit"
              );
          } while (
            hvac_tables_responses["gross_profit"]["status"] != "success"
          );

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET gross_profit = '${hvac_tables_responses["gross_profit"]["status"]}' WHERE id=${lastInsertedId}`;

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
        const jobs_data_pool =
          data_lake["job_details"]["jpm__jobs"]["data_pool"];
        const vendors_data_pool =
          data_lake["vendor"]["inventory__vendors"]["data_pool"];
        const invoice_data_pool =
          data_lake["invoice"]["accounting__invoices"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        // console.log("purchase_order_data_pool: ", purchase_order_data_pool);
        // console.log("header_data: ", header_data);

        if (initial_execute) {
          final_data_pool.push({
            id: 1,
            status: "default",
            total: 0,
            tax: 0,
            date: "1999-01-01T00:00:00.00Z",
            requiredOn: "1999-01-01T00:00:00.00Z",
            sentOn: "1999-01-01T00:00:00.00Z",
            receivedOn: "1999-01-01T00:00:00.00Z",
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            job_details_id: 1,
            actual_job_details_id: 1,
            invoice_id: 1,
            actual_invoice_id: 1,
            vendor_id: 1,
            actual_vendor_id: 1,
          });

          final_data_pool.push({
            id: 2,
            status: "default",
            total: 0,
            tax: 0,
            date: "1999-01-01T00:00:00.00Z",
            requiredOn: "1999-01-01T00:00:00.00Z",
            sentOn: "1999-01-01T00:00:00.00Z",
            receivedOn: "1999-01-01T00:00:00.00Z",
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            job_details_id: 2,
            actual_job_details_id: 2,
            invoice_id: 2,
            actual_invoice_id: 2,
            vendor_id: 2,
            actual_vendor_id: 2,
          });

          final_data_pool.push({
            id: 3,
            status: "default",
            total: 0,
            tax: 0,
            date: "1999-01-01T00:00:00.00Z",
            requiredOn: "1999-01-01T00:00:00.00Z",
            sentOn: "1999-01-01T00:00:00.00Z",
            receivedOn: "1999-01-01T00:00:00.00Z",
            createdOn: "1999-01-01T00:00:00.00Z",
            modifiedOn: "1999-01-01T00:00:00.00Z",
            job_details_id: 3,
            actual_job_details_id: 3,
            invoice_id: 3,
            actual_invoice_id: 3,
            vendor_id: 3,
            actual_vendor_id: 3,
          });
        }

        Object.keys(purchase_order_data_pool).map((record_id) => {
          const record = purchase_order_data_pool[record_id];

          let job_details_id = record["instance_id"];
          let actual_job_details_id = record["instance_id"];
          if (record["jobId"]) {
            actual_job_details_id = record["jobId"];
            if (jobs_data_pool[record["jobId"]]) {
              job_details_id = record["jobId"];
            }
          }

          let invoice_id = record["instance_id"];
          let actual_invoice_id = record["instance_id"];
          if (record["invoiceId"]) {
            actual_invoice_id = record["invoiceId"];
            if (invoice_data_pool[record["invoiceId"]]) {
              invoice_id = record["invoiceId"];
            }
          }

          let vendor_id = record["instance_id"];
          let actual_vendor_id = record["instance_id"];
          if (record["vendorId"]) {
            actual_vendor_id = record["vendorId"];
            if (vendors_data_pool[record["vendorId"]]) {
              vendor_id = record["vendorId"];
            }
          }

          let date = "2000-01-01T00:00:00.00Z";

          if (record["date"]) {
            if (
              new Date(record["date"]) > new Date("2000-01-01T00:00:00.00Z")
            ) {
              date = record["date"];
            }
          } else {
            date = "2001-01-01T00:00:00.00Z";
          }

          let requiredOn = "2000-01-01T00:00:00.00Z";

          if (record["requiredOn"]) {
            if (
              new Date(record["requiredOn"]) >
              new Date("2000-01-01T00:00:00.00Z")
            ) {
              requiredOn = record["requiredOn"];
            }
          } else {
            requiredOn = "2001-01-01T00:00:00.00Z";
          }

          let sentOn = "2000-01-01T00:00:00.00Z";

          if (record["sentOn"]) {
            if (
              new Date(record["sentOn"]) > new Date("2000-01-01T00:00:00.00Z")
            ) {
              sentOn = record["sentOn"];
            }
          } else {
            sentOn = "2001-01-01T00:00:00.00Z";
          }

          let receivedOn = "2000-01-01T00:00:00.00Z";

          if (record["receivedOn"]) {
            if (
              new Date(record["receivedOn"]) >
              new Date("2000-01-01T00:00:00.00Z")
            ) {
              receivedOn = record["receivedOn"];
            }
          } else {
            receivedOn = "2001-01-01T00:00:00.00Z";
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
            status: record["status"] ? record["status"] : "default",
            total: record["total"] ? record["total"] : 0,
            tax: record["tax"] ? record["tax"] : 0,
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
            vendor_id: vendor_id,
            actual_vendor_id: actual_vendor_id,
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

        console.log("purchase order data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          do {
            hvac_tables_responses["purchase_order"]["status"] =
              await hvac_data_insertion(
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
        }

        break;
      }

      case "cogs_labor": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const gross_pay_items_data_pool =
          data_lake[table_name]["payroll__gross-pay-items"]["data_pool"];
        const payrolls_data_pool =
          data_lake[table_name]["payroll__payrolls"]["data_pool"];
        const jobs_data_pool =
          data_lake["job_details"]["jpm__jobs"]["data_pool"];
        const technician_data_pool =
          data_lake["technician"]["settings__technicians"]["data_pool"];
        const invoice_data_pool =
          data_lake["invoice"]["accounting__invoices"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        // console.log("gross_pay_items_data_pool: ", gross_pay_items_data_pool);
        // console.log("payrolls_data_pool: ", payrolls_data_pool);
        // console.log("jobs_data_pool: ", jobs_data_pool);
        // console.log("technician_data_pool: ", technician_data_pool);
        // console.log("header_data: ", header_data);

        if (initial_execute) {
          final_data_pool.push({
            paid_duration: 0,
            burden_rate: 0,
            labor_cost: 0,
            burden_cost: 0,
            activity: "default",
            paid_time_type: "default",
            job_details_id: 1,
            actual_job_details_id: 1,
            invoice_id: 1,
            actual_invoice_id: 1,
            technician_id: 1,
            actual_technician_id: 1,
          });

          final_data_pool.push({
            paid_duration: 0,
            burden_rate: 0,
            labor_cost: 0,
            burden_cost: 0,
            activity: "default",
            paid_time_type: "default",
            job_details_id: 2,
            actual_job_details_id: 2,
            invoice_id: 2,
            actual_invoice_id: 2,
            technician_id: 2,
            actual_technician_id: 2,
          });

          final_data_pool.push({
            paid_duration: 0,
            burden_rate: 0,
            labor_cost: 0,
            burden_cost: 0,
            activity: "default",
            paid_time_type: "default",
            job_details_id: 3,
            actual_job_details_id: 3,
            invoice_id: 3,
            actual_invoice_id: 3,
            technician_id: 3,
            actual_technician_id: 3,
          });
        }

        gross_pay_items_data_pool.map((record) => {
          let burden_rate = payrolls_data_pool[record["payrollId"]][
            "burdenRate"
          ]
            ? payrolls_data_pool[record["payrollId"]]["burdenRate"]
            : 0.0;

          let burden_cost =
            (record["paidDurationHours"] ? record["paidDurationHours"] : 0) *
            burden_rate;

          let job_details_id = record["instance_id"];
          let actual_job_details_id = record["instance_id"];
          if (record["jobId"]) {
            actual_job_details_id = record["jobId"];
            if (jobs_data_pool[record["jobId"]]) {
              job_details_id = record["jobId"];
            }
          }

          let invoice_id = record["instance_id"];
          let actual_invoice_id = record["instance_id"];
          if (record["invoiceId"]) {
            actual_invoice_id = record["invoiceId"];
            if (invoice_data_pool[record["invoiceId"]]) {
              invoice_id = record["invoiceId"];
            }
          }

          let technician_id = record["instance_id"];
          let actual_technician_id = record["employeeId"]
            ? record["employeeId"]
            : record["instance_id"];
          if (technician_data_pool[record["employeeId"]]) {
            technician_id = record["employeeId"];
          }

          final_data_pool.push({
            paid_duration: record["paidDurationHours"]
              ? record["paidDurationHours"]
              : 0,
            burden_rate: burden_rate,
            labor_cost: record["amount"] ? record["amount"] : 0,
            burden_cost: burden_cost,
            activity: record["activity"] ? record["activity"] : "default",
            paid_time_type: record["paidTimeType"]
              ? record["paidTimeType"]
              : "default",
            job_details_id: job_details_id,
            actual_job_details_id: actual_job_details_id,
            invoice_id: invoice_id,
            actual_invoice_id: actual_invoice_id,
            technician_id: technician_id,
            actual_technician_id: actual_technician_id,
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

        console.log("cogs_labor data: ", final_data_pool.length);
        if (final_data_pool.length > 0) {
          do {
            hvac_tables_responses["cogs_labor"]["status"] =
              await hvac_data_insertion(
                sql_request,
                final_data_pool,
                header_data,
                table_name
              );
          } while (hvac_tables_responses["cogs_labor"]["status"] != "success");

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET cogs_labor = '${hvac_tables_responses["cogs_labor"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

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
    console.log("==================================");
    console.log("==================================");
    console.log("==================================");
    console.log("==================================");
    console.log("==================================");
    console.log("==================================");
    console.log("==================================");

    await flush_hvac_data(sql_request);
    await data_processor(data_lake, Object.keys(data_lake));
  } else {
    // free previous batch data lake and call next iteration
    data_lake = {};

    console.log("==================================");
    console.log("goint to enter auto_update");
    console.log("==================================");

    await auto_update();
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

  // console.log("data_lake: ", data_lake['customer_details']['crm__customers']['data_pool']);

  // await find_total_length(data_lake);

  await azure_sql_operations(data_lake, Object.keys(data_lake));
}

async function flush_data_pool(is_initial_execute) {
  const sql_request = await create_sql_connection();
  await flush_hvac_schema(sql_request, is_initial_execute);
  await sql.close();
}

async function auto_update() {
  console.log("auto_update callingg");

  // Get the current date and time // Calculate the next hour
  const previous_batch_next_day = new Date(params_header["createdBefore"]);
  previous_batch_next_day.setDate(previous_batch_next_day.getDate() + 1);

  console.log("finished batch: ", params_header["createdBefore"]);
  console.log("next batch: ", previous_batch_next_day);

  const now = new Date();
  // now.setHours(now.getHours() + timezoneOffsetHours);

  // Check if it's the next hour
  if (now < previous_batch_next_day) {
    // Schedule the next call after an hour
    const timeUntilNextBatch = previous_batch_next_day - now; // Calculate milliseconds until the next hour
    console.log("timer funtion entering", timeUntilNextBatch);

    await new Promise((resolve) => setTimeout(resolve, timeUntilNextBatch));

    await auto_update();
  } else {
    console.log("next batch initiated");

    // setting createdBefore time to current hour
    // now.setMinutes(0);
    // now.setSeconds(0);
    // now.setMilliseconds(0);

    now.setUTCHours(7, 30, 0, 0);

    params_header["createdBefore"] = now.toISOString();
    console.log("params_header: ", params_header);

    should_auto_update = true;
  }
}

// start_pipeline().then(() => {
//   console.log("should auto update ????");
//   if (should_auto_update) {
//     start_pipeline();
//   }
// });

async function orchestrate() {
  do {
    console.log("===========================================");
    console.log("===========================================");
    console.log("starting pipeline");
    console.log("should_auto_update: before", should_auto_update);
    await flush_data_pool(!should_auto_update);
    await start_pipeline();
    console.log("should_auto_update: after", should_auto_update);
  } while (should_auto_update);
}

orchestrate();

// Check the time every second
// setInterval(auto_update, 10800000);

// auto_update callingghing data: 00:33:49.075
// previous_batch_hour:  2023-10-19T17:30:00.000Z
// previous_batch_next_hour:  2023-10-19T18:30:00.000Z
// next batch initiated
// HVAC Tables deletedshing data: 00:33:52.131
// params_header:
