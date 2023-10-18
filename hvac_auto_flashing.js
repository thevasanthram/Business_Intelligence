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
const kpi_data = require("./modules/business_units_details");

const heapdump = require("heapdump");

// Insert this code at the beginning of your application's entry point
heapdump.writeSnapshot("/path/to/heapdump.heapsnapshot");

// Rest of your application startup code

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

let timezoneOffsetHours = 0; // 0 hours ahead of UTC
let timezoneOffsetMinutes = 0; // 0 minutes ahead of UTC

// Check if the date is in daylight saving time (PDT)
const today = new Date();
const daylightSavingStart = new Date(today.getFullYear(), 2, 14); // March 14
const daylightSavingEnd = new Date(today.getFullYear(), 10, 7); // November 7

if (today >= daylightSavingStart && today < daylightSavingEnd) {
  // Date is in PDT
  timezoneOffsetHours = 7;
} else {
  // Date is in PST
  timezoneOffsetHours = 8;
}

let createdBeforeTime = new Date();

createdBeforeTime.setHours(createdBeforeTime.getHours() + timezoneOffsetHours);
createdBeforeTime.setMinutes(0 - 30); // Set minutes to 0
createdBeforeTime.setSeconds(0);
createdBeforeTime.setMilliseconds(0);

console.log(createdBeforeTime.toISOString());

const params_header = {
  createdOnOrAfter: "", // 2023-08-01T00:00:00.00Z
  createdBefore: createdBeforeTime.toISOString(),
  includeTotal: true,
  pageSize: 2000,
};

console.log("params_header: ", params_header);

let initial_execute = true;
let lastInsertedId = 0;

let data_lake = {};

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
    local_table: {
      account_type: "",
      business_unit_name: "",
      business_unit_official_name: "",
      id: "",
      revenue_type: "",
      trade_type: "",
    },
    foreign_table: {
      legal_entity__id: "",
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
    local_table: {
      address_city: "",
      address_state: "",
      address_street: "",
      address_unit: "",
      address_zip: "",
      creation_date: "",
      id: "",
      is_active: "",
      name: "",
      type: "",
    },
    foreign_table: {},
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
    // crm locations
    local_table: {
      city: "",
      id: "",
      state: "",
      street: "",
      taxzone: "",
      unit: "",
      zip: "",
      zone_id: "",
    },
    foreign_table: {},
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
        constraint: { nullable: true },
      },
      booking_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      sold_by_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
    },
    // jpm -  jobs, job-types
    local_table: {
      booking_id: "",
      campaign_id: "",
      created_by_id: "",
      id: "",
      job_completion_time: "",
      job_number: "",
      job_start_time: "",
      job_type_id: "",
      job_type_name: "",
      lead_call_id: "",
      project_id: "",
      sold_by_id: "",
    },
    foreign_table: {
      business_unit__id: "",
      location__id: "",
      customer_details__id: "",
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
    // inventory vendors
    local_table: {
      id: "",
      is_active: "",
      name: "",
    },
    foreign_table: {},
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
    local_table: {
      id: "",
      name: "",
    },
    foreign_table: {
      business_unit__id: "",
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
    // materials, equipment, invoices_item
    local_table: {
      id: "",
      sku_name: "", // skuCode
      sku_type: "",
      sku_unit_price: "",
    },
    foreign_table: {
      vendor__id: "", // primaryVendor['vendorId']
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
    // invoice api-- items
    local_table: {
      id: "",
      quantity: "",
      total_cost: "",
    },
    foreign_table: {
      job_details__id: "",
      sku_details__id: "",
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
    local_table: {
      id: "",
      quantity: "",
      total_cost: "",
    },
    foreign_table: {
      job_details__id: "",
      sku_details__id: "",
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
    local_table: {
      id: "",
      quantity: "",
      total_cost: "",
    },
    foreign_table: {
      job_details__id: "",
      sku_details__id: "",
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
    // payroll - gross pay items, payrolls
    local_table: {
      activity: "",
      burden_cost: "",
      burden_rate: "",
      id: "", // running number for cogs
      labor_cost: "",
      paid_duration: "",
      paid_time_type: "",
    },
    foreign_table: {
      job_details__id: "",
      technician__id: "",
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
    // invoice
    local_table: {
      date: "",
      id: "",
      invoice_type_id: "",
      invoice_type_name: "",
      is_trialdata: "",
      subtotal: "",
      tax: "",
      total: "",
    },
    foreign_table: {
      job_details__id: "",
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
    local_table: {
      burden: "",
      equipment_cost: "",
      gross_margin: "",
      gross_profit: "",
      id: "",
      labor_cost: "",
      labor_hours: "",
      material_cost: "",
      po_cost: "",
      revenue: "",
      units: "",
    },
    foreign_table: {
      invoice__id: "",
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
  customer_details: {
    status: "",
  },
  location: {
    status: "",
  },
  job_details: {
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

    process.stdout.write(`Time elapsed for ${task_name}: ${formattedTime}\r`);

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

      // signing a new access token in Service Titan's API
      const access_token = await getAccessToken(client_id, client_secret);

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

                // continuously fetching whole api data
                const data_pool = await getAPIWholeData(
                  access_token,
                  app_key,
                  instance_name,
                  tenant_id,
                  api_group_temp,
                  api_name_temp,
                  params_header
                );

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
                    ...data_pool,
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
  const sql_request = await create_sql_connection();
  const sql_pool = await create_sql_pool();

  await create_hvac_schema(sql_request);

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
      customer_details,
      [location],
      job_details,
      vendor,
      technician,
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
      }','${start_time.toISOString()}','${end_time}','${timeDifferenceInMinutes}','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated','not yet updated', 'not yet updated')`;

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
  await data_processor(data_lake, sql_pool, sql_request, table_list);
  console.log("Time Taken for pushing all data: ", pushing_time());

  // Close the connection pool
  await sql.close();
}

async function data_processor(data_lake, sql_pool, sql_request, table_list) {
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
          hvac_tables_responses["legal_entity"]["status"] =
            await hvac_data_insertion(
              sql_request,
              Object.values(data_pool),
              header_data,
              table_name
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

          final_data_pool.push({
            id: 108709,
            business_unit_name: "Imported Default Businessunit",
            business_unit_official_name:
              "Expert Imported Default Business Unit",
            trade_type: "HIS",
            revenue_type: "HIS",
            account_type: "HIS",
            legal_entity_id: 1,
          });

          final_data_pool.push({
            id: 1000004,
            business_unit_name: "Imported Businessunit",
            business_unit_official_name:
              "Expert Imported Default Business Unit",
            trade_type: "HIS",
            revenue_type: "HIS",
            account_type: "HIS",
            legal_entity_id: 1,
          });

          final_data_pool.push({
            id: 166181,
            business_unit_name: "Imported Default Businessunit",
            business_unit_official_name:
              "Family Imported Default Business Unit",
            trade_type: "HIS",
            revenue_type: "HIS",
            account_type: "HIS",
            legal_entity_id: 3,
          });
        }

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];

          // console.log("id: ", record["id"]);
          // console.log("Acc type", kpi_data[record["id"]]["Account Type"]);
          // console.log("Trade type", kpi_data[record["id"]]["Trade Type"]);

          final_data_pool.push({
            id: record["id"],
            business_unit_name: record["name"]
              ? record["name"]
              : "default_business",
            business_unit_official_name: record["officialName"]
              ? record["officialName"]
              : "default_business",
            trade_type: kpi_data[record["id"]]["Trade Type"]
              ? kpi_data[record["id"]]["Trade Type"]
              : "",
            revenue_type: kpi_data[record["id"]]["Revenue Type"]
              ? kpi_data[record["id"]]["Revenue Type"]
              : "",
            account_type: kpi_data[record["id"]]["Account Type"]
              ? kpi_data[record["id"]]["Account Type"]
              : "",
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
          hvac_tables_responses["business_unit"]["status"] =
            await hvac_data_insertion(
              sql_request,
              final_data_pool,
              header_data,
              table_name
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
            creation_date: "2001-01-01T00:00:00.00Z",
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
            creation_date: "2001-01-01T00:00:00.00Z",
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
            creation_date: "2001-01-01T00:00:00.00Z",
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
          }

          final_data_pool.push({
            id: record["id"],
            name: record["name"] ? record["name"] : "default",
            is_active: record["active"] ? 1 : 0,
            type: record["type"] ? record["type"] : "default",
            creation_date: creation_date,
            address_street: record["address"]["street"]
              ? record["address"]["street"]
              : "default",
            address_unit: record["address"]["unit"]
              ? record["address"]["unit"]
              : "default",
            address_city: record["address"]["city"]
              ? record["address"]["city"]
              : "default",
            address_state: record["address"]["state"]
              ? record["address"]["state"]
              : "default",
            address_zip: record["address"]["zip"]
              ? record["address"]["zip"]
              : "default",
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
          hvac_tables_responses["customer_details"]["status"] =
            await hvac_data_insertion(
              sql_request,
              final_data_pool,
              header_data,
              table_name
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
          hvac_tables_responses["location"]["status"] =
            await hvac_data_insertion(
              sql_request,
              final_data_pool,
              header_data,
              table_name
            );

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

      case "job_details": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const jobs_data_pool = data_lake[api_name]["jpm__jobs"]["data_pool"];
        const job_types_data_pool =
          data_lake[api_name]["jpm__job-types"]["data_pool"];
        const business_unit_data_pool =
          data_lake["business_unit"]["settings__business-units"]["data_pool"];
        const customer_data_pool =
          data_lake["customer_details"]["crm__customers"]["data_pool"];
        const location_data_pool =
          data_lake["location"]["crm__locations"]["data_pool"];

        const header_data = hvac_tables[table_name]["columns"];

        let final_data_pool = [];

        if (initial_execute) {
          final_data_pool.push({
            id: 1,
            job_type_id: 1,
            job_type_name: "default_job_1",
            job_number: "1",
            job_status: "default",
            project_id: 0,
            job_completion_time: "2001-01-01T00:00:00.00Z",
            business_unit_id: 1,
            actual_business_unit_id: 1,
            location_id: 1,
            actual_location_id: 1,
            customer_details_id: 1,
            actual_customer_details_id: 1,
            campaign_id: 0,
            createdOn: "2001-01-01T00:00:00.00Z",
            modifiedOn: "2001-01-01T00:00:00.00Z",
            created_by_id: 0,
            leadCallId: 0,
            booking_id: 0,
            sold_by_id: 0,
          });

          final_data_pool.push({
            id: 2,
            job_type_id: 2,
            job_type_name: "default_job_2",
            job_number: "2",
            job_status: "default",
            project_id: 0,
            job_completion_time: "2001-01-01T00:00:00.00Z",
            business_unit_id: 2,
            actual_business_unit_id: 2,
            location_id: 2,
            actual_location_id: 2,
            customer_details_id: 2,
            actual_customer_details_id: 2,
            campaign_id: 0,
            createdOn: "2001-01-01T00:00:00.00Z",
            modifiedOn: "2001-01-01T00:00:00.00Z",
            created_by_id: 0,
            leadCallId: 0,
            booking_id: 0,
            sold_by_id: 0,
          });

          final_data_pool.push({
            id: 3,
            job_type_id: 3,
            job_type_name: "default_job_3",
            job_number: "3",
            job_status: "default",
            project_id: 0,
            job_completion_time: "2001-01-01T00:00:00.00Z",
            business_unit_id: 3,
            actual_business_unit_id: 3,
            location_id: 3,
            actual_location_id: 3,
            customer_details_id: 3,
            actual_customer_details_id: 3,
            campaign_id: 0,
            createdOn: "2001-01-01T00:00:00.00Z",
            modifiedOn: "2001-01-01T00:00:00.00Z",
            created_by_id: 0,
            leadCallId: 0,
            booking_id: 0,
            sold_by_id: 0,
          });
        }
        // console.log("jobs_data_pool: ", jobs_data_pool);
        // console.log("header_data: ", header_data);

        Object.keys(jobs_data_pool).map((record_id) => {
          const record = jobs_data_pool[record_id];
          let business_unit_id = record["instance_id"];
          let actual_business_unit_id = record["instance_id"];
          let customer_details_id = record["instance_id"];
          let actual_customer_details_id = record["instance_id"];
          let location_id = record["instance_id"];
          let actual_location_id = record["instance_id"];
          let job_type_name = "default";

          if (
            business_unit_data_pool[record["businessUnitId"]] ||
            record["businessUnitId"] == 108709 ||
            record["businessUnitId"] == 1000004 ||
            record["businessUnitId"] == 166181
          ) {
            business_unit_id = record["businessUnitId"];
            actual_business_unit_id = record["businessUnitId"];
          }

          if (customer_data_pool[record["customerId"]]) {
            customer_details_id = record["customerId"];
            actual_customer_details_id = record["customerId"];
          }

          if (location_data_pool[record["locationId"]]) {
            location_id = record["locationId"];
            actual_location_id = record["locationId"];
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
          }

          let modifiedOn = "2000-01-01T00:00:00.00Z";
          if (record["modifiedOn"]) {
            if (
              new Date(record["modifiedOn"]) >
              new Date("2000-01-01T00:00:00.00Z")
            ) {
              modifiedOn = record["modifiedOn"];
            }
          }

          let job_completion_time = "2000-01-01T00:00:00.00Z";

          if (record["completedOn"]) {
            if (
              new Date(record["completedOn"]) >
              new Date("2000-01-01T00:00:00.00Z")
            ) {
              job_completion_time = record["completedOn"];
            }
          }

          final_data_pool.push({
            id: record["id"],
            job_type_id: record["jobTypeId"] ? record["jobTypeId"] : 0,
            job_type_name: job_type_name ? job_type_name : "default_job",
            job_number: record["jobNumber"] ? record["jobNumber"] : "default",
            job_status: record["jobStatus"],
            project_id: record["projectId"] ? record["projectId"] : 0,
            job_completion_time: job_completion_time,
            business_unit_id: business_unit_id,
            actual_business_unit_id: actual_business_unit_id,
            location_id: location_id,
            actual_location_id: actual_location_id,
            customer_details_id: customer_details_id,
            actual_customer_details_id: actual_customer_details_id,
            campaign_id: record["campaignId"] ? record["campaignId"] : 0,
            createdOn: createdOn,
            modifiedOn: modifiedOn,
            created_by_id: record["createdById"] ? record["createdById"] : 0,
            leadCallId: record["leadCallId"] ? record["leadCallId"] : 0,
            booking_id: record["bookingId"] ? record["bookingId"] : 0,
            sold_by_id: record["soldById"] ? record["soldById"] : 0,
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

        console.log("job details data: ", final_data_pool.length);

        if (final_data_pool.length > 0) {
          hvac_tables_responses["job_details"]["status"] =
            await hvac_data_insertion(
              sql_request,
              final_data_pool,
              header_data,
              table_name
            );

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET job_details = '${hvac_tables_responses["job_details"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

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
          hvac_tables_responses["vendor"]["status"] = await hvac_data_insertion(
            sql_request,
            final_data_pool,
            header_data,
            table_name
          );

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
            business_unit_data_pool[record["businessUnitId"]] ||
            record["businessUnitId"] == 108709 ||
            record["businessUnitId"] == 1000004 ||
            record["businessUnitId"] == 166181
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
          hvac_tables_responses["technician"]["status"] =
            await hvac_data_insertion(
              sql_request,
              final_data_pool,
              header_data,
              table_name
            );

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
          hvac_tables_responses["sku_details"]["status"] =
            await hvac_data_insertion(
              sql_request,
              final_data_pool,
              header_data,
              table_name
            );

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

        // deleting purchase order_records, where jobId = null (:- for reducing time complexity )
        Object.keys(purchase_order_data_pool).map((po_record_id) => {
          const po_record = purchase_order_data_pool[po_record_id];
          if (po_record["jobId"] != null) {
            if (!po_and_gpi_data[po_record["jobId"]]) {
              po_and_gpi_data[po_record["jobId"]] = {
                po_total: 0,
                labor_cost: 0,
                labor_hours: 0,
                burden: 0,
              };
            }

            po_and_gpi_data[po_record["jobId"]]["po_total"] +=
              po_record["total"];
          }
        });

        // console.log("po_and_gpi_data: ", Object.keys(po_and_gpi_data).length);

        Object.keys(gross_pay_items_data_pool).map((gpi_record_id) => {
          const gpi_record = gross_pay_items_data_pool[gpi_record_id];
          if (!po_and_gpi_data[gpi_record["jobId"]]) {
            po_and_gpi_data[gpi_record["jobId"]] = {
              po_total: 0,
              labor_cost: 0,
              labor_hours: 0,
              burden: 0,
            };
          }

          po_and_gpi_data[gpi_record["jobId"]]["labor_cost"] += gpi_record[
            "amount"
          ]
            ? gpi_record["amount"]
            : 0;

          po_and_gpi_data[gpi_record["jobId"]]["labor_hours"] += gpi_record[
            "paidDurationHours"
          ]
            ? gpi_record["paidDurationHours"]
            : 0;

          po_and_gpi_data[gpi_record["jobId"]]["burden"] +=
            (gpi_record["paidDurationHours"]
              ? gpi_record["paidDurationHours"]
              : 0) *
            (payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
              ? payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
              : 0);
        });

        if (initial_execute) {
          invoice_final_data_pool.push({
            id: 1,
            syncStatus: "default",
            date: "2001-01-01T00:00:00.00Z",
            dueDate: "2001-01-01T00:00:00.00Z",
            subtotal: 0,
            tax: 0,
            total: 0,
            balance: 0,
            depositedOn: "2001-01-01T00:00:00.00Z",
            createdOn: "2001-01-01T00:00:00.00Z",
            modifiedOn: "2001-01-01T00:00:00.00Z",
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
            date: "2001-01-01T00:00:00.00Z",
            dueDate: "2001-01-01T00:00:00.00Z",
            subtotal: 0,
            tax: 0,
            total: 0,
            balance: 0,
            depositedOn: "2001-01-01T00:00:00.00Z",
            createdOn: "2001-01-01T00:00:00.00Z",
            modifiedOn: "2001-01-01T00:00:00.00Z",
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
            date: "2001-01-01T00:00:00.00Z",
            dueDate: "2001-01-01T00:00:00.00Z",
            subtotal: 0,
            tax: 0,
            total: 0,
            balance: 0,
            depositedOn: "2001-01-01T00:00:00.00Z",
            createdOn: "2001-01-01T00:00:00.00Z",
            modifiedOn: "2001-01-01T00:00:00.00Z",
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
        }

        Object.keys(invoice_data_pool).map((record_id) => {
          const record = invoice_data_pool[record_id];

          let job_details_id = record["instance_id"];
          let actual_job_details_id = record["instance_id"];
          if (record["job"]) {
            if (jobs_data_pool[record["job"]["id"]]) {
              job_details_id = record["job"]["id"];
              actual_job_details_id = record["job"]["id"];
            }
          }

          let business_unit_id = record["instance_id"];
          let actual_business_unit_id = record["instance_id"];
          if (record["businessUnit"]) {
            if (business_unit_data_pool[record["businessUnit"]["id"]]) {
              business_unit_id = record["businessUnit"]["id"];
              actual_business_unit_id = record["businessUnit"]["id"];
            }
          }

          let location_id = record["instance_id"];
          let actual_location_id = record["instance_id"];
          if (record["location"]) {
            if (location_data_pool[record["location"]["id"]]) {
              location_id = record["location"]["id"];
              actual_location_id = record["location"]["id"];
            }
          }

          let customer_id = record["instance_id"];
          let actual_customer_id = record["instance_id"];
          let customer_name = "default";
          if (record["customer"]) {
            if (customer_data_pool[record["customer"]["id"]]) {
              customer_id = record["customer"]["id"];
              actual_customer_id = record["customer"]["id"];
              customer_name = record["customer"]["name"];
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
          }

          let dueDate = "2000-01-01T00:00:00.00Z";

          if (record["dueDate"]) {
            if (
              new Date(record["dueDate"]) > new Date("2000-01-01T00:00:00.00Z")
            ) {
              dueDate = record["dueDate"];
            }
          }

          let depositedOn = "2000-01-01T00:00:00.00Z";

          if (record["depositedOn"]) {
            if (
              new Date(record["depositedOn"]) >
              new Date("2000-01-01T00:00:00.00Z")
            ) {
              depositedOn = record["depositedOn"];
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
          }

          let modifiedOn = "2000-01-01T00:00:00.00Z";

          if (record["modifiedOn"]) {
            if (
              new Date(record["modifiedOn"]) >
              new Date("2000-01-01T00:00:00.00Z")
            ) {
              modifiedOn = record["modifiedOn"];
            }
          }

          const js_date = new Date(invoice_date);

          const current_date = new Date();

          if (js_date <= current_date) {
            let invoice_type_id = 0;
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
          }

          let po_cost = 0;
          let labor_cost = 0;
          let labor_hours = 0;
          let burden = 0;

          try {
            po_cost = po_and_gpi_data[job_details_id]["po_total"];
            labor_cost = po_and_gpi_data[job_details_id]["labor_cost"];
            labor_hours = po_and_gpi_data[job_details_id]["labor_hours"];
            burden = po_and_gpi_data[job_details_id]["burden"];
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
                let actual_sku_details_id = record["instance_id"];
                if (sku_details_data_pool[items_record["skuId"]]) {
                  sku_details_id = items_record["skuId"];
                  actual_sku_details_id = items_record["skuId"];
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
                let actual_sku_details_id = record["instance_id"];
                if (sku_details_data_pool[items_record["skuId"]]) {
                  sku_details_id = items_record["skuId"];
                  actual_sku_details_id = items_record["skuId"];
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
                let actual_sku_details_id = record["instance_id"];
                if (sku_details_data_pool[items_record["skuId"]]) {
                  sku_details_id = items_record["skuId"];
                  actual_sku_details_id = items_record["skuId"];
                }

                // for gross profit
                switch (generalLedgerAccounttype) {
                  case "Accounts Receivable": {
                    accounts_receivable += items_record["total"]
                      ? parseFloat(items_record["total"])
                      : 0;

                    break;
                  }
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
                  job_details_id: record["instance_id"],
                  actual_job_details_id: record["instance_id"],
                  invoice_id: record["id"],
                  sku_details_id: sku_details_id,
                  actual_sku_details_id: actual_sku_details_id,
                });
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
          hvac_tables_responses["invoice"]["status"] =
            await hvac_data_insertion(
              sql_request,
              invoice_final_data_pool,
              invoice_header_data,
              "invoice"
            );

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
          hvac_tables_responses["cogs_material"]["status"] =
            await hvac_data_insertion(
              sql_request,
              cogs_material_final_data_pool,
              cogs_material_header_data,
              "cogs_material"
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
          hvac_tables_responses["cogs_equipment"]["status"] =
            await hvac_data_insertion(
              sql_request,
              cogs_equipment_final_data_pool,
              cogs_equipment_header_data,
              "cogs_equipment"
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
          hvac_tables_responses["cogs_service"]["status"] =
            await hvac_data_insertion(
              sql_request,
              cogs_services_final_data_pool,
              cogs_service_header_data,
              "cogs_service"
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
          hvac_tables_responses["gross_profit"]["status"] =
            await hvac_data_insertion(
              sql_request,
              gross_profit_final_data_pool,
              gross_profit_header_data,
              "gross_profit"
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

        Object.keys(purchase_order_data_pool).map((record_id) => {
          const record = purchase_order_data_pool[record_id];

          let job_details_id = record["instance_id"];
          let actual_job_details_id = record["instance_id"];
          if (record["jobId"]) {
            if (jobs_data_pool[record["jobId"]]) {
              job_details_id = record["jobId"];
              actual_job_details_id = record["jobId"];
            }
          }

          let invoice_id = record["instance_id"];
          let actual_invoice_id = record["instance_id"];
          if (record["invoiceId"]) {
            if (invoice_data_pool[record["invoiceId"]]) {
              invoice_id = record["invoiceId"];
              actual_invoice_id = record["invoiceId"];
            }
          }

          let vendor_id = record["instance_id"];
          let actual_vendor_id = record["instance_id"];
          if (record["vendorId"]) {
            if (vendors_data_pool[record["vendorId"]]) {
              vendor_id = record["vendorId"];
              actual_vendor_id = record["vendorId"];
            }
          }

          let date = "2000-01-01T00:00:00.00Z";

          if (record["date"]) {
            if (
              new Date(record["date"]) > new Date("2000-01-01T00:00:00.00Z")
            ) {
              date = record["date"];
            }
          }

          let requiredOn = "2000-01-01T00:00:00.00Z";

          if (record["requiredOn"]) {
            if (
              new Date(record["requiredOn"]) >
              new Date("2000-01-01T00:00:00.00Z")
            ) {
              requiredOn = record["requiredOn"];
            }
          }

          let sentOn = "2000-01-01T00:00:00.00Z";

          if (record["sentOn"]) {
            if (
              new Date(record["sentOn"]) > new Date("2000-01-01T00:00:00.00Z")
            ) {
              sentOn = record["sentOn"];
            }
          }

          let receivedOn = "2000-01-01T00:00:00.00Z";

          if (record["receivedOn"]) {
            if (
              new Date(record["receivedOn"]) >
              new Date("2000-01-01T00:00:00.00Z")
            ) {
              receivedOn = record["receivedOn"];
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
          }

          let modifiedOn = "2000-01-01T00:00:00.00Z";

          if (record["modifiedOn"]) {
            if (
              new Date(record["modifiedOn"]) >
              new Date("2000-01-01T00:00:00.00Z")
            ) {
              modifiedOn = record["modifiedOn"];
            }
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
          hvac_tables_responses["purchase_order"]["status"] =
            await hvac_data_insertion(
              sql_request,
              final_data_pool,
              header_data,
              table_name
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
            if (jobs_data_pool[record["jobId"]]) {
              job_details_id = record["jobId"];
              actual_job_details_id = record["jobId"];
            }
          }

          let invoice_id = record["instance_id"];
          let actual_invoice_id = record["instance_id"];
          if (record["invoiceId"]) {
            if (invoice_data_pool[record["invoiceId"]]) {
              invoice_id = record["invoiceId"];
              actual_invoice_id = record["invoiceId"];
            }
          }

          let technician_id = record["instance_id"];
          let actual_technician_id = record["instance_id"];
          if (technician_data_pool[record["employeeId"]]) {
            technician_id = record["employeeId"];
            actual_technician_id = record["employeeId"];
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
          hvac_tables_responses["cogs_labor"]["status"] =
            await hvac_data_insertion(
              sql_request,
              final_data_pool,
              header_data,
              table_name
            );

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

  if (!is_all_table_updated) {
    console.log("Pushing failed tables again.");
    await azure_sql_operations(data_lake, failure_tables);
  } else {
    // free previous batch data lake and call next iteration
    data_lake = {};
    await auto_update();
  }
}

// for automatic mass ETL
async function start_pipeline() {
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

async function flush_data_pool() {
  const sql_request = await create_sql_connection();
  await flush_hvac_schema(sql_request);
  await sql.close();
}

async function auto_update() {
  console.log("auto_update callingg");

  // Get the current date and time
  const previous_batch_time = new Date(params_header["createdBefore"]);
  const previous_batch_hour = previous_batch_time.getHours();

  // Calculate the next hour
  const previous_batch_next_hour = (previous_batch_hour + 1) % 24;

  const now = new Date();
  now.setHours(now.getHours() + timezoneOffsetHours);
  const currentHour = now.getHours();

  console.log("currentHour: ", currentHour);
  console.log("previous_batch_next_hour: ", previous_batch_next_hour);

  console.log("condition: ", currentHour != previous_batch_next_hour);

  // Check if it's the next hour
  if (currentHour != previous_batch_next_hour) {
    // Schedule the next call after an hour
    const timeUntilNextHour = (60 - now.getMinutes()) * 60 * 1000; // Calculate milliseconds until the next hour
    console.log("timer funtion entering", timeUntilNextHour);
    setTimeout(auto_update, timeUntilNextHour + 60000);
  } else {
    console.log("next batch initiated");
    await flush_data_pool();

    // Incrementing createdBefore time by one hour
    const createdBeforeTime = new Date(params_header["createdBefore"]);
    createdBeforeTime.setHours(currentHour);
    params_header["createdBefore"] = createdBeforeTime.toISOString();
    console.log("params_header: ", params_header);

    await start_pipeline(); // Call your function
  }
}

try {
  start_pipeline();
} catch (err) {
  console.log("error: ", err);
  start_pipeline();
}

// Check the time every second
// setInterval(auto_update, 10800000);
