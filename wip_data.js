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
const { parse } = require("path");
const { start } = require("repl");

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

let createdBeforeTime = new Date();

createdBeforeTime.setUTCHours(7, 0, 0, 0);

const params_header = {
  modifiedOnOrAfter: "", // "2024-02-12T00:00:00.00Z"
  modifiedBefore: "", //createdBeforeTime.toISOString()
  includeTotal: true,
  pageSize: 2000,
  active: "any",
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
        data_type: "INT",
        constraint: { nullable: false },
      },
    },
  },
  employees: {
    columns: {
      id: {
        data_type: "INT",
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
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_business_unit_id: {
        data_type: "INT",
        constraint: { nullable: true },
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
        data_type: "INT",
        constraint: { nullable: true },
      },
      zone_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
    },
  },
  gross_pay_items: {
    columns: {
      id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      payrollId: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      amount: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      paidDurationHours: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
      projectId: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      invoiceId: {
        data_type: "INT",
        constraint: { nullable: true },
      },
    },
  },
  payrolls: {
    columns: {
      id: {
        data_type: "INT",
        constraint: { primary: true, nullable: false },
      },
      burdenRate: {
        data_type: "DECIMAL",
        constraint: { nullable: true },
      },
    },
  },
  job_types: {
    columns: {
      id: {
        data_type: "INT",
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
        data_type: "INT",
        constraint: { primary: true, nullable: false },
      },
      status: {
        data_type: "NVARCHAR",
        constraint: { nullable: true },
      },
      purchaseOrderId: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      jobId: {
        data_type: "INT",
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
      po_returns: {
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
      project_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_project_id: {
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
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_business_unit_id: {
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
  projects_wip_data: {
    columns: {
      id: {
        data_type: "INT",
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
        data_type: "INT",
        constraint: { nullable: true },
      },
      actual_business_unit_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      customer_details_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      actual_customer_details_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
      location_id: {
        data_type: "INT",
        constraint: { nullable: true },
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
      as_of_date: {
        data_type: "DATETIME2",
        constraint: { nullable: true },
      },
    },
  },
  project_managers: {
    columns: {
      id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      manager_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_manager_id: {
        data_type: "INT",
        constraint: { nullable: true },
      },
    },
  },
  call_details: {
    columns: {
      id: {
        data_type: "INT",
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
        constraint: { nullable: false },
      },
      actual_job_type_id: {
        data_type: "INT",
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
      project_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_project_id: {
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
      project_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_project_id: {
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
        data_type: "INT",
        constraint: { nullable: false },
      },
      acutal_address_zip: {
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
      project_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_project_id: {
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
      project_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_project_id: {
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
      project_id: {
        data_type: "INT",
        constraint: { nullable: false },
      },
      actual_project_id: {
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
  gross_profit: {
    columns: {
      id: {
        data_type: "INT",
        constraint: { primary: true, nullable: false },
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
  gross_pay_items: {
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
  projects_wip_data: {
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
  returns: {
    status: "",
  },
};

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

  console.log("========================================");
  console.log("CURRENT BATCH DATE: ", params_header["modifiedBefore"]);
  console.log("========================================");

  await data_processor(data_lake, sql_request, table_list);

  // Close the connection pool
  await sql.close();
}

async function data_processor(data_lake, sql_request, table_list) {
  let invoice_cache = {};
  let project_cache = {};
  let purchase_order_returns_cache = {};
  for (let api_count = 0; api_count < 15; api_count++) {
    // Object.keys(data_lake).length
    // table_list.length
    const api_name = table_list[api_count];

    console.log("table_name: ", api_name);

    switch (api_name) {
      case "returns": {
        const table_name = "returns";
        const data_pool =
          data_lake["returns"]["inventory__returns"]["data_pool"];
        const purchase_order_data_pool =
          data_lake["purchase_order"]["inventory__purchase-orders"][
            "data_pool"
          ];

        const header_data = hvac_tables[table_name]["columns"];

        const returns_final_data_pool = [];

        purchase_order_returns_cache[1] = {
          returnAmount: 0,
        };

        purchase_order_returns_cache[2] = {
          returnAmount: 0,
        };

        purchase_order_returns_cache[3] = {
          returnAmount: 0,
        };

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];

          const returnAmount = record["returnAmount"]
            ? parseFloat(record["returnAmount"])
            : 0;

          const taxAmount = record["taxAmount"]
            ? parseFloat(record["taxAmount"])
            : 0;

          if (record["purchaseOrderId"]) {
            if (!purchase_order_returns_cache[record["purchaseOrderId"]]) {
              purchase_order_returns_cache[record["purchaseOrderId"]] = {
                returnAmount: 0,
              };
            }

            if (purchase_order_data_pool[record["purchaseOrderId"]]) {
              purchase_order_returns_cache[record["purchaseOrderId"]][
                "returnAmount"
              ] += returnAmount;
            } else {
              purchase_order_returns_cache[record["instance_id"]][
                "returnAmount"
              ] += returnAmount;
            }
          } else {
            purchase_order_returns_cache[record["instance_id"]][
              "returnAmount"
            ] += returnAmount;
          }

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

        // console.log(
        //   "returns_final_data_pool order data: ",
        //   returns_final_data_pool.length
        // );

        // if (returns_final_data_pool.length > 0) {
        //   do {
        //     hvac_tables_responses["returns"]["status"] =
        //       await hvac_data_insertion(
        //         sql_request,
        //         returns_final_data_pool,
        //         header_data,
        //         table_name
        //       );
        //   } while (hvac_tables_responses["returns"]["status"] != "success");

        //   // entry into auto_update table
        //   try {
        //     const auto_update_query = `UPDATE auto_update SET returns = '${hvac_tables_responses["returns"]["status"]}' WHERE id=${lastInsertedId}`;

        //     await sql_request.query(auto_update_query);

        //     console.log("Auto_Update log created ");
        //   } catch (err) {
        //     console.log("Error while inserting into auto_update", err);
        //   }
        // }

        break;
      }

      case "purchase_order": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool = data_lake["projects"]["jpm__projects"]["data_pool"];
        const invoice_data_pool =
          data_lake["invoice"]["accounting__invoices"]["data_pool"];
        let jobs_data_pool = data_lake["job_details"]["jpm__jobs"]["data_pool"];
        let vendors_data_pool =
          data_lake["vendor"]["inventory__vendors"]["data_pool"];

        const header_data = hvac_tables[table_name]["columns"];

        const purchase_order_data_pool =
          data_lake[api_name]["inventory__purchase-orders"]["data_pool"];

        let purchase_order_final_data_pool = [];
        // deleting purchase order_records, where jobId = null (:- for reducing time complexity )

        if (initial_execute) {
          purchase_order_final_data_pool.push({
            id: 1,
            status: "default",
            total: 0,
            tax: 0,
            po_returns: purchase_order_returns_cache[1]["returnAmount"],
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
            project_id: 1,
            actual_project_id: 1,
            vendor_id: 1,
            actual_vendor_id: 1,
          });

          purchase_order_final_data_pool.push({
            id: 2,
            status: "default",
            total: 0,
            tax: 0,
            po_returns: purchase_order_returns_cache[2]["returnAmount"],
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
            project_id: 2,
            actual_project_id: 2,
            vendor_id: 2,
            actual_vendor_id: 2,
          });

          purchase_order_final_data_pool.push({
            id: 3,
            status: "default",
            total: 0,
            tax: 0,
            po_returns: purchase_order_returns_cache[3]["returnAmount"],
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
            project_id: 3,
            actual_project_id: 3,
            vendor_id: 3,
            actual_vendor_id: 3,
          });
        }

        // for projects and invoice table

        let invoice_po_and_gpi_data = {};

        let invoice_dummy_values = {
          po_cost: {
            1: 0,
            2: 0,
            3: 0,
          },
          po_returns: {
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

        let projects_po_and_gpi_data = {};

        let project_total_data = {};

        const project_dummy_values = {
          po_cost: {
            1: 0,
            2: 0,
            3: 0,
          },
          po_returns: {
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
          billed_amount: {
            1: 0,
            2: 0,
            3: 0,
          },
          equipment_cost: {
            1: 0,
            2: 0,
            3: 0,
          },
          material_cost: {
            1: 0,
            2: 0,
            3: 0,
          },
          accounts_receivable: {
            1: 0,
            2: 0,
            3: 0,
          },
          expense: {
            1: 0,
            2: 0,
            3: 0,
          },
          income: {
            1: 0,
            2: 0,
            3: 0,
          },
          current_liability: {
            1: 0,
            2: 0,
            3: 0,
          },
          membership_liability: {
            1: 0,
            2: 0,
            3: 0,
          },
          contract_value: {
            1: 0,
            2: 0,
            3: 0,
          },
          sold_contract_value: {
            1: 0,
            2: 0,
            3: 0,
          },
          budget_expense: {
            1: 0,
            2: 0,
            3: 0,
          },
          budget_hours: {
            1: 0,
            2: 0,
            3: 0,
          },
          balance: {
            1: 0,
            2: 0,
            3: 0,
          },
        };

        Object.keys(purchase_order_data_pool).map((po_record_id) => {
          const po_record = purchase_order_data_pool[po_record_id];

          let po_returns = 0;
          if (purchase_order_returns_cache[po_record_id]) {
            po_returns =
              purchase_order_returns_cache[po_record_id]["returnAmount"];
          }

          if (po_record["status"] != "Canceled") {
            // data accumulation for invoice table
            if (po_record["invoiceId"] != null) {
              if (!invoice_po_and_gpi_data[po_record["invoiceId"]]) {
                invoice_po_and_gpi_data[po_record["invoiceId"]] = {
                  po_cost: 0,
                  po_returns: 0,
                  labor_cost: 0,
                  labor_hours: 0,
                  burden: 0,
                };
              }

              if (!invoice_data_pool[po_record["invoiceId"]]) {
                invoice_dummy_values["po_cost"][po_record["instance_id"]] +=
                  parseFloat(po_record["total"]);

                invoice_dummy_values["po_returns"][po_record["instance_id"]] +=
                  po_returns;
              } else {
                invoice_po_and_gpi_data[po_record["invoiceId"]]["po_cost"] +=
                  parseFloat(po_record["total"]);

                invoice_po_and_gpi_data[po_record["invoiceId"]]["po_returns"] +=
                  po_returns;
              }
            } else {
              invoice_dummy_values["po_cost"][po_record["instance_id"]] +=
                parseFloat(po_record["total"]);

              invoice_dummy_values["po_returns"][po_record["instance_id"]] +=
                po_returns;
            }

            // data accumulation for projects table
            if (po_record["projectId"] != null) {
              if (!projects_po_and_gpi_data[po_record["projectId"]]) {
                projects_po_and_gpi_data[po_record["projectId"]] = {
                  po_cost: 0,
                  po_returns: 0,
                  labor_cost: 0,
                  labor_hours: 0,
                  burden: 0,
                };
              }

              if (!data_pool[po_record["projectId"]]) {
                project_dummy_values["po_cost"][po_record["instance_id"]] +=
                  parseFloat(po_record["total"]);

                project_dummy_values["po_returns"][po_record["instance_id"]] +=
                  po_returns;
              } else {
                projects_po_and_gpi_data[po_record["projectId"]]["po_cost"] +=
                  parseFloat(po_record["total"]);

                projects_po_and_gpi_data[po_record["projectId"]][
                  "po_returns"
                ] += po_returns;
              }
            } else {
              project_dummy_values["po_cost"][po_record["instance_id"]] +=
                parseFloat(po_record["total"]);

              project_dummy_values["po_returns"][po_record["instance_id"]] +=
                po_returns;
            }
          }

          let job_details_id = po_record["instance_id"];
          let actual_job_details_id = po_record["instance_id"];
          if (po_record["jobId"]) {
            actual_job_details_id = po_record["jobId"];
            if (jobs_data_pool[po_record["jobId"]]) {
              job_details_id = po_record["jobId"];
            }
          }

          let invoice_id = po_record["instance_id"];
          let actual_invoice_id = po_record["instance_id"];
          if (po_record["invoiceId"]) {
            actual_invoice_id = po_record["invoiceId"];
            if (invoice_data_pool[po_record["invoiceId"]]) {
              invoice_id = po_record["invoiceId"];
            }
          }

          let project_id = po_record["instance_id"];
          let actual_project_id = po_record["instance_id"];
          if (po_record["projectId"]) {
            actual_project_id = po_record["projectId"];
            if (data_pool[po_record["projectId"]]) {
              project_id = po_record["projectId"];
            }
          }

          let vendor_id = po_record["instance_id"];
          let actual_vendor_id = po_record["instance_id"];
          if (po_record["vendorId"]) {
            actual_vendor_id = po_record["vendorId"];
            if (vendors_data_pool[po_record["vendorId"]]) {
              vendor_id = po_record["vendorId"];
            }
          }

          let date = "2000-01-01T00:00:00.00Z";

          if (po_record["date"]) {
            if (
              new Date(po_record["date"]) > new Date("2000-01-01T00:00:00.00Z")
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

          purchase_order_final_data_pool.push({
            id: po_record["id"],
            status: po_record["status"] ? po_record["status"] : "default",
            total: po_record["total"] ? po_record["total"] : 0,
            tax: po_record["tax"] ? po_record["tax"] : 0,
            po_returns: po_returns,
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
        });

        project_cache["invoice_po_and_gpi_data"] = invoice_po_and_gpi_data;
        project_cache["invoice_dummy_values"] = invoice_dummy_values;
        project_cache["projects_po_and_gpi_data"] = projects_po_and_gpi_data;
        project_cache["project_total_data"] = project_total_data;
        project_cache["project_dummy_values"] = project_dummy_values;

        // console.log("purchase_order_final_data_pool: ", purchase_order_final_data_pool);
        // console.log("header_data: ", header_data);

        // await hvac_flat_data_insertion(
        //   sql_request,
        //   purchase_order_final_data_pool,
        //   header_data,
        //   table_name
        // );

        // console.log(
        //   "purchase order data: ",
        //   purchase_order_final_data_pool.length
        // );

        // if (purchase_order_final_data_pool.length > 0) {
        //   do {
        //     hvac_tables_responses["purchase_order"]["status"] =
        //       await hvac_data_insertion(
        //         sql_request,
        //         purchase_order_final_data_pool,
        //         header_data,
        //         table_name
        //       );
        //   } while (
        //     hvac_tables_responses["purchase_order"]["status"] != "success"
        //   );

        //   // entry into auto_update table
        //   try {
        //     const auto_update_query = `UPDATE auto_update SET purchase_order = '${hvac_tables_responses["purchase_order"]["status"]}' WHERE id=${lastInsertedId}`;

        //     await sql_request.query(auto_update_query);

        //     console.log("Auto_Update log created ");
        //   } catch (err) {
        //     console.log("Error while inserting into auto_update", err);
        //   }
        // }

        break;
      }

      case "sales_details": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool = data_lake[api_name]["sales__estimates"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];

        const projects_data_pool =
          data_lake["projects"]["jpm__projects"]["data_pool"];
        let jobs_data_pool = data_lake["job_details"]["jpm__jobs"]["data_pool"];

        const business_unit_data_pool =
          data_lake["business_unit"]["settings__business-units"]["data_pool"];

        const customer_data_pool =
          data_lake["customer_details"]["crm__customers"]["data_pool"];

        const location_data_pool =
          data_lake["location"]["crm__locations"]["data_pool"];

        const employees_data_pool =
          data_lake["employees"]["settings__employees"]["data_pool"];

        let final_data_pool = [];

        // calculating contract value from sales estimates for projects table
        project_cache["project_contract_value"] = {};

        Object.keys(data_pool).map((record_id) => {
          const record = data_pool[record_id];

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

          let project_id = record["instance_id"];
          let actual_project_id = record["projectId"]
            ? record["projectId"]
            : record["instance_id"];
          if (projects_data_pool[record["projectId"]]) {
            project_id = record["projectId"];
          }

          let soldBy_name = "default";
          if (employees_data_pool[record["soldBy"]]) {
            soldBy_name = employees_data_pool[record["soldBy"]]["name"];
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

          final_data_pool.push({
            id: record["id"],
            name: record["name"] ? record["name"] : "default",
            project_id: project_id,
            actual_project_id: actual_project_id,
            job_number: record["jobNumber"] ? record["jobNumber"] : "default",
            soldOn: soldOn,
            soldBy: record["soldBy"] ? record["soldBy"] : record["instance_id"],
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

          if (record["status"] && record["status"]["name"] == "Sold") {
            // preparing data for projects table
            if (record["projectId"] != null) {
              if (
                !project_cache["project_contract_value"][record["projectId"]]
              ) {
                project_cache["project_contract_value"][record["projectId"]] = {
                  contract_value: 0,
                  sold_contract_value: 0,
                  budget_expense: 0,
                  budget_hours: 0,
                };
              }

              if (!projects_data_pool[record["projectId"]]) {
                project_cache["project_dummy_values"]["contract_value"][
                  record["instance_id"]
                ] += parseFloat(record["subtotal"]);
                project_cache["project_dummy_values"]["budget_expense"][
                  record["instance_id"]
                ] += totalCost;
                project_cache["project_dummy_values"]["budget_hours"][
                  record["instance_id"]
                ] += budget_hours;
                project_cache["project_dummy_values"]["sold_contract_value"][
                  record["instance_id"]
                ] += parseFloat(record["subtotal"]);
              } else {
                project_cache["project_contract_value"][record["projectId"]][
                  "contract_value"
                ] += parseFloat(record["subtotal"]);

                project_cache["project_contract_value"][record["projectId"]][
                  "budget_expense"
                ] += totalCost;

                project_cache["project_contract_value"][record["projectId"]][
                  "budget_hours"
                ] += budget_hours;
                project_cache["project_contract_value"][record["projectId"]][
                  "sold_contract_value"
                ] += parseFloat(record["subtotal"]);
              }
            } else {
              project_cache["project_dummy_values"]["contract_value"][
                record["instance_id"]
              ] += parseFloat(record["subtotal"]);
              project_cache["project_dummy_values"]["budget_expense"][
                record["instance_id"]
              ] += totalCost;
              project_cache["project_dummy_values"]["budget_hours"][
                record["instance_id"]
              ] += budget_hours;
              project_cache["project_dummy_values"]["sold_contract_value"][
                record["instance_id"]
              ] += parseFloat(record["subtotal"]);
            }
          }
        });

        // console.log("sales_details data: ", final_data_pool.length);

        // if (final_data_pool.length > 0) {
        //   do {
        //     hvac_tables_responses["sales_details"]["status"] =
        //       await hvac_data_insertion(
        //         sql_request,
        //         final_data_pool,
        //         header_data,
        //         table_name
        //       );
        //   } while (
        //     hvac_tables_responses["sales_details"]["status"] != "success"
        //   );

        //   // entry into auto_update table
        //   try {
        //     const auto_update_query = `UPDATE auto_update SET sales_details = '${hvac_tables_responses["sales_details"]["status"]}' WHERE id=${lastInsertedId}`;

        //     await sql_request.query(auto_update_query);

        //     console.log("Auto_Update log created ");
        //   } catch (err) {
        //     console.log("Error while inserting into auto_update", err);
        //   }
        // }

        // delete data_lake[api_name];

        break;
      }

      case "projects": {
        const table_name = main_api_list[api_name][0]["table_name"];
        const data_pool = data_lake[api_name]["jpm__projects"]["data_pool"];
        const invoice_data_pool =
          data_lake["invoice"]["accounting__invoices"]["data_pool"];
        const header_data = hvac_tables[table_name]["columns"];
        const wip_header_data = hvac_tables["projects_wip_data"]["columns"];
        const project_managers_header_data =
          hvac_tables["project_managers"]["columns"];

        // getting data directly from service titan

        let jobs_data_pool = data_lake["job_details"]["jpm__jobs"]["data_pool"];
        let gross_pay_items_data_pool =
          data_lake["cogs_labor"]["payroll__gross-pay-items"]["data_pool"];
        let payrolls_data_pool =
          data_lake["cogs_labor"]["payroll__payrolls"]["data_pool"];

        const business_unit_data_pool =
          data_lake["business_unit"]["settings__business-units"]["data_pool"];

        const customer_data_pool =
          data_lake["customer_details"]["crm__customers"]["data_pool"];

        const location_data_pool =
          data_lake["location"]["crm__locations"]["data_pool"];

        const employees_data_pool =
          data_lake["employees"]["settings__employees"]["data_pool"];

        const sku_details_data_pool = {
          ...data_lake["sku_details"]["pricebook__materials"]["data_pool"],
          ...data_lake["sku_details"]["pricebook__equipment"]["data_pool"],
          ...data_lake["sku_details"]["pricebook__services"]["data_pool"],
        };

        let final_data_pool = [];
        let wip_final_data_pool = [];
        let project_managers_final_data_pool = [];

        let invoice_po_and_gpi_data = project_cache["invoice_po_and_gpi_data"];

        let invoice_dummy_values = project_cache["invoice_dummy_values"];

        let projects_po_and_gpi_data =
          project_cache["projects_po_and_gpi_data"];

        let project_total_data = project_cache["project_total_data"];

        const project_dummy_values = project_cache["project_dummy_values"];

        Object.keys(gross_pay_items_data_pool).map((gpi_record_id) => {
          const gpi_record = gross_pay_items_data_pool[gpi_record_id];
          // data accumulation for invoice table
          if (gpi_record["invoiceId"] != null) {
            if (!invoice_po_and_gpi_data[gpi_record["invoiceId"]]) {
              invoice_po_and_gpi_data[gpi_record["invoiceId"]] = {
                po_cost: 0,
                labor_cost: 0,
                labor_hours: 0,
                burden: 0,
              };
            }

            if (!invoice_data_pool[gpi_record["invoiceId"]]) {
              invoice_dummy_values["labor_cost"][gpi_record["instance_id"]] +=
                gpi_record["amount"] ? parseFloat(gpi_record["amount"]) : 0;

              invoice_dummy_values["labor_hours"][gpi_record["invoiceId"]] +=
                gpi_record["paidDurationHours"]
                  ? parseFloat(gpi_record["paidDurationHours"])
                  : 0;

              invoice_dummy_values["burden_cost"][gpi_record["instance_id"]] +=
                (gpi_record["paidDurationHours"]
                  ? parseFloat(gpi_record["paidDurationHours"])
                  : 0) *
                (payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
                  ? parseFloat(
                      payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
                    )
                  : 0);
            } else {
              invoice_po_and_gpi_data[gpi_record["invoiceId"]]["labor_cost"] +=
                gpi_record["amount"] ? parseFloat(gpi_record["amount"]) : 0;

              invoice_po_and_gpi_data[gpi_record["invoiceId"]]["labor_hours"] +=
                gpi_record["paidDurationHours"]
                  ? parseFloat(gpi_record["paidDurationHours"])
                  : 0;

              invoice_po_and_gpi_data[gpi_record["invoiceId"]]["burden"] +=
                (gpi_record["paidDurationHours"]
                  ? parseFloat(gpi_record["paidDurationHours"])
                  : 0) *
                (payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
                  ? parseFloat(
                      payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
                    )
                  : 0);
            }
          } else {
            invoice_dummy_values["labor_cost"][gpi_record["instance_id"]] +=
              gpi_record["amount"] ? parseFloat(gpi_record["amount"]) : 0;

            invoice_dummy_values["labor_hours"][gpi_record["invoiceId"]] +=
              gpi_record["paidDurationHours"]
                ? parseFloat(gpi_record["paidDurationHours"])
                : 0;

            invoice_dummy_values["burden_cost"][gpi_record["instance_id"]] +=
              (gpi_record["paidDurationHours"]
                ? parseFloat(gpi_record["paidDurationHours"])
                : 0) *
              (payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
                ? parseFloat(
                    payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
                  )
                : 0);
          }

          // data accumulation for projects table
          if (gpi_record["projectId"] != null) {
            if (!projects_po_and_gpi_data[gpi_record["projectId"]]) {
              projects_po_and_gpi_data[gpi_record["projectId"]] = {
                po_cost: 0,
                labor_cost: 0,
                labor_hours: 0,
                burden: 0,
              };
            }

            if (!data_pool[gpi_record["projectId"]]) {
              project_dummy_values["labor_cost"][gpi_record["instance_id"]] +=
                gpi_record["amount"] ? parseFloat(gpi_record["amount"]) : 0;

              project_dummy_values["labor_hours"][gpi_record["projectId"]] +=
                gpi_record["paidDurationHours"]
                  ? parseFloat(gpi_record["paidDurationHours"])
                  : 0;

              project_dummy_values["burden_cost"][gpi_record["instance_id"]] +=
                (gpi_record["paidDurationHours"]
                  ? parseFloat(gpi_record["paidDurationHours"])
                  : 0) *
                (payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
                  ? parseFloat(
                      payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
                    )
                  : 0);
            } else {
              projects_po_and_gpi_data[gpi_record["projectId"]]["labor_cost"] +=
                gpi_record["amount"] ? parseFloat(gpi_record["amount"]) : 0;

              projects_po_and_gpi_data[gpi_record["projectId"]][
                "labor_hours"
              ] += gpi_record["paidDurationHours"]
                ? parseFloat(gpi_record["paidDurationHours"])
                : 0;

              projects_po_and_gpi_data[gpi_record["projectId"]]["burden"] +=
                (gpi_record["paidDurationHours"]
                  ? parseFloat(gpi_record["paidDurationHours"])
                  : 0) *
                (payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
                  ? parseFloat(
                      payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
                    )
                  : 0);
            }
          } else {
            project_dummy_values["labor_cost"][gpi_record["instance_id"]] +=
              gpi_record["amount"] ? parseFloat(gpi_record["amount"]) : 0;

            project_dummy_values["labor_hours"][gpi_record["projectId"]] +=
              gpi_record["paidDurationHours"]
                ? parseFloat(gpi_record["paidDurationHours"])
                : 0;

            project_dummy_values["burden_cost"][gpi_record["instance_id"]] +=
              (gpi_record["paidDurationHours"]
                ? parseFloat(gpi_record["paidDurationHours"])
                : 0) *
              (payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
                ? parseFloat(
                    payrolls_data_pool[gpi_record["payrollId"]]["burdenRate"]
                  )
                : 0);
          }
        });

        let invoice_final_data_pool = [];
        let cogs_material_final_data_pool = [];
        let cogs_equipment_final_data_pool = [];
        let cogs_services_final_data_pool = [];
        let gross_profit_final_data_pool = [];

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
            project_id: 1,
            actual_project_id: 1,
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
            address_zip: 57483,
            acutal_address_zip: "57483",
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
            project_id: 2,
            actual_project_id: 2,
            business_unit_id: 2,
            actual_business_unit_id: 2,
            location_id: 2,
            actual_location_id: 2,
            address_street: "default",
            address_unit: "default",
            address_city: "default",
            address_state: "default",
            address_country: "default",
            address_zip: 57483,
            acutal_address_zip: "57483",
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
            project_id: 3,
            actual_project_id: 3,
            business_unit_id: 3,
            actual_business_unit_id: 3,
            location_id: 3,
            actual_location_id: 3,
            address_street: "default",
            address_unit: "default",
            address_city: "default",
            address_state: "default",
            address_country: "default",
            address_zip: 57483,
            acutal_address_zip: "57483",
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
            project_id: 1,
            actual_project_id: 1,
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
            project_id: 2,
            actual_project_id: 2,
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
            project_id: 3,
            actual_project_id: 3,
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
            project_id: 1,
            actual_project_id: 1,
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
            project_id: 2,
            actual_project_id: 2,
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
            project_id: 3,
            actual_project_id: 3,
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
            project_id: 1,
            actual_project_id: 1,
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
            project_id: 2,
            actual_project_id: 2,
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
            project_id: 3,
            actual_project_id: 3,
            invoice_id: 3,
            sku_details_id: 3,
            actual_sku_details_id: 3,
          });

          gross_profit_final_data_pool.push({
            id: 1,
            accounts_receivable: 0,
            expense: 0,
            income: 0,
            current_liability: 0,
            membership_liability: 0,
            default: 0,
            total: 0,
            po_cost: invoice_dummy_values["po_cost"][1], // purchase orders
            po_returns: invoice_dummy_values["po_returns"][1],
            equipment_cost: 0, //
            material_cost: 0, //
            labor_cost: invoice_dummy_values["labor_cost"][1], // cogs_labor burden cost, labor cost, paid duration
            burden: invoice_dummy_values["burden_cost"][1], // cogs_labor
            // gross_profit: gross_profit, // invoice[total] - po - equi - mater - labor - burden
            // gross_margin: gross_margin, // gross_profit / invoice['total'] * 100 %
            units: 1, //  currently for 1
            labor_hours: invoice_dummy_values["labor_hours"][1], // cogs_labor paid duration
          });

          gross_profit_final_data_pool.push({
            id: 2,
            accounts_receivable: 0,
            expense: 0,
            income: 0,
            current_liability: 0,
            membership_liability: 0,
            default: 0,
            total: 0,
            po_cost: invoice_dummy_values["po_cost"][2], // purchase orders
            po_returns: invoice_dummy_values["po_returns"][2],
            equipment_cost: 0, //
            material_cost: 0, //
            labor_cost: invoice_dummy_values["labor_cost"][2], // cogs_labor burden cost, labor cost, paid duration
            burden: invoice_dummy_values["burden_cost"][2], // cogs_labor
            // gross_profit: gross_profit, // invoice[total] - po - equi - mater - labor - burden
            // gross_margin: gross_margin, // gross_profit / invoice['total'] * 100 %
            units: 1, //  currently for 1
            labor_hours: invoice_dummy_values["labor_hours"][2], // cogs_labor paid duration
          });

          gross_profit_final_data_pool.push({
            id: 3,
            accounts_receivable: 0,
            expense: 0,
            income: 0,
            current_liability: 0,
            membership_liability: 0,
            default: 0,
            total: 0,
            po_cost: invoice_dummy_values["po_cost"][3], // purchase orders
            po_returns: invoice_dummy_values["po_returns"][3],
            equipment_cost: 0, //
            material_cost: 0, //
            labor_cost: invoice_dummy_values["labor_cost"][3], // cogs_labor burden cost, labor cost, paid duration
            burden: invoice_dummy_values["burden_cost"][3], // cogs_labor
            // gross_profit: gross_profit, // invoice[total] - po - equi - mater - labor - burden
            // gross_margin: gross_margin, // gross_profit / invoice['total'] * 100 %
            units: 1, //  currently for 1
            labor_hours: invoice_dummy_values["labor_hours"][3], // cogs_labor paid duration
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

          let project_id = record["instance_id"];
          let actual_project_id = record["instance_id"];
          if (record["projectId"]) {
            actual_project_id = record["projectId"];
            if (data_pool[record["projectId"]]) {
              project_id = record["projectId"];
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

          // data accumulation for projects table
          if (record["projectId"] != null) {
            if (!project_total_data[record["projectId"]]) {
              project_total_data[record["projectId"]] = {
                billed_amount: 0,
                equipment_cost: 0,
                material_cost: 0,
                accounts_receivable: 0,
                expense: 0,
                income: 0,
                current_liability: 0,
                membership_liability: 0,
                balance: 0,
                business_unit_id: business_unit_id,
                actual_business_unit_id: actual_business_unit_id,
              };
            }

            // calculating billed amount
            if (!data_pool[record["projectId"]]) {
              project_dummy_values["billed_amount"][record["instance_id"]] +=
                parseFloat(record["total"]);
              project_dummy_values["balance"][record["instance_id"]] +=
                parseFloat(record["balance"]);
            } else {
              project_total_data[record["projectId"]]["billed_amount"] +=
                parseFloat(record["total"]);
              project_total_data[record["projectId"]]["balance"] += parseFloat(
                record["balance"]
              );
            }
          } else {
            project_dummy_values["billed_amount"][record["instance_id"]] +=
              parseFloat(record["total"]);
            project_dummy_values["balance"][record["instance_id"]] +=
              parseFloat(record["balance"]);
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

          let po_cost = 0;
          let po_returns = 0;
          let labor_cost = 0;
          let labor_hours = 0;
          let burden = 0;

          if (invoice_po_and_gpi_data[record["id"]]) {
            po_cost = invoice_po_and_gpi_data[record["id"]]["po_cost"]
              ? invoice_po_and_gpi_data[record["id"]]["po_cost"]
              : 0;
            po_returns = invoice_po_and_gpi_data[record["id"]]["po_returns"]
              ? invoice_po_and_gpi_data[record["id"]]["po_returns"]
              : 0;
            labor_cost = invoice_po_and_gpi_data[record["id"]]["labor_cost"]
              ? invoice_po_and_gpi_data[record["id"]]["labor_cost"]
              : 0;
            labor_hours = invoice_po_and_gpi_data[record["id"]]["labor_hours"]
              ? invoice_po_and_gpi_data[record["id"]]["labor_hours"]
              : 0;
            burden = invoice_po_and_gpi_data[record["id"]]["burden"]
              ? invoice_po_and_gpi_data[record["id"]]["burden"]
              : 0;
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

                // for projects table
                if (record["projectId"] != null) {
                  // calculating material cost
                  if (!data_pool[record["projectId"]]) {
                    project_dummy_values["material_cost"][
                      record["instance_id"]
                    ] += parseFloat(items_record["totalCost"]);
                  } else {
                    project_total_data[record["projectId"]]["material_cost"] +=
                      parseFloat(items_record["totalCost"]);
                  }
                } else {
                  project_dummy_values["material_cost"][
                    record["instance_id"]
                  ] += parseFloat(items_record["totalCost"]);
                }

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
                  project_id: project_id,
                  actual_project_id: actual_project_id,
                  invoice_id: record["id"],
                  sku_details_id: sku_details_id,
                  actual_sku_details_id: actual_sku_details_id,
                });
              }

              if (items_record["type"] == "Equipment") {
                equipment_cost =
                  equipment_cost + parseFloat(items_record["totalCost"]);

                // for projects table
                if (record["projectId"] != null) {
                  // calculating equipment cost
                  if (!data_pool[record["projectId"]]) {
                    project_dummy_values["equipment_cost"][
                      record["instance_id"]
                    ] += parseFloat(items_record["totalCost"]);
                  } else {
                    project_total_data[record["projectId"]]["equipment_cost"] +=
                      parseFloat(items_record["totalCost"]);
                  }
                } else {
                  project_dummy_values["equipment_cost"][
                    record["instance_id"]
                  ] += parseFloat(items_record["totalCost"]);
                }

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
                if (sku_details_data_pool[items_record["skuId"]]) {
                  sku_details_id = items_record["skuId"];
                }

                // for gross profit
                switch (generalLedgerAccounttype) {
                  case "Accounts Receivable": {
                    accounts_receivable += items_record["total"]
                      ? parseFloat(items_record["total"])
                      : 0;

                    // for projects table
                    if (record["projectId"] != null) {
                      // calculating accounts_receivable
                      if (!data_pool[record["projectId"]]) {
                        project_dummy_values["accounts_receivable"][
                          record["instance_id"]
                        ] += items_record["total"]
                          ? parseFloat(items_record["total"])
                          : 0;
                      } else {
                        project_total_data[record["projectId"]][
                          "accounts_receivable"
                        ] += items_record["total"]
                          ? parseFloat(items_record["total"])
                          : 0;
                      }
                    } else {
                      project_dummy_values["accounts_receivable"][
                        record["instance_id"]
                      ] += items_record["total"]
                        ? parseFloat(items_record["total"])
                        : 0;
                    }

                    break;
                  }
                  case "Current Liability": {
                    current_liability += items_record["total"]
                      ? parseFloat(items_record["total"])
                      : 0;

                    // for projects table
                    if (record["projectId"] != null) {
                      // calculating current_liability
                      if (!data_pool[record["projectId"]]) {
                        project_dummy_values["current_liability"][
                          record["instance_id"]
                        ] += items_record["total"]
                          ? parseFloat(items_record["total"])
                          : 0;
                      } else {
                        project_total_data[record["projectId"]][
                          "current_liability"
                        ] += items_record["total"]
                          ? parseFloat(items_record["total"])
                          : 0;
                      }
                    } else {
                      project_dummy_values["current_liability"][
                        record["instance_id"]
                      ] += items_record["total"]
                        ? parseFloat(items_record["total"])
                        : 0;
                    }

                    break;
                  }
                  case "Membership Liability": {
                    membership_liability += items_record["total"]
                      ? parseFloat(items_record["total"])
                      : 0;

                    // for projects table
                    if (record["projectId"] != null) {
                      // calculating membership_liability
                      if (!data_pool[record["projectId"]]) {
                        project_dummy_values["membership_liability"][
                          record["instance_id"]
                        ] += items_record["total"]
                          ? parseFloat(items_record["total"])
                          : 0;
                      } else {
                        project_total_data[record["projectId"]][
                          "membership_liability"
                        ] += items_record["total"]
                          ? parseFloat(items_record["total"])
                          : 0;
                      }
                    } else {
                      project_dummy_values["membership_liability"][
                        record["instance_id"]
                      ] += items_record["total"]
                        ? parseFloat(items_record["total"])
                        : 0;
                    }

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
                  project_id: project_id,
                  actual_project_id: actual_project_id,
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

                  // for projects table
                  if (record["projectId"] != null) {
                    // calculating expense
                    if (!data_pool[record["projectId"]]) {
                      project_dummy_values["expense"][record["instance_id"]] +=
                        items_record["total"]
                          ? parseFloat(items_record["total"])
                          : 0;
                    } else {
                      project_total_data[record["projectId"]]["expense"] +=
                        items_record["total"]
                          ? parseFloat(items_record["total"])
                          : 0;
                    }
                  } else {
                    project_dummy_values["expense"][record["instance_id"]] +=
                      items_record["total"]
                        ? parseFloat(items_record["total"])
                        : 0;
                  }

                  break;
                }
                case "Income": {
                  income += items_record["total"]
                    ? parseFloat(items_record["total"])
                    : 0;

                  // for projects table
                  if (record["projectId"] != null) {
                    // calculating income
                    if (!data_pool[record["projectId"]]) {
                      project_dummy_values["income"][record["instance_id"]] +=
                        items_record["total"]
                          ? parseFloat(items_record["total"])
                          : 0;
                    } else {
                      project_total_data[record["projectId"]]["income"] +=
                        items_record["total"]
                          ? parseFloat(items_record["total"])
                          : 0;
                    }
                  } else {
                    project_dummy_values["income"][record["instance_id"]] +=
                      items_record["total"]
                        ? parseFloat(items_record["total"])
                        : 0;
                  }

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
              id: record["id"],
              accounts_receivable: accounts_receivable,
              expense: expense,
              income: income,
              current_liability: current_liability,
              membership_liability: membership_liability,
              default: default_val,
              total: record["total"] ? parseFloat(record["total"]) : 0,
              po_cost: po_cost, // purchase orders
              po_returns: po_returns,
              equipment_cost: equipment_cost, //
              material_cost: material_cost, //
              labor_cost: labor_cost, // cogs_labor burden cost, labor cost, paid duration
              burden: burden, // cogs_labor
              // gross_profit: gross_profit, // invoice[total] - po - equi - mater - labor - burden
              // gross_margin: gross_margin, // gross_profit / invoice['total'] * 100 %
              units: 1, //  currently for 1
              labor_hours: labor_hours, // cogs_labor paid duration
            });
          }
        });

        invoice_po_and_gpi_data = {};
        invoice_dummy_values = {};

        invoice_cache["invoice_final_data_pool"] = invoice_final_data_pool;
        invoice_cache["cogs_material_final_data_pool"] =
          cogs_material_final_data_pool;
        invoice_cache["cogs_equipment_final_data_pool"] =
          cogs_equipment_final_data_pool;
        invoice_cache["cogs_services_final_data_pool"] =
          cogs_services_final_data_pool;
        invoice_cache["gross_profit_final_data_pool"] =
          gross_profit_final_data_pool;

        if (initial_execute) {
          final_data_pool.push({
            id: 1,
            number: "1",
            name: "1",
            status: "No Status",
            billed_amount: project_dummy_values["billed_amount"][1],
            balance: project_dummy_values["balance"][1],
            contract_value: project_dummy_values["contract_value"][1],
            sold_contract_value: project_dummy_values["sold_contract_value"][1],
            budget_expense: project_dummy_values["budget_expense"][1],
            budget_hours: project_dummy_values["budget_hours"][1],
            po_cost: project_dummy_values["po_cost"][1],
            po_returns: project_dummy_values["po_returns"][1],
            equipment_cost: project_dummy_values["equipment_cost"][1],
            material_cost: project_dummy_values["material_cost"][1],
            labor_cost: project_dummy_values["labor_cost"][1],
            labor_hours: project_dummy_values["labor_hours"][1],
            burden: project_dummy_values["burden_cost"][1],
            accounts_receivable: project_dummy_values["accounts_receivable"][1],
            expense: project_dummy_values["expense"][1],
            income: project_dummy_values["income"][1],
            current_liability: project_dummy_values["current_liability"][1],
            membership_liability:
              project_dummy_values["membership_liability"][1],
            business_unit_id: 1,
            actual_business_unit_id: 1,
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
            number: "2",
            name: "2",
            status: "No Status",
            billed_amount: project_dummy_values["billed_amount"][2],
            balance: project_dummy_values["balance"][2],
            contract_value: project_dummy_values["contract_value"][2],
            sold_contract_value: project_dummy_values["sold_contract_value"][2],
            budget_expense: project_dummy_values["budget_expense"][2],
            budget_hours: project_dummy_values["budget_hours"][2],
            po_cost: project_dummy_values["po_cost"][2],
            po_returns: project_dummy_values["po_returns"][2],
            equipment_cost: project_dummy_values["equipment_cost"][2],
            material_cost: project_dummy_values["material_cost"][2],
            labor_cost: project_dummy_values["labor_cost"][2],
            labor_hours: project_dummy_values["labor_hours"][2],
            burden: project_dummy_values["burden_cost"][2],
            accounts_receivable: project_dummy_values["accounts_receivable"][2],
            expense: project_dummy_values["expense"][2],
            income: project_dummy_values["income"][2],
            current_liability: project_dummy_values["current_liability"][2],
            membership_liability:
              project_dummy_values["membership_liability"][2],
            business_unit_id: 2,
            actual_business_unit_id: 2,
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
            number: "3",
            name: "3",
            status: "No Status",
            billed_amount: project_dummy_values["billed_amount"][3],
            balance: project_dummy_values["balance"][3],
            contract_value: project_dummy_values["contract_value"][3],
            sold_contract_value: project_dummy_values["sold_contract_value"][3],
            budget_expense: project_dummy_values["budget_expense"][3],
            budget_hours: project_dummy_values["budget_hours"][3],
            po_cost: project_dummy_values["po_cost"][3],
            po_returns: project_dummy_values["po_returns"][3],
            equipment_cost: project_dummy_values["equipment_cost"][3],
            material_cost: project_dummy_values["material_cost"][3],
            labor_cost: project_dummy_values["labor_cost"][3],
            labor_hours: project_dummy_values["labor_hours"][3],
            burden: project_dummy_values["burden_cost"][3],
            accounts_receivable: project_dummy_values["accounts_receivable"][3],
            expense: project_dummy_values["expense"][3],
            income: project_dummy_values["income"][3],
            current_liability: project_dummy_values["current_liability"][3],
            membership_liability:
              project_dummy_values["membership_liability"][3],
            business_unit_id: 3,
            actual_business_unit_id: 3,
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

          let billed_amount = 0;
          let balance = 0;
          let contract_value = 0;
          let sold_contract_value = 0;
          let budget_expense = 0;
          let budget_hours = 0;
          let po_cost = 0;
          let po_returns = 0;
          let equipment_cost = 0;
          let material_cost = 0;
          let labor_cost = 0;
          let labor_hours = 0;
          let burden = 0;
          let accounts_receivable = 0;
          let expense = 0;
          let income = 0;
          let current_liability = 0;
          let membership_liability = 0;
          let business_unit_id = record["instance_id"];
          let actual_business_unit_id = record["instance_id"];

          if (project_total_data[record["id"]]) {
            billed_amount = project_total_data[record["id"]]["billed_amount"]
              ? project_total_data[record["id"]]["billed_amount"]
              : 0;
            balance = project_total_data[record["id"]]["balance"]
              ? project_total_data[record["id"]]["balance"]
              : 0;
            equipment_cost = project_total_data[record["id"]]["equipment_cost"]
              ? project_total_data[record["id"]]["equipment_cost"]
              : 0;
            material_cost = project_total_data[record["id"]]["material_cost"]
              ? project_total_data[record["id"]]["material_cost"]
              : 0;

            accounts_receivable = project_total_data[record["id"]][
              "accounts_receivable"
            ]
              ? project_total_data[record["id"]]["accounts_receivable"]
              : 0;
            expense = project_total_data[record["id"]]["expense"]
              ? project_total_data[record["id"]]["expense"]
              : 0;
            income = project_total_data[record["id"]]["income"]
              ? project_total_data[record["id"]]["income"]
              : 0;
            current_liability = project_total_data[record["id"]][
              "current_liability"
            ]
              ? project_total_data[record["id"]]["current_liability"]
              : 0;
            membership_liability = project_total_data[record["id"]][
              "membership_liability"
            ]
              ? project_total_data[record["id"]]["membership_liability"]
              : 0;

            business_unit_id = project_total_data[record["id"]][
              "business_unit_id"
            ]
              ? project_total_data[record["id"]]["business_unit_id"]
              : record["instance_id"];

            actual_business_unit_id = project_total_data[record["id"]][
              "actual_business_unit_id"
            ]
              ? project_total_data[record["id"]]["actual_business_unit_id"]
              : record["instance_id"];
          }

          if (project_cache["project_contract_value"][record["id"]]) {
            contract_value = project_cache["project_contract_value"][
              record["id"]
            ]["contract_value"]
              ? project_cache["project_contract_value"][record["id"]][
                  "contract_value"
                ]
              : 0;

            sold_contract_value = project_cache["project_contract_value"][
              record["id"]
            ]["sold_contract_value"]
              ? project_cache["project_contract_value"][record["id"]][
                  "sold_contract_value"
                ]
              : 0;

            budget_expense = project_cache["project_contract_value"][
              record["id"]
            ]["budget_expense"]
              ? project_cache["project_contract_value"][record["id"]][
                  "budget_expense"
                ]
              : 0;

            budget_hours = project_cache["project_contract_value"][
              record["id"]
            ]["budget_hours"]
              ? project_cache["project_contract_value"][record["id"]][
                  "budget_hours"
                ]
              : 0;
          }

          if (projects_po_and_gpi_data[record["id"]]) {
            po_cost = projects_po_and_gpi_data[record["id"]]["po_cost"]
              ? projects_po_and_gpi_data[record["id"]]["po_cost"]
              : 0;
            po_returns = projects_po_and_gpi_data[record["id"]]["po_returns"]
              ? projects_po_and_gpi_data[record["id"]]["po_returns"]
              : 0;
            labor_cost = projects_po_and_gpi_data[record["id"]]["labor_cost"]
              ? projects_po_and_gpi_data[record["id"]]["labor_cost"]
              : 0;
            labor_hours = projects_po_and_gpi_data[record["id"]]["labor_hours"]
              ? projects_po_and_gpi_data[record["id"]]["labor_hours"]
              : 0;
            burden = projects_po_and_gpi_data[record["id"]]["burden"]
              ? projects_po_and_gpi_data[record["id"]]["burden"]
              : 0;
          }

          // preparing project_managers_final_data_pool
          const project_managers_ids = record["projectManagerIds"];
          project_managers_ids.map((manager_id) => {
            let manager_ID = record["instance_id"];
            let actual_manager_id = manager_id;

            if (employees_data_pool[manager_id]) {
              manager_ID = manager_id;
            }

            project_managers_final_data_pool.push({
              id: record["id"],
              manager_id: manager_ID,
              actual_manager_id: actual_manager_id,
            });
          });

          final_data_pool.push({
            id: record["id"],
            number: record["number"] ? record["number"] : "default",
            name: record["name"] ? record["name"] : `${record["id"]}`,
            status: record["status"] ? record["status"] : "No Status",
            billed_amount: billed_amount,
            balance: balance,
            contract_value: contract_value,
            sold_contract_value: sold_contract_value,
            budget_expense: budget_expense,
            budget_hours: budget_hours,
            po_cost: po_cost,
            po_returns: po_returns,
            equipment_cost: equipment_cost,
            material_cost: material_cost,
            labor_cost: labor_cost,
            labor_hours: labor_hours,
            burden: burden,
            accounts_receivable: accounts_receivable,
            expense: expense,
            income: income,
            current_liability: current_liability,
            membership_liability: membership_liability,
            business_unit_id: business_unit_id,
            actual_business_unit_id: actual_business_unit_id,
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

          const as_of_date = new Date(params_header["modifiedBefore"])
            .toISOString()
            .slice(0, 10);

          wip_final_data_pool.push({
            id: record["id"],
            number: record["number"] ? record["number"] : "default",
            name: record["name"] ? record["name"] : `${record["id"]}`,
            status: record["status"] ? record["status"] : "No Status",
            billed_amount: billed_amount,
            balance: balance,
            contract_value: contract_value,
            sold_contract_value: sold_contract_value,
            budget_expense: budget_expense,
            budget_hours: budget_hours,
            po_cost: po_cost,
            po_returns: po_returns,
            equipment_cost: equipment_cost,
            material_cost: material_cost,
            labor_cost: labor_cost,
            labor_hours: labor_hours,
            burden: burden,
            accounts_receivable: accounts_receivable,
            expense: expense,
            income: income,
            current_liability: current_liability,
            membership_liability: membership_liability,
            business_unit_id: business_unit_id,
            actual_business_unit_id: actual_business_unit_id,
            customer_details_id: customer_details_id,
            actual_customer_details_id: actual_customer_details_id,
            location_id: location_id,
            actual_location_id: actual_location_id,
            startDate: startDate,
            targetCompletionDate: targetCompletionDate,
            actualCompletionDate: actualCompletionDate,
            createdOn: createdOn,
            modifiedOn: modifiedOn,
            as_of_date: as_of_date,
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

        if (wip_final_data_pool.length > 0) {
          do {
            hvac_tables_responses["projects_wip_data"]["status"] =
              await hvac_data_insertion(
                sql_request,
                wip_final_data_pool,
                wip_header_data,
                "projects_wip_data"
              );
          } while (
            hvac_tables_responses["projects_wip_data"]["status"] != "success"
          );

          // entry into auto_update table
          try {
            const auto_update_query = `UPDATE auto_update SET projects_wip_data = '${hvac_tables_responses["projects_wip_data"]["status"]}' WHERE id=${lastInsertedId}`;

            await sql_request.query(auto_update_query);

            console.log("Auto_Update log created ");
          } catch (err) {
            console.log("Error while inserting into auto_update", err);
          }
        }

        break;
      }
    }
  }
}

// for automatic mass ETL
async function start_pipeline() {
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

async function wip_data() {
  const start_date = new Date("2024-02-01T07:00:00.00Z");
  const end_date = new Date("2024-02-09T07:00:00.00Z");

  let current_batch_date = start_date;

  while (current_batch_date <= end_date) {
    data_lake = {};

    params_header["modifiedBefore"] = current_batch_date.toISOString();

    console.log("========================================");
    console.log("CURRENT BATCH DATE: ", params_header["modifiedBefore"]);
    console.log("========================================");

    await start_pipeline();

    current_batch_date.setDate(current_batch_date.getDate() + 1);
  }
}

wip_data();
