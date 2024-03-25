-- legal_entity
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'legal_entity')
BEGIN
CREATE TABLE legal_entity (
  id INT PRIMARY KEY,
  legal_name NVARCHAR(MAX) NULL
);
END;

-- us_cities
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'us_cities')
BEGIN
CREATE TABLE us_cities (
  id INT PRIMARY KEY,
  -- latitude DECIMAL(9, 6) NULL,
  -- longitude DECIMAL(9, 6) NULL,
  latitude NVARCHAR(MAX) NULL,
  longitude NVARCHAR(MAX) NULL,
  city NVARCHAR(MAX) NULL,
  [state] NVARCHAR(MAX) NULL,
  county NVARCHAR(MAX) NULL,
);
END;

-- business_unit
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'business_unit')
BEGIN
CREATE TABLE business_unit (
  id NVARCHAR(MAX) PRIMARY KEY,
  business_unit_name NVARCHAR(MAX) NULL,
  business_unit_official_name NVARCHAR(MAX) NULL,
  trade_type NVARCHAR(MAX) NULL,
  segment_type NVARCHAR(MAX) NULL,
  revenue_type NVARCHAR(MAX) NULL,
  business NVARCHAR(MAX) NULL,
  is_active TINYINT NULL,
  legal_entity_id INT NOT NULL,
  FOREIGN KEY (legal_entity_id) REFERENCES legal_entity (id)
);
END;

-- project_business_unit
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'project_business_unit')
BEGIN
CREATE TABLE project_business_unit (
  id NVARCHAR(MAX) PRIMARY KEY,
  business_unit_name NVARCHAR(MAX) NULL,
  business_unit_official_name NVARCHAR(MAX) NULL,
  trade_type NVARCHAR(MAX) NULL,
  segment_type NVARCHAR(MAX) NULL,
  revenue_type NVARCHAR(MAX) NULL,
  business NVARCHAR(MAX) NULL,
  is_active TINYINT NULL,
  legal_entity_id INT NOT NULL,
  legal_entity_name NVARCHAR(MAX) NULL,
  FOREIGN KEY (legal_entity_id) REFERENCES legal_entity (id)
);
END;

-- employees
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='employees')
BEGIN
CREATE TABLE employees(
  id INT PRIMARY KEY,
  [name] NVARCHAR(MAX) NULL,
  [role] NVARCHAR(MAX) NULL,
  business_unit_id NVARCHAR(MAX) NOT NULL,
  actual_business_unit_id NVARCHAR(MAX) NULL,
  FOREIGN KEY (business_unit_id) REFERENCES business_unit (id),
)
END;

-- campaigns
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'campaigns')
BEGIN
CREATE TABLE campaigns (
  id INT PRIMARY KEY,
  [name] NVARCHAR(MAX) NULL,
  is_active TINYINT NULL,
  createdOn DATETIME2 NULL,
  modifiedOn DATETIME2 NULL,
  category_id INT NULL,
  category_name NVARCHAR(MAX) NULL,
  is_category_active TINYINT NULL,
  source NVARCHAR(MAX) NULL,
  medium NVARCHAR(MAX) NULL,
  business_unit_id NVARCHAR(MAX) NOT NULL,
  actual_business_unit_id NVARCHAR(MAX) NULL,
  FOREIGN KEY (business_unit_id) REFERENCES business_unit (id),
);
END;

-- bookings
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'bookings')
BEGIN
CREATE TABLE bookings (
  id INT PRIMARY KEY,
  [name] NVARCHAR(MAX) NULL,
  source NVARCHAR(MAX) NULL,
  [status] NVARCHAR(MAX) NULL,
  customer_type NVARCHAR(MAX) NULL,
  [start] DATETIME2 NULL,
  bookingProviderId INT NULL,
  createdOn DATETIME2 NULL,
  modifiedOn DATETIME2 NULL,
  address_street NVARCHAR(MAX) NULL,
  address_unit NVARCHAR(MAX) NULL,
  address_city NVARCHAR(MAX) NULL,
  address_state NVARCHAR(MAX) NULL,
  address_zip NVARCHAR(MAX) NULL,
  address_country NVARCHAR(MAX) NULL,
  business_unit_id NVARCHAR(MAX) NOT NULL,
  actual_business_unit_id NVARCHAR(MAX) NULL,
  campaign_id INT NOT NULL,
  actual_campaign_id INT NULL,
  job_details_id INT NULL,
  FOREIGN KEY (business_unit_id) REFERENCES business_unit (id),
  FOREIGN KEY (campaign_id) REFERENCES campaigns (id)
);
END;

-- customer_details
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'customer_details')
BEGIN
CREATE TABLE customer_details (
  id INT PRIMARY KEY,
  [name] NVARCHAR(MAX) NULL,
  is_active TINYINT NULL,
  type NVARCHAR(MAX) NULL,
  creation_date DATETIME2 NULL,
  address_street NVARCHAR(MAX) NULL,
  address_unit NVARCHAR(MAX) NULL,
  address_city NVARCHAR(MAX) NULL,
  address_state NVARCHAR(MAX) NULL,
  address_zip NVARCHAR(MAX) NULL
);
END;

-- location
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'location')
BEGIN
CREATE TABLE location (
  id INT PRIMARY KEY,
  name NVARCHAR(MAX) NULL,
  street NVARCHAR(MAX) NULL,
  unit NVARCHAR(MAX) NULL,
  city NVARCHAR(MAX) NULL,
  state NVARCHAR(MAX) NULL,
  country NVARCHAR(MAX) NULL,
  address_zip INT NOT NULL,
  acutal_address_zip NVARCHAR(MAX) NULL,
  latitude NVARCHAR(MAX) NULL,
  longitude NVARCHAR(MAX) NULL,
  taxzone INT NULL,
  zone_id INT NULL,
  full_address NVARCHAR(MAX) NULL,
  FOREIGN KEY (address_zip) REFERENCES us_cities (id)
);
END;

--
-- gross_pay_items
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'gross_pay_items')
BEGIN
CREATE TABLE gross_pay_items (
  id INT NULL,
  payrollId INT NULL,
  amount DECIMAL(18, 8) NULL,
  paidDurationHours DECIMAL(18, 8) NULL,
  projectId INT NULL,
  invoiceId INT NULL,
);
END;

-- payrolls
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'payrolls')
BEGIN
CREATE TABLE payrolls (
  id INT PRIMARY KEY,
  burdenRate DECIMAL(18, 8) NULL,
);
END;

-- job_types
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'job_types')
BEGIN
CREATE TABLE job_types (
  id INT PRIMARY KEY,
  job_type_name NVARCHAR(MAX) NULL,
);
END;

-- purchase_order
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'returns')
BEGIN
  CREATE TABLE returns (
    id INT PRIMARY KEY,
    status NVARCHAR(MAX) NULL,
    purchaseOrderId INT NULL,
    jobId INT NULL,
    returnAmount DECIMAL(18, 8) NULL,
    taxAmount DECIMAL(18, 8) NULL,
  );
END;

-- purchase_order
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'purchase_order')
BEGIN
  CREATE TABLE purchase_order (
    id INT PRIMARY KEY,
    status NVARCHAR(MAX) NULL,
    total DECIMAL(18, 8) NULL,
    tax DECIMAL(18, 8) NULL,
    inventory_bill_amount DECIMAL(18, 8) NULL,
    po_returns DECIMAL(18, 8) NULL,
    date DATETIME2 NULL,
    requiredOn DATETIME2 NULL,
    sentOn DATETIME2 NULL,
    receivedOn DATETIME2 NULL,
    createdOn DATETIME2 NULL,
    modifiedOn DATETIME2 NULL,
    job_details_id INT NOT NULL,
    actual_job_details_id INT NULL,
    invoice_id INT NOT NULL,
    actual_invoice_id INT NULL,
    project_id INT NOT NULL,
    actual_project_id INT NULL,
    vendor_id INT NOT NULL,
    actual_vendor_id INT NULL,
    -- FOREIGN KEY (job_details_id) REFERENCES job_details (id),
    -- FOREIGN KEY (invoice_id) REFERENCES invoice (id),
    -- FOREIGN KEY (project_id) REFERENCES projects (id),
    -- FOREIGN KEY (vendor_id) REFERENCES vendor (id),
  );
END;

-- sales_details
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sales_details')
BEGIN
CREATE TABLE sales_details (
  id INT PRIMARY KEY,
  [name] NVARCHAR(MAX) NULL,
  project_id INT NOT NULL,
  actual_project_id INT NULL,
  job_number NVARCHAR(MAX) NULL,
  soldOn DATETIME2 NULL,
  soldBy INT NOT NULL,
  soldBy_name NVARCHAR(MAX) NULL,
  is_active TINYINT NULL,
  subtotal DECIMAL(18, 8) NULL,
  estimates_age INT NULL,
  estimates_sold_hours DECIMAL(18, 8) NULL,
  budget_expense DECIMAL(18, 8) NULL,
  budget_hours DECIMAL(18, 8) NULL,
  status_value INT NULL,
  status_name NVARCHAR(MAX) NULL,
  createdOn DATETIME2 NULL,
  modifiedOn DATETIME2 NULL,
  business_unit_id NVARCHAR(MAX) NOT NULL,
  actual_business_unit_id NVARCHAR(MAX) NULL,
  businessUnitName NVARCHAR(MAX) NULL,
  job_details_id INT NOT NULL,
  actual_job_details_id INT NULL,
  location_id INT NOT NULL,
  actual_location_id INT NULL,
  customer_details_id INT NOT NULL,
  actual_customer_details_id INT NULL,
  FOREIGN KEY (soldBy) REFERENCES employees (id),
  -- FOREIGN KEY (project_id) REFERENCES projects (id),
  -- FOREIGN KEY (business_unit_id) REFERENCES business_unit (id),
  -- FOREIGN KEY (job_details_id) REFERENCES job_details (id),
  -- FOREIGN KEY (location_id) REFERENCES location (id),
  -- FOREIGN KEY (customer_details_id) REFERENCES customer_details (id)
);
END;

-- projects
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='projects')
BEGIN
CREATE TABLE projects(
  id INT PRIMARY KEY,
  [number] NVARCHAR(MAX) NULL,
  [name] NVARCHAR(MAX) NULL,
  [status] NVARCHAR(MAX) NULL,
  billed_amount DECIMAL(18, 8) NULL,
  balance DECIMAL(18, 8) NULL,
  contract_value DECIMAL(18, 8) NULL,
  sold_contract_value DECIMAL(18, 8) NULL,
  budget_expense DECIMAL(18, 8) NULL,
  budget_hours DECIMAL(18, 8) NULL,
  inventory_bill_amount DECIMAL(18, 8) NULL,
  po_cost DECIMAL(18, 8) NULL,
  po_returns DECIMAL(18, 8) NULL,
  equipment_cost DECIMAL(18, 8) NULL,
  material_cost DECIMAL(18, 8) NULL,
  labor_cost DECIMAL(18, 8) NULL,
  labor_hours DECIMAL(18, 8) NULL,
  burden DECIMAL(18, 8) NULL,
  accounts_receivable DECIMAL(18, 8) NULL,
  expense DECIMAL(18, 8) NULL,
  income DECIMAL(18, 8) NULL,
  current_liability DECIMAL(18, 8) NULL,
  membership_liability DECIMAL(18, 8) NULL,
  business_unit_id NVARCHAR(MAX) NOT NULL,
  actual_business_unit_id NVARCHAR(MAX) NULL,
  customer_details_id INT NOT NULL,
  actual_customer_details_id INT NULL,
  location_id INT NOT NULL,
  actual_location_id INT NULL,
  startDate DATETIME2 NULL,
  targetCompletionDate DATETIME2 NULL,
  actualCompletionDate DATETIME2 NULL,
  createdOn DATETIME2 NULL,
  modifiedOn DATETIME2 NULL,
  FOREIGN KEY (business_unit_id) REFERENCES business_unit (id),
  FOREIGN KEY (customer_details_id) REFERENCES customer_details (id),
  FOREIGN KEY (location_id) REFERENCES location (id),
)
END;

-- projects_wip_data
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='projects_wip_data')
BEGIN
CREATE TABLE projects_wip_data(
  id INT NULL,
  [number] NVARCHAR(MAX) NULL,
  [name] NVARCHAR(MAX) NULL,
  [status] NVARCHAR(MAX) NULL,
  billed_amount DECIMAL(18, 8) NULL,
  balance DECIMAL(18, 8) NULL,
  contract_value DECIMAL(18, 8) NULL,
  sold_contract_value DECIMAL(18, 8) NULL,
  budget_expense DECIMAL(18, 8) NULL,
  budget_hours DECIMAL(18, 8) NULL,
  inventory_bill_amount DECIMAL(18, 8) NULL,
  po_cost DECIMAL(18, 8) NULL,
  po_returns DECIMAL(18, 8) NULL,
  equipment_cost DECIMAL(18, 8) NULL,
  material_cost DECIMAL(18, 8) NULL,
  labor_cost DECIMAL(18, 8) NULL,
  labor_hours DECIMAL(18, 8) NULL,
  burden DECIMAL(18, 8) NULL,
  accounts_receivable DECIMAL(18, 8) NULL,
  expense DECIMAL(18, 8) NULL,
  income DECIMAL(18, 8) NULL,
  current_liability DECIMAL(18, 8) NULL,
  membership_liability DECIMAL(18, 8) NULL,
  business_unit_id NVARCHAR(MAX) NULL,
  actual_business_unit_id NVARCHAR(MAX) NULL,
  customer_details_id INT  NULL,
  actual_customer_details_id INT NULL,
  location_id INT  NULL,
  actual_location_id INT NULL,
  startDate DATETIME2 NULL,
  targetCompletionDate DATETIME2 NULL,
  actualCompletionDate DATETIME2 NULL,
  createdOn DATETIME2 NULL,
  modifiedOn DATETIME2 NULL,
  UTC_update_date DATETIME2 NULL,
)
END;

-- project_managers
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='project_managers')
BEGIN
CREATE TABLE project_managers(
  id INT NOT NULL,
  manager_id INT NOT NULL,
  actual_manager_id INT NULL,
  FOREIGN KEY (id) REFERENCES projects (id),
  FOREIGN KEY (manager_id) REFERENCES employees (id),
)
END;

-- call_details
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'call_details')
BEGIN
CREATE TABLE call_details (
  id INT PRIMARY KEY,
  instance_id INT NULL,
  job_number NVARCHAR(MAX) NULL,
  [status] NVARCHAR(MAX) NULL,
  project_id INT NOT NULL,
  actual_project_id INT NULL,
  createdOn DATETIME2 NULL,
  modifiedOn DATETIME2 NULL,
  receivedOn DATETIME2 NULL,
  duration NVARCHAR(MAX) NULL,
  [from] NVARCHAR(MAX) NULL,
  [to] NVARCHAR(MAX) NULL,
  direction NVARCHAR(MAX) NULL,
  call_type NVARCHAR(MAX) NULL,
  customer_details_id INT NOT NULL,
  actual_customer_details_id INT NULL,
  is_customer_active TINYINT NULL,
  customer_name NVARCHAR(MAX) NULL,
  street_address NVARCHAR(MAX) NULL,
  street NVARCHAR(MAX) NULL,
  unit NVARCHAR(MAX) NULL,
  city NVARCHAR(MAX) NULL,
  [state] NVARCHAR(MAX) NULL,
  country NVARCHAR(MAX) NULL,
  zip NVARCHAR(MAX) NULL,
  latitude DECIMAL(9, 6) NULL,
  longitude DECIMAL(9, 6) NULL,
  customer_import_id NVARCHAR(MAX) NULL,
  customer_type NVARCHAR(MAX) NULL,
  campaign_id INT NULL,
  campaign_category NVARCHAR(MAX) NULL,
  campaign_source NVARCHAR(MAX) NULL,
  campaign_medium NVARCHAR(MAX) NULL,
  campaign_dnis NVARCHAR(MAX) NULL,
  campaign_name NVARCHAR(MAX) NULL,
  campaign_createdOn DATETIME2 NULL,
  campaign_modifiedOn DATETIME2 NULL,
  is_campaign_active TINYINT NULL,
  agent_id INT NULL,
  agent_externalId INT NULL,
  agent_name NVARCHAR(MAX) NULL,
  business_unit_id NVARCHAR(MAX) NOT NULL,
  actual_business_unit_id NVARCHAR(MAX) NULL,
  business_unit_active TINYINT NULL,
  business_unit_name NVARCHAR(MAX) NULL,
  business_unit_official_name NVARCHAR(MAX) NULL,
  type_id INT NULL,
  type_name NVARCHAR(MAX) NULL,
  type_modifiedOn DATETIME2 NULL,
  FOREIGN KEY (project_id) REFERENCES projects (id),
  FOREIGN KEY (business_unit_id) REFERENCES business_unit (id),
  FOREIGN KEY (customer_details_id) REFERENCES customer_details (id)
);
END;

-- job_details
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'job_details')
BEGIN
CREATE TABLE job_details (
  id INT PRIMARY KEY,
  job_type_id INT NOT NULL,
  actual_job_type_id INT NULL,
  job_number NVARCHAR(MAX) NULL,
  job_status NVARCHAR(MAX) NULL,
  job_completion_time DATETIME2 NULL,
  business_unit_id NVARCHAR(MAX) NOT NULL,
  actual_business_unit_id NVARCHAR(MAX) NULL,
  location_id INT NOT NULL,
  actual_location_id INT NULL,
  customer_details_id INT NOT NULL,
  actual_customer_details_id INT NULL,
  project_id INT NOT NULL,
  actual_project_id INT NULL,
  campaign_id INT NOT NULL,
  actual_campaign_id INT NULL,
  createdOn DATETIME2 NULL,
  modifiedOn DATETIME2 NULL,
  created_by_id INT NULL,
  lead_call_id INT NOT NULL,
  actual_lead_call_id INT NULL,
  booking_id INT NOT NULL,
  actual_booking_id INT NULL,
  sold_by_id INT NULL,
  FOREIGN KEY (business_unit_id) REFERENCES business_unit (id),
  FOREIGN KEY (location_id) REFERENCES location (id),
  FOREIGN KEY (customer_details_id) REFERENCES customer_details (id),
  FOREIGN KEY (lead_call_id) REFERENCES call_details (id),
  FOREIGN KEY (campaign_id) REFERENCES campaigns (id),
  FOREIGN KEY (booking_id) REFERENCES bookings (id),
  FOREIGN KEY (project_id) REFERENCES projects (id),
  FOREIGN KEY (job_type_id) REFERENCES job_types (id),
);
END;

-- appointments
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'appointments')
BEGIN
CREATE TABLE appointments (
  id INT PRIMARY KEY,
  job_details_id INT NOT NULL,
  actual_job_details_id INT NULL,
  appointmentNumber NVARCHAR(MAX) NULL,
  [start] DATETIME2 NULL,
  [end] DATETIME2 NULL,
  arrivalWindowStart DATETIME2 NULL,
  arrivalWindowEnd DATETIME2 NULL,
  [status] NVARCHAR(MAX) NULL,
  createdOn DATETIME2 NULL,
  modifiedOn DATETIME2 NULL,
  customer_details_id INT NOT NULL,
  actual_customer_details_id INT NULL,
  FOREIGN KEY (job_details_id) REFERENCES job_details (id),
  FOREIGN KEY (customer_details_id) REFERENCES customer_details (id)
);
END;

-- vendor_details
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'vendor')
BEGIN
CREATE TABLE vendor (
  id INT PRIMARY KEY,
  [name] NVARCHAR(MAX) NULL,
  is_active TINYINT NULL
);
END;

-- inventory_bills
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='inventory_bills')
BEGIN
CREATE TABLE inventory_bills(
  id INT NOT NULL,
  purchase_order_id INT NOT NULL, 
  actual_purchase_order_id INT NULL,
  syncStatus NVARCHAR(MAX) NULL,
  referenceNumber NVARCHAR(MAX) NULL,
  vendorNumber NVARCHAR(MAX) NULL,
  billDate DATETIME2 NULL,
  billAmount DECIMAL(18, 8) NULL,
  taxAmount DECIMAL(18, 8) NULL,
  shippingAmount DECIMAL(18, 8) NULL,
  createdOn DATETIME2 NULL,
  dueDate DATETIME2 NULL,
  business_unit_id NVARCHAR(MAX) NOT NULL,
  actual_business_unit_id NVARCHAR(MAX) NULL,
  vendor_id INT NOT NULL,
  actual_vendor_id INT NULL,
  job_details_id INT NOT NULL,
  actual_job_details_id INT NULL,
  job_number NVARCHAR(MAX) NULL,
  FOREIGN KEY (vendor_id) REFERENCES vendor (id),
  FOREIGN KEY (business_unit_id) REFERENCES business_unit (id),
  FOREIGN KEY (purchase_order_id) REFERENCES purchase_order (id),
  FOREIGN KEY (job_details_id) REFERENCES job_details (id),
)
END;

-- technician
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'technician')
BEGIN
CREATE TABLE technician (
  id INT PRIMARY KEY,
  [name] NVARCHAR(MAX) NULL,
  business_unit_id NVARCHAR(MAX) NOT NULL,
  actual_business_unit_id NVARCHAR(MAX) NULL,
  FOREIGN KEY (business_unit_id) REFERENCES business_unit (id)
);
END;

--appointment_assignments
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'appointment_assignments')
BEGIN
CREATE TABLE appointment_assignments (
  id INT PRIMARY KEY,
  technician_id INT NOT NULL,
  actual_technician_id INT NULL,
  technician_name NVARCHAR(MAX) NULL,
  assigned_by_id INT NULL,
  assignedOn DATETIME2 NULL,
  [status] NVARCHAR(MAX) NULL,
  is_paused TINYINT NULL,
  job_details_id INT NOT NULL,
  actual_job_details_id INT NULL,
  appointment_id INT NOT NULL,
  actual_appointment_id INT NULL,
  FOREIGN KEY (technician_id) REFERENCES technician (id),
  FOREIGN KEY (job_details_id) REFERENCES job_details (id),
  FOREIGN KEY (appointment_id) REFERENCES appointments (id),
);
END;

-- non-job-appointments
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'non_job_appointments')
BEGIN
CREATE TABLE non_job_appointments (
  id INT PRIMARY KEY,
  technician_id INT NOT NULL,
  actual_technician_id INT NULL,
  [start] DATETIME2 NULL,
  [name] NVARCHAR(MAX) NULL,
  duration NVARCHAR(MAX) NULL,
  timesheetCodeId INT NULL,
  clearDispatchBoard TINYINT NULL,
  clearTechnicianView TINYINT NULL,
  removeTechnicianFromCapacityPlanning TINYINT NULL,
  is_all_day TINYINT NULL,
  is_active TINYINT NULL,
  createdOn DATETIME2 NULL,
  created_by_id INT NULL,
  FOREIGN KEY (technician_id) REFERENCES technician (id)
);
END;

-- sku_details
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sku_details')
BEGIN
CREATE TABLE sku_details (
  id INT PRIMARY KEY,
  sku_name NVARCHAR(MAX) NULL,
  sku_type NVARCHAR(MAX) NULL,
  sku_unit_price DECIMAL(18, 8) NULL,
  vendor_id INT NOT NULL,
  actual_vendor_id INT NULL,
  FOREIGN KEY (vendor_id) REFERENCES vendor (id)
);
END;

-- invoice
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'invoice')
BEGIN
CREATE TABLE invoice (
  id INT PRIMARY KEY,
  syncStatus NVARCHAR(MAX) NULL,
  date DATETIME2 NULL,
  dueDate DATETIME2 NULL,
  subtotal DECIMAL(18, 8) NULL,
  tax DECIMAL(18, 8) NULL,
  total DECIMAL(18, 8) NULL,
  balance DECIMAL(18, 8) NULL,
  depositedOn DATETIME2 NULL,
  createdOn DATETIME2 NULL,
  modifiedOn DATETIME2 NULL, -- syncStatus, dueDate, 
  invoice_type_id INT NULL,
  invoice_type_name NVARCHAR(MAX) NULL,
  job_details_id INT NOT NULL,
  actual_job_details_id INT NULL,
  project_id INT NOT NULL,
  actual_project_id INT NULL,
  business_unit_id NVARCHAR(MAX) NOT NULL,
  actual_business_unit_id NVARCHAR(MAX) NULL,
  location_id INT NOT NULL,
  actual_location_id INT NULL,
  address_street NVARCHAR(MAX) NULL,
  address_unit NVARCHAR(MAX) NULL,
  address_city NVARCHAR(MAX) NULL,
  address_state NVARCHAR(MAX) NULL,
  address_country NVARCHAR(MAX) NULL,
  address_zip INT NOT NULL,
  acutal_address_zip NVARCHAR(MAX) NULL,
  customer_id INT NOT NULL,
  actual_customer_id INT NULL,
  customer_name NVARCHAR(MAX) NULL,
  FOREIGN KEY (job_details_id) REFERENCES job_details (id),
  FOREIGN KEY (business_unit_id) REFERENCES business_unit (id),
  FOREIGN KEY (location_id) REFERENCES location (id),
  FOREIGN KEY (customer_id) REFERENCES customer_details (id),
  FOREIGN KEY (address_zip) REFERENCES us_cities (id),
  FOREIGN KEY (project_id) REFERENCES projects (id),
);
END;

-- cogs_labor
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'cogs_labor')
BEGIN
CREATE TABLE cogs_labor (
  id INT IDENTITY(1,1) PRIMARY KEY,
  paid_duration DECIMAL(18, 8) NULL,
  burden_rate DECIMAL(18, 8) NULL,
  labor_cost DECIMAL(18, 8) NULL,
  burden_cost DECIMAL(18, 8) NULL,
  activity NVARCHAR(MAX) NULL,
  paid_time_type NVARCHAR(MAX) NULL,
  job_details_id INT NOT NULL,
  actual_job_details_id INT NULL,
  invoice_id INT NOT NULL,
  actual_invoice_id INT NULL,
  project_id INT NOT NULL,
  actual_project_id INT NULL,
  technician_id INT NOT NULL,
  actual_technician_id INT NULL,
  FOREIGN KEY (job_details_id) REFERENCES job_details (id),
  FOREIGN KEY (invoice_id) REFERENCES invoice (id),
  FOREIGN KEY (project_id) REFERENCES projects (id),
  FOREIGN KEY (technician_id) REFERENCES technician (id)
);
END;


-- cogs_material
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'cogs_material')
BEGIN
  CREATE TABLE cogs_material (
    id INT IDENTITY(1,1) PRIMARY KEY,
    quantity DECIMAL(18, 10) NULL,
    cost DECIMAL(18, 10) NULL,
    total_cost DECIMAL(18, 8) NULL,
    price DECIMAL(18, 10) NULL,
    sku_name NVARCHAR(MAX) NULL,
    sku_total DECIMAL(18, 10) NULL,
    generalLedgerAccountid BIGINT NULL,
    generalLedgerAccountname NVARCHAR(MAX) NULL,
    generalLedgerAccountnumber BIGINT NULL,
    generalLedgerAccounttype NVARCHAR(MAX) NULL,
    generalLedgerAccountdetailType NVARCHAR(MAX) NULL,
    job_details_id INT NOT NULL,
    actual_job_details_id INT NULL,
    project_id INT NOT NULL,
    actual_project_id INT NULL,
    invoice_id INT NOT NULL,
    sku_details_id INT NOT NULL,
    actual_sku_details_id INT NULL,
    -- FOREIGN KEY (job_details_id) REFERENCES job_details (id),
    -- FOREIGN KEY (invoice_id) REFERENCES invoice (id),
    -- FOREIGN KEY (sku_details_id) REFERENCES sku_details (id)
  );
END;

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'cogs_service')
BEGIN
  CREATE TABLE cogs_service (
    id INT IDENTITY(1,1) PRIMARY KEY,
    quantity DECIMAL(18, 10) NULL,
    cost DECIMAL(18, 10) NULL,
    total_cost DECIMAL(18, 8) NULL,
    price DECIMAL(18, 10) NULL,
    sku_name NVARCHAR(MAX) NULL,
    sku_total DECIMAL(18, 10) NULL,
    generalLedgerAccountid BIGINT NULL,
    generalLedgerAccountname NVARCHAR(MAX) NULL,
    generalLedgerAccountnumber BIGINT NULL,
    generalLedgerAccounttype NVARCHAR(MAX) NULL,
    generalLedgerAccountdetailType NVARCHAR(MAX) NULL,
    job_details_id INT NOT NULL,
    actual_job_details_id INT NULL,
    project_id INT NOT NULL,
    actual_project_id INT NULL,
    invoice_id INT NOT NULL,
    sku_details_id INT NOT NULL,
    actual_sku_details_id INT NULL,
    -- FOREIGN KEY (job_details_id) REFERENCES job_details (id),
    -- FOREIGN KEY (invoice_id) REFERENCES invoice (id),
    -- FOREIGN KEY (sku_details_id) REFERENCES sku_details (id)
  );
END;


-- cogs_equipment
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'cogs_equipment')
BEGIN
CREATE TABLE cogs_equipment (
  id INT IDENTITY(1,1) PRIMARY KEY,
  quantity DECIMAL(18, 10) NULL,
  cost DECIMAL(18, 10) NULL,
  total_cost DECIMAL(18, 8) NULL,
  price DECIMAL(18, 10) NULL,
  sku_name NVARCHAR(MAX) NULL,
  sku_total DECIMAL(18, 10) NULL,
  generalLedgerAccountid BIGINT NULL,
  generalLedgerAccountname NVARCHAR(MAX) NULL,
  generalLedgerAccountnumber BIGINT NULL,
  generalLedgerAccounttype NVARCHAR(MAX) NULL,
  generalLedgerAccountdetailType NVARCHAR(MAX) NULL,
  job_details_id INT NOT NULL,
  actual_job_details_id INT NULL,
  project_id INT NOT NULL,
  actual_project_id INT NULL,
  invoice_id INT NOT NULL,
  sku_details_id INT NOT NULL,
  actual_sku_details_id INT NULL,
  -- FOREIGN KEY (job_details_id) REFERENCES job_details (id),
  -- FOREIGN KEY (invoice_id) REFERENCES invoice (id),
  -- FOREIGN KEY (sku_details_id) REFERENCES sku_details (id)
);
END;


-- gross_profit
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'gross_profit')
BEGIN
CREATE TABLE gross_profit (
  id INT PRIMARY KEY,
  accounts_receivable DECIMAL(18, 8) NULL,
  expense DECIMAL(18, 8) NULL,
  income DECIMAL(18, 8) NULL,
  current_liability DECIMAL(18, 8) NULL,
  membership_liability DECIMAL(18, 8) NULL,
  [default] DECIMAL(18, 8) NULL,
  total DECIMAL(18, 8) NULL,
  inventory_bill_amount DECIMAL(18, 8) NULL,
  po_cost DECIMAL(18, 8) NULL,
  po_returns DECIMAL(18, 8) NULL,
  equipment_cost DECIMAL(18, 8) NULL,
  material_cost DECIMAL(18, 8) NULL,
  labor_cost DECIMAL(18, 8) NULL,
  burden DECIMAL(18, 8) NULL,
  units INT NULL,
  labor_hours DECIMAL(18, 8) NULL,
);
END;

-- auto_update
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'auto_update')
BEGIN
CREATE TABLE auto_update (
  id INT IDENTITY(1,1) PRIMARY KEY,
  query_date DATETIME2 NULL,
  start_time DATETIME2 NULL,
  end_time DATETIME2 NULL,
  total_minutes DECIMAL(18, 8) NULL,
  legal_entity NVARCHAR(MAX) NULL,
  us_cities NVARCHAR(MAX) NULL,
  business_unit NVARCHAR(MAX) NULL,
  project_business_unit NVARCHAR(MAX) NULL,
  employees NVARCHAR(MAX) NULL,
  campaigns NVARCHAR(MAX) NULL,
  bookings NVARCHAR(MAX) NULL,  
  customer_details NVARCHAR(MAX) NULL,
  [location] NVARCHAR(MAX) NULL,
  gross_pay_items NVARCHAR(MAX) NULL,
  payrolls NVARCHAR(MAX) NULL,
  job_types NVARCHAR(MAX) NULL,
  projects NVARCHAR(MAX) NULL,
  projects_wip_data NVARCHAR(MAX) NULL,
  project_managers NVARCHAR(MAX) NULL,
  call_details NVARCHAR(MAX) NULL,
  job_details NVARCHAR(MAX) NULL,
  appointments NVARCHAR(MAX) NULL,
  sales_details NVARCHAR(MAX) NULL,
  vendor NVARCHAR(MAX) NULL,
  inventory_bills NVARCHAR(MAX) NULL,
  technician NVARCHAR(MAX) NULL,
  appointment_assignments NVARCHAR(MAX) NULL,
  non_job_appointments NVARCHAR(MAX) NULL,
  sku_details NVARCHAR(MAX) NULL,
  invoice NVARCHAR(MAX) NULL,
  cogs_material NVARCHAR(MAX) NULL,
  cogs_equipment NVARCHAR(MAX) NULL,
  cogs_service NVARCHAR(MAX) NULL,
  cogs_labor NVARCHAR(MAX) NULL,
  returns NVARCHAR(MAX) NULL, 
  purchase_order NVARCHAR(MAX) NULL,
  gross_profit NVARCHAR(MAX) NULL,
  overall_status NVARCHAR(MAX) NULL
);
END;