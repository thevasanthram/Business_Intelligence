-- legal_entity
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'legal_entity')
BEGIN
CREATE TABLE legal_entity (
  id INT PRIMARY KEY,
  legal_name NVARCHAR(MAX) NULL
);
END;

-- business_unit
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'business_unit')
BEGIN
CREATE TABLE business_unit (
  id INT PRIMARY KEY,
  business_unit_name NVARCHAR(MAX) NULL,
  business_unit_official_name NVARCHAR(MAX) NULL,
  trade_type NVARCHAR(MAX) NULL,
  revenue_type NVARCHAR(MAX) NULL,
  account_type NVARCHAR(MAX) NULL,
  legal_entity_id INT NOT NULL,
  FOREIGN KEY (legal_entity_id) REFERENCES legal_entity (id)
);
END;

-- customer_details
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'customer_details')
BEGIN
CREATE TABLE customer_details (
  id INT PRIMARY KEY,
  name NVARCHAR(MAX) NULL,
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
  street NVARCHAR(MAX) NULL,
  unit NVARCHAR(MAX) NULL,
  city NVARCHAR(MAX) NULL,
  state NVARCHAR(MAX) NULL,
  zip NVARCHAR(MAX) NULL,
  taxzone INT NULL,
  zone_id INT NULL
);
END;

-- job_details
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'job_details')
BEGIN
CREATE TABLE job_details (
  id INT PRIMARY KEY,
  job_type_id INT NULL,
  job_type_name NVARCHAR(MAX) NULL,
  job_number NVARCHAR(MAX) NULL,
  job_status NVARCHAR(MAX) NULL,
  job_start_time DATETIME NULL,
  project_id INT NULL,
  job_completion_time DATETIME NULL,
  business_unit_id INT NOT NULL,
  location_id INT NOT NULL,
  customer_details_id INT NOT NULL,
  campaign_id INT NULL,
  created_by_id INT NULL,
  lead_call_id INT NULL,
  booking_id INT NULL,
  sold_by_id INT NULL,
  FOREIGN KEY (business_unit_id) REFERENCES business_unit (id),
  FOREIGN KEY (location_id) REFERENCES location (id),
  FOREIGN KEY (customer_details_id) REFERENCES customer_details (id)
);
END;

-- vendor_details
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'vendor')
BEGIN
CREATE TABLE vendor (
  id INT PRIMARY KEY,
  name NVARCHAR(MAX) NULL,
  is_active TINYINT NULL
);
END;

-- sku_details
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sku_details')
BEGIN
CREATE TABLE sku_details (
  id INT PRIMARY KEY,
  sku_name NVARCHAR(MAX) NULL,
  sku_type NVARCHAR(MAX) NULL,
  sku_unit_price DECIMAL NULL,
  vendor_id INT NOT NULL,
  FOREIGN KEY (vendor_id) REFERENCES vendor (id)
);
END;

-- cogs_equipment
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'cogs_equipment')
BEGIN
CREATE TABLE cogs_equipment (
  id INT PRIMARY KEY,
  quantity INT NULL,
  total_cost DECIMAL(10, 0) NULL,
  job_details_id INT NOT NULL,
  sku_details_id INT NOT NULL,
  FOREIGN KEY (job_details_id) REFERENCES job_details (id),
  FOREIGN KEY (sku_details_id) REFERENCES sku_details (id)
);
END;

-- technician
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'technician')
BEGIN
CREATE TABLE technician (
  id INT PRIMARY KEY,
  name NVARCHAR(MAX) NULL,
  business_unit_id INT NOT NULL,
  FOREIGN KEY (business_unit_id) REFERENCES business_unit (id)
);
END;

-- cogs_labor
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'cogs_labor')
BEGIN
CREATE TABLE cogs_labor (
  id INT PRIMARY KEY,
  paid_duration DECIMAL(10, 0) NULL,
  burden_rate DECIMAL NULL,
  labor_cost DECIMAL(10, 0) NULL,
  burden_cost DECIMAL(10, 0) NULL,
  activity NVARCHAR(MAX) NULL,
  paid_time_type NVARCHAR(MAX) NULL,
  job_details_id INT NOT NULL,
  technician_id INT NOT NULL,
  FOREIGN KEY (job_details_id) REFERENCES job_details (id),
  FOREIGN KEY (technician_id) REFERENCES technician (id)
);
END;

-- cogs_material
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'cogs_material')
BEGIN
CREATE TABLE cogs_material (
  id INT PRIMARY KEY,
  quantity INT NULL,
  total_cost DECIMAL(10, 0) NULL,
  job_details_id INT NOT NULL,
  sku_details_id INT NOT NULL,
  FOREIGN KEY (job_details_id) REFERENCES job_details (id),
  FOREIGN KEY (sku_details_id) REFERENCES sku_details (id)
);
END;

-- invoice
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'invoice')
BEGIN
CREATE TABLE invoice (
  id INT PRIMARY KEY,
  is_trialdata TINYINT NULL,
  date DATETIME NULL,
  subtotal DECIMAL(10, 0) NULL,
  tax DECIMAL(10, 0) NULL,
  total DECIMAL(10, 0) NULL,
  invoice_type_id INT NULL,
  invoice_type_name NVARCHAR(MAX) NULL,
  job_details_id INT NOT NULL,
  FOREIGN KEY (job_details_id) REFERENCES job_details (id)
);
END;

-- gross_profit
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'gross_profit')
BEGIN
CREATE TABLE gross_profit (
  id INT PRIMARY KEY,
  revenue DECIMAL(10, 0) NULL,
  po_cost DECIMAL(10, 0) NULL,
  equipment_cost DECIMAL(10, 0) NULL,
  material_cost DECIMAL(10, 0) NULL,
  labor_cost DECIMAL(10, 0) NULL,
  burden DECIMAL(10, 0) NULL,
  gross_profit DECIMAL(10, 0) NULL,
  gross_margin DECIMAL(10, 0) NULL,
  units INT NULL,
  labor_hours DECIMAL(10, 0) NULL,
  invoice_id INT NOT NULL,
  FOREIGN KEY (invoice_id) REFERENCES invoice (id)
);
END;