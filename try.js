// const api_collection = [
//   {
//     api_group: "accounting",
//     api_name: "invoices",
//     mode: "items",
//   },
//   {
//     api_group: "inventory",
//     api_name: "adjustments",
//     mode: "items",
//   },
//   {
//     api_group: "inventory",
//     api_name: "transfers",
//     mode: "items",
//   },
//   {
//     api_group: "accounting",
//     api_name: "invoices",
//   },
//   {
//     api_group: "accounting",
//     api_name: "inventory-bills",
//   },
//   {
//     api_group: "accounting",
//     api_name: "payments",
//   },
//   {
//     api_group: "accounting",
//     api_name: "ap-payments",
//   },
//   {
//     api_group: "accounting",
//     api_name: "journal-entries",
//   },
//   {
//     api_group: "accounting",
//     api_name: "payment-terms",
//   },
//   {
//     api_group: "accounting",
//     api_name: "payment-types",
//   },
//   {
//     api_group: "accounting",
//     api_name: "tax-zones",
//   },
//   {
//     api_group: "crm",
//     api_name: "customers",
//   },
//   {
//     api_group: "crm",
//     api_name: "bookings",
//   },
//   {
//     api_group: "crm",
//     api_name: "locations",
//   },
//   {
//     api_group: "crm",
//     api_name: "booking-provider-tags",
//   },
//   {
//     api_group: "crm",
//     api_name: "leads",
//   },
//   {
//     api_group: "dispatch",
//     api_name: "appointment-assignments",
//   },
//   {
//     api_group: "dispatch",
//     api_name: "zones",
//   },
//   {
//     api_group: "dispatch",
//     api_name: "non-job-appointments",
//   },
//   {
//     api_group: "dispatch",
//     api_name: "teams",
//   },
//   {
//     api_group: "dispatch",
//     api_name: "technician-shifts",
//   },
//   {
//     api_group: "equipmentsystems",
//     api_name: "installed-equipment",
//   },
//   {
//     api_group: "inventory",
//     api_name: "adjustments",
//   },
//   {
//     api_group: "inventory",
//     api_name: "transfers",
//   },
//   {
//     api_group: "inventory",
//     api_name: "purchase-orders",
//   },
//   {
//     api_group: "inventory",
//     api_name: "receipts",
//   },
//   {
//     api_group: "inventory",
//     api_name: "returns",
//   },
//   {
//     api_group: "inventory",
//     api_name: "trucks",
//   },
//   {
//     api_group: "inventory",
//     api_name: "vendors",
//   },
//   {
//     api_group: "inventory",
//     api_name: "warehouses",
//   },
//   {
//     api_group: "inventory",
//     api_name: "purchase-order-markups",
//   },
//   {
//     api_group: "inventory",
//     api_name: "purchase-order-types",
//   },
//   {
//     api_group: "jbce",
//     api_name: "call-reasons",
//   },
//   {
//     api_group: "jpm",
//     api_name: "appointments", // end keyword is used in this api response
//   },
//   {
//     api_group: "jpm",
//     api_name: "job-types",
//   },
//   {
//     api_group: "jpm",
//     api_name: "jobs",
//   },
//   {
//     api_group: "jpm",
//     api_name: "projects",
//   },
//   {
//     api_group: "jpm",
//     api_name: "job-cancel-reasons",
//   },
//   {
//     api_group: "jpm",
//     api_name: "job-hold-reasons",
//   },
//   {
//     api_group: "jpm",
//     api_name: "project-statuses",
//   },
//   {
//     api_group: "jpm",
//     api_name: "project-substatuses",
//   },
//   {
//     api_group: "marketing",
//     api_name: "campaigns",
//   },
//   {
//     api_group: "marketing",
//     api_name: "categories",
//   },
//   {
//     api_group: "marketing",
//     api_name: "suppressions",
//   },
//   {
//     api_group: "marketing",
//     api_name: "costs",
//   },
//   {
//     api_group: "marketingreputation",
//     api_name: "reviews",
//   },
//   {
//     api_group: "memberships",
//     api_name: "memberships",
//   },
//   {
//     api_group: "memberships",
//     api_name: "recurring-services",
//   },
//   {
//     api_group: "memberships",
//     api_name: "recurring-service-events",
//   },
//   {
//     api_group: "memberships",
//     api_name: "recurring-service-types",
//   },
//   {
//     api_group: "memberships",
//     api_name: "membership-types",
//   },
//   {
//     api_group: "payroll",
//     api_name: "payrolls",
//   },
//   {
//     api_group: "payroll",
//     api_name: "payroll-adjustments",
//   },
//   {
//     api_group: "payroll",
//     api_name: "gross-pay-items",
//   },
//   {
//     api_group: "payroll",
//     api_name: "jobs/splits",
//   },
//   {
//     api_group: "payroll",
//     api_name: "jobs/timesheets",
//   },
//   {
//     api_group: "payroll",
//     api_name: "timesheet-codes",
//   },
//   {
//     api_group: "payroll",
//     api_name: "activity-codes",
//   },
//   {
//     api_group: "payroll",
//     api_name: "locations/rates",
//   },
//   {
//     api_group: "payroll",
//     api_name: "non-job-timesheets",
//   },
//   {
//     api_group: "pricebook",
//     api_name: "categories",
//   },
//   {
//     api_group: "pricebook",
//     api_name: "equipment",
//   },
//   {
//     api_group: "pricebook",
//     api_name: "materials",
//   },
//   {
//     api_group: "pricebook",
//     api_name: "discounts-and-fees",
//   },
//   {
//     api_group: "pricebook",
//     api_name: "images",
//   },
//   {
//     api_group: "pricebook",
//     api_name: "services",
//   },
//   {
//     api_group: "pricebook",
//     api_name: "materialsmarkup",
//   },
//   {
//     api_group: "reporting",
//     api_name: "report-categories",
//   },
//   {
//     api_group: "sales",
//     api_name: "estimates",
//   },
//   {
//     api_group: "sales",
//     api_name: "estimates/export",
//   },
//   {
//     api_group: "sales",
//     api_name: "estimates/items",
//   },
//   {
//     api_group: "service-agreements",
//     api_name: "service-agreements",
//   },
//   {
//     api_group: "settings",
//     api_name: "business-units",
//   },
//   {
//     api_group: "settings",
//     api_name: "employees",
//   },
//   {
//     api_group: "settings",
//     api_name: "technicians",
//   },
//   {
//     api_group: "settings",
//     api_name: "tag-types",
//   },
//   {
//     api_group: "settings",
//     api_name: "user-roles",
//   },
//   {
//     api_group: "taskmanagement",
//     api_name: "data",
//   },
//   {
//     api_group: "telecom",
//     api_name: "export/calls",
//   },
//   {
//     api_group: "telecom",
//     api_name: "calls",
//   },
//   {
//     api_group: "forms",
//     api_name: "forms",
//   },
//   {
//     api_group: "forms",
//     api_name: "submissions",
//   },
// ];
