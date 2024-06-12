const fields = [
    { "name": "ProjectNumber", "label": "Project #" },
    { "name": "CustomerName", "label": "Customer Name" },
    { "name": "ProjectName", "label": "Project Name" },
    { "name": "ProjectStatus", "label": "Project Status" },
    { "name": "ProjectStartDate", "label": "Project Start Date" },
    { "name": "ActualCompletionDate", "label": "Actual Completion Date" },
    { "name": "ContractValue", "label": "Contract Value" },
    { "name": "ChangeOrderValue", "label": "Change Order Value" },
    { "name": "CostAdjustment", "label": "Cost Adjustment" },
    { "name": "TotalEstimatedCost", "label": "Est. Total Cost" },
    { "name": "EstimatedMargin", "label": "Est. Margin" },
    { "name": "EstimatedMarginPercentage", "label": "Est. Margin %" },
    { "name": "TotalCost", "label": "Total Cost" },
    { "name": "CostToComplete", "label": "Cost To Complete" },
    { "name": "PercentCompleteCost", "label": "% Complete Cost" },
    { "name": "EarnedRevenue", "label": "Earned Revenue" },
    { "name": "TotalRevenue", "label": "Revenue" },
    { "name": "OverBilling", "label": "Over Billing" },
    { "name": "UnderBilling", "label": "Under Billing" }
];

const data = [
    "31461989",
    "KNL - Bradford, Barry",
    "KNL - (P) Bradford, Barry Plumb/HVAC",
    "In Progress",
    "2023-02-23T00:00:00-05:00",
    null,
    135281.00,
    0.00,
    0.00,
    135281.00,
    0.00,
    0.00,
    40691.48,
    94589.52,
    0.30,
    40584.30,
    90631.45,
    50047.15,
    null
];

const record = fields.reduce((obj, field, index) => {
    obj[field.name] = data[index];
    return obj;
}, {});

console.log(record);
