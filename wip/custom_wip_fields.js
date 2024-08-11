const getAccessToken = require("./../modules/get_access_token");

const instance_details = [
  {
    instance_name: "Expert Heating and Cooling Co LLC",
    tenant_id: 1011756844,
    app_key: "ak1.ztsdww9rvuk0sjortd94dmxwx",
    client_id: "cid.jk53hfwwcq6a1zgtbh96byil4",
    client_secret: "cs1.2hdc1yd19hpxzmdeg5rfuc6i3smpxy9iei0yhq1p7qp8mwyjda",
    wip_report_id: 87933193,
    active_project_id: 91683578,
    completed_projects_id: 91678548,
  },
  {
    instance_name: "PARKER-ARNTZ PLUMBING AND HEATING, INC.",
    tenant_id: 1475606437,
    app_key: "ak1.w9fgjo8psqbyi84vocpvzxp8y",
    client_id: "cid.r82bhd4u7htjv56h7sqjk0jya",
    client_secret: "cs1.4q3yjgyhjb9yaeietpsoozzc8u2qgw80j8ze43ovz1308e7zz7",
    wip_report_id: 85353848,
    active_project_id: 94758741,
    completed_projects_id: 94755857,
  },
  {
    instance_name: "Family Heating & Cooling Co LLC",
    tenant_id: 1056112968,
    app_key: "ak1.h0wqje4yshdqvn1fso4we8cnu",
    client_id: "cid.qlr4t6egndd4mbvq3vu5tef11",
    client_secret: "cs1.v9jhueeo6kgcjx5in1r8716hpnmuh6pbxiddgsv5d3y0822jay",
    wip_report_id: 75359909,
    active_project_id: 79737287,
    completed_projects_id: 79746676,
  },
  {
    instance_name: "Swift Air Mechanical LLC",
    tenant_id: 2450322465,
    app_key: "ak1.bquspwfwag2gqtls6lgi0dl1b",
    client_id: "cid.ls3q3h7dmtoaiu3o5y5oddgca",
    client_secret: "cs1.75f7fqxtilh4xj5vkym3fda1aaz9kmpv701w91kaej9w2r2rxp",
    wip_report_id: 58999783,
    active_project_id: 72752883,
    completed_projects_id: 72763695,
  },
  //   {
  //     instance_name: "Jetstream Mechanicals LLC",
  //     tenant_id: 2450309401,
  //     app_key: "ak1.u9fb0767d46nh1mid81ow3pgz",
  //     client_id: "cid.my53hbp127i8vzxpn4o4h54ho",
  //     client_secret: "cs1.3t0bo8k8b8xgrnbdyf9j4cj8zeq2upq2z72x9h4wkmf1w692jc",
  //     wip_report_id: 180419843,
  //     active_project_id: 258292875,
  //     completed_projects_id: 258288362,
  //   },
];

async function get_custom_wip_report_fields(
  access_token,
  app_key,
  tenant_id,
  wip_report_id,
  fields_data,
  wip_table_name
) {
  let has_error_occured = false;
  const api_url = `https://api.servicetitan.io/reporting/v2/tenant/${tenant_id}/report-category/operations/reports/${wip_report_id}`;

  const request = await fetch(api_url, {
    method: "GET",
    headers: {
      "Content-type": "application/json",
      Authorization: access_token, // Include "Bearer" before access token
      "ST-App-Key": app_key,
    },
  });

  const response = await request.json();

  fields_data = response.fields;

  return { fields_data, has_error_occured };
}

let fields_data_collection = {
  "Expert Heating and Cooling Co LLC": {
    wip_active_projects: {},
    wip_completed_projects: {},
  },
  "PARKER-ARNTZ PLUMBING AND HEATING, INC.": {
    wip_active_projects: {},
    wip_completed_projects: {},
  },
  "Family Heating & Cooling Co LLC": {
    wip_active_projects: {},
    wip_completed_projects: {},
  },
  "Swift Air Mechanical LLC": {
    wip_active_projects: {},
    wip_completed_projects: {},
  },
  "Jetstream Mechanicals LLC": {
    wip_active_projects: {},
    wip_completed_projects: {},
  },
};

let output = {
  wip_active_projects: [],
  wip_completed_projects: [],
};

async function getSeparateUniqueNames(fieldsDataCollection) {
  let uniqueActiveProjectNames = [];
  let uniqueCompletedProjectNames = [];

  Object.keys(fieldsDataCollection).map((instance_name) => {
    const instance_data = fieldsDataCollection[instance_name];

    uniqueActiveProjectNames.push(...instance_data["wip_active_projects"]);
    uniqueCompletedProjectNames.push(
      ...instance_data["wip_completed_projects"]
    );
  });

  uniqueActiveProjectNames = [...new Set(...uniqueActiveProjectNames)];
  uniqueActiveProjectNames = [...new Set(...uniqueActiveProjectNames)];
}

async function custom_wip_fields(instance_details) {
  const table_list = [
    {
      table_name: "wip_active_projects",
      column_name: "active_project_id",
    },
    {
      table_name: "wip_completed_projects",
      column_name: "completed_projects_id",
    },
  ];

  for (let i = 0; i < 2; i++) {
    const wip_table_name = table_list[i]["table_name"];
    const column_name = table_list[i]["column_name"];

    await Promise.all(
      instance_details.map(async (instance_data) => {
        const instance_name = instance_data["instance_name"];
        const tenant_id = instance_data["tenant_id"];
        const app_key = instance_data["app_key"];
        const client_id = instance_data["client_id"];
        const client_secret = instance_data["client_secret"];
        const wip_report_id = instance_data[column_name];

        let access_token = "";

        // refreshing for the first time manually
        do {
          access_token = await getAccessToken(client_id, client_secret);
        } while (!access_token);

        const refreshAccessToken = async () => {
          // Signing a new access token in Service Titan's API
          do {
            access_token = await getAccessToken(client_id, client_secret);
          } while (!access_token);
        };

        setInterval(refreshAccessToken, 1000 * 60 * 3);

        // continuously fetching whole api data
        let fields_data = [];
        let has_error_occured = false;

        do {
          ({ fields_data, has_error_occured } =
            await get_custom_wip_report_fields(
              access_token,
              app_key,
              tenant_id,
              wip_report_id,
              fields_data,
              wip_table_name
            ));
        } while (has_error_occured);

        if (fields_data.length > 0) {
          fields_data_collection[instance_name][wip_table_name] = fields_data;
        }
      })
    );
  }

  const { uniqueActiveProjectNames, uniqueCompletedProjectNames } =
    await getSeparateUniqueNames(fields_data_collection);
  console.log("Unique Active Project Names:", uniqueActiveProjectNames);
  console.log("Unique Completed Project Names:", uniqueCompletedProjectNames);

  console.log("fields_data_collection: ", fields_data_collection);
}

custom_wip_fields(instance_details);

module.exports = custom_wip_fields;
