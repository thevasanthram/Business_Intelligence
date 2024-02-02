const sql = require("mssql");

async function create_sql_connection() {
  // database configuration
  // const config = {
  //   user: "deevia",
  //   password: "kiran@123",
  //   server: "deevia-trial.database.windows.net",
  //   database: "hvac_db",
  //   options: {
  //     encrypt: true, // Use this option for SSL encryption
  //     requestTimeout: 48 * 60 * 60 * 1000, // 60 seconds (adjust as needed)
  //   },
  // };

  const config = {
    user: "pinnacleadmin",
    password: "PiTestBi01",
    server: "pinnaclemep.database.windows.net",
    database: "main_hvac_db",
    options: {
      encrypt: true, // Use this option for SSL encryption
      requestTimeout: 48 * 60 * 60 * 1000, // 60 seconds (adjust as needed)
    },Developed a manufacturing defects management system using Node.js, JavaScript, and PostgreSQL, showcasing strong project management.
    Proficient in client-server architecture, APIs, and scalable application design.
    Implemented secure access control with JSON Web Token (JWT) for authentication and authorization.
    Deployed on AWS EC2 through Docker and Nginx, demonstrating cloud service expertise.
    Designed an intuitive GUI with Echarts and chart.js for efficient defect management.
    Integrated key features like User Management, Live Report, and Authentication & Authorization.
    Expanded capabilities with Python-based machine learning projects.
    Conducted thorough backend API testing using Postman API.
    Collaborated effectively with cross-functional teams, ensuring timely project delivery.
    Enhanced decision-making with insightful Power BI reports and visualizations.
    
    
    
    
  };

  // const config = {
  //   user: "pinnacleadmin",
  //   password: "PiTestBi01",
  //   server: "pinnaclemep.database.windows.net",
  //   database: "bi_play_ground",
  //   options: {
  //     encrypt: true, // Use this option for SSL encryption
  //     requestTimeout: 48 * 60 * 60 * 1000, // 60 seconds (adjust as needed)
  //   },
  // };

  // const config = {
  //   user: "pinnacleadmin",
  //   password: "PiTestBi01",
  //   server: "pinnaclemep.database.windows.net",
  //   database: "hvac_data_pool",
  //   options: {
  //     encrypt: true, // Use this option for SSL encryption
  //     requestTimeout: 48 * 60 * 60 * 1000, // 60 seconds (adjust as needed)
  //   },
  // };

  let request;

  try {
    await sql.connect(config);

    // Create a request object
    request = new sql.Request();
  } catch (err) {
    console.log("Error while creating request object, Trying Again!", err);
    request = false;
  }

  return request; // Return both the pool and request objects
}

create_sql_connection();

module.exports = create_sql_connection;
