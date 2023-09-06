const sql = require("mssql");

// Database connection configuration
const config = {
  user: "deevia",
  password: "kiran@123",
  server: "deevia-trial.database.windows.net",
  database: "crop_db",
  options: {
    encrypt: true, // Use this option for SSL encryption
  },
};

async function connectToAzureSQL() {
  try {
    await sql.connect(config); // Connect to the database
    console.log("Connected to Azure SQL Database");

    const request = new sql.Request();

    const insert_customer_details = await request.query(
      `IF NOT EXISTS (SELECT 1 FROM customer_details WHERE customer_id = 32170497)
        BEGIN
          INSERT INTO customer_details (customer_id, customer_name, customer_address_state, customer_address_unit, customer_address_city, customer_address_zip, customer_address_country) VALUES ('32188120', 'GAY, DOROTHY', 'null', 'null', 'WOODHAVEN MI', '48183', 'null');
        END;`
    );

    console.log("Query results:", insert_customer_details);
  } catch (error) {
    console.error("Error:", error);
  } finally {
    await sql.close(); // Close the database connection
  }
}

// Call the function to connect and query the database
connectToAzureSQL();
