
const employee_response = require('./employees.json')

const jsonData = { /* ... Your JSON data ... */ };

function convertToSnakeCase(input) {
//   return input.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
    return input;
}

function generateTableAndInsertQueries(data) {
  const sampleObject = data[0];
  let createTableQuery = `CREATE TABLE IF NOT EXISTS my_table (\n`;

  for (const key in sampleObject) {
    if (typeof sampleObject[key] === 'object') {
      for (const nestedKey in sampleObject[key]) {
        const field = convertToSnakeCase(`${key}_${nestedKey}`);
        createTableQuery += `${field} VARCHAR(255),\n`;
      }
    } else {
      const field = convertToSnakeCase(key);
      createTableQuery += `${field} VARCHAR(255),\n`;
    }
  }

  createTableQuery = createTableQuery.slice(0, -2); // Remove the last comma and newline
  createTableQuery += `\n);\n\n`;

  let insertQueries = '';
  for (const item of data) {
    let insertQuery = `INSERT INTO my_table (`;
    let values = '';

    for (const key in item) {
      if (typeof item[key] === 'object') {
        for (const nestedKey in item[key]) {
          const field = convertToSnakeCase(`${key}_${nestedKey}`);
          insertQuery += `${field}, `;
          values += `'${item[key][nestedKey]}', `;
        }
      } else {
        const field = convertToSnakeCase(key);
        insertQuery += `${field}, `;
        values += `'${item[key]}', `;
      }
    }

    insertQuery = insertQuery.slice(0, -2); // Remove the last comma and space
    values = values.slice(0, -2); // Remove the last comma and space
    insertQuery += `) VALUES (${values});\n`;
    insertQueries += insertQuery;
  }

  return createTableQuery + insertQueries;
}

const sqlQueries = generateTableAndInsertQueries(employee_response.data);
console.log(sqlQueries); // or return sqlQueries if you want to use it in a function

