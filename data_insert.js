

let access_token = '';
const auth_url = 'https://auth.servicetitan.io/connect/token';
const api_url = 'https://api.servicetitan.io/accounting/v2/tenant/1011756844/export/invoices';

async function getAccessToken() {
    try {
        const auth_response = await fetch(auth_url, {
            method: 'POST',
            headers: {
                'Content-type': 'application/x-www-form-urlencoded'
            },
            body: new URLSearchParams({
                grant_type: 'client_credentials',
                client_id: 'cid.jk53hfwwcq6a1zgtbh96byil4',
                client_secret: 'cs1.2hdc1yd19hpxzmdeg5rfuc6i3smpxy9iei0yhq1p7qp8mwyjda',
            })
        });

        const auth_data = await auth_response.json();
        access_token = auth_data.access_token;
        // console.log('Access Token: ', access_token);
    } catch (error) {
        console.error('Error while getting access token:', error);
    }
}

async function getAPIData() {
    try {
        const api_response = await fetch(api_url, {
            method: 'GET',
            headers: {
                'Content-type': 'application/x-www-form-urlencoded',
                Authorization: access_token, // Include "Bearer" before access token
                'ST-App-Key': 'ak1.ztsdww9rvuk0sjortd94dmxwx'
            },
            params: new URLSearchParams({
                includeRecentChanges: true,
                from: '2023-08-01',
            })
        });

        const api_data = await api_response.json();

        console.log('sample api_data: ', api_data['data'][0])

        // convert json into sql query

        // Function to flatten a nested object without any separator
        function flattenObject(obj, parentKey = '') {
            let result = {};
            for (const key in obj) {
            if (typeof obj[key] === 'object' && obj[key] !== null) {
                const flattened = flattenObject(obj[key], parentKey + key);
                result = { ...result, ...flattened };
            } else {
                result[parentKey + key] = obj[key];
            }
            }
            return result;
        }
        
        const tableName = 'invoice_api_data'; // Replace with your table name
        
        // Generate SQL statements to create the table
        const sampleObj = api_data['data'][0]; // Take a sample object to infer the table structure
        const flattenedSampleObj = flattenObject(sampleObj);
        
        const createTableSQL = `CREATE TABLE IF NOT EXISTS ${tableName} ( ${Object.keys(flattenedSampleObj).map(key => `${key} TEXT`).join(', ')} );`;

        console.log('=============================================================================================================================================================================================================================')

        console.log('createTableSQL: ', createTableSQL)

        console.log('=============================================================================================================================================================================================================================')
        
                
        // Generate SQL statements to insert data
        api_data['data'].forEach((currentObj, index) => {
            const flattenedObj = flattenObject(currentObj);
        
            const insertDataSQL = `INSERT INTO ${tableName} (${Object.keys(flattenedObj).join(', ')}) VALUES (${Object.values(flattenedObj).map(value => `'${value}'`).join(', ')});`;
        
            if(index == 0){
              console.log('insertDataSQL: ', insertDataSQL);
              console.log('=============================================================================================================================================================================================================================')
            }

        });        

    } catch (error) {
        console.error('Error while processing data or storing into db:', error);
    }
}

async function manager() {
    await getAccessToken();
    await getAPIData();
}

// Initial execution
manager();

// Refresh access token every 13 minutes
setInterval(() => {
    getAccessToken();
}, 780000);
