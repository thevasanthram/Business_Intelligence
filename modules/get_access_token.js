async function getAccessToken(client_id, client_secret) {
  let access_token = "";
  try {
    const auth_url = "https://auth.servicetitan.io/connect/token";

    const auth_response = await fetch(auth_url, {
      method: "POST",
      headers: {
        "Content-type": "application/x-www-form-urlencoded",
      },
      body: new URLSearchParams({
        grant_type: "client_credentials",
        client_id: client_id,
        client_secret: client_secret,
      }),
    });

    const auth_data = await auth_response.json();
    access_token = auth_data.access_token;

    // console.log('Access Token: ', access_token);
  } catch (error) {
    access_token = await getAccessToken(client_id, client_secret);
    console.error("Error while getting access token:", error);
  }

  return access_token;
}

module.exports = getAccessToken;
