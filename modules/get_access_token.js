async function getAccessToken() {
  try {
    const auth_url = "https://auth.servicetitan.io/connect/token";
    const auth_response = await fetch(auth_url, {
      method: "POST",
      headers: {
        "Content-type": "application/x-www-form-urlencoded",
      },
      body: new URLSearchParams({
        grant_type: "client_credentials",
        client_id: "cid.jk53hfwwcq6a1zgtbh96byil4",
        client_secret: "cs1.2hdc1yd19hpxzmdeg5rfuc6i3smpxy9iei0yhq1p7qp8mwyjda",
      }),
    });

    const auth_data = await auth_response.json();
    const access_token = auth_data.access_token;

    return access_token;
    // console.log('Access Token: ', access_token);
  } catch (error) {
    console.error("Error while getting access token:", error);
  }
}

module.exports = getAccessToken;
