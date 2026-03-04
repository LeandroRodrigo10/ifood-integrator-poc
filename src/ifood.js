const axios = require("axios");
require("dotenv").config();

const BASE_URL = "https://merchant-api.ifood.com.br";

let tokenCache = {
  accessToken: null,
  expiresAt: 0
};

function tokenValid() {
  return tokenCache.accessToken && Date.now() < tokenCache.expiresAt;
}

async function getAccessToken() {

  if (tokenValid()) {
    return tokenCache.accessToken;
  }

  const url = `${BASE_URL}/authentication/v1.0/oauth/token`;

  const params = new URLSearchParams();
  params.append("grantType", "client_credentials");
  params.append("clientId", process.env.IFOOD_CLIENT_ID);
  params.append("clientSecret", process.env.IFOOD_CLIENT_SECRET);

  const response = await axios.post(
    url,
    params.toString(),
    {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded"
      }
    }
  );

  const data = response.data;

  tokenCache.accessToken = data.accessToken;
  tokenCache.expiresAt = Date.now() + (data.expiresIn * 1000);

  return tokenCache.accessToken;
}

async function pollEvents() {

  const token = await getAccessToken();

  const url = `${BASE_URL}/order/v1.0/events:polling`;

  const response = await axios.get(
    url,
    {
      headers: {
        Authorization: `Bearer ${token}`
      }
    }
  );

  return response.data;
}

async function ackEvents(eventIds) {

  if (!eventIds.length) return;

  const token = await getAccessToken();

  const url = `${BASE_URL}/order/v1.0/events/acknowledgment`;

  const body = eventIds.map(id => ({ id }));

  const response = await axios.post(
    url,
    body,
    {
      headers: {
        Authorization: `Bearer ${token}`
      }
    }
  );

  return response.status === 202;
}

async function getOrderDetails(orderId) {

  const token = await getAccessToken();

  const url = `${BASE_URL}/order/v1.0/orders/${orderId}`;

  const response = await axios.get(
    url,
    {
      headers: {
        Authorization: `Bearer ${token}`
      }
    }
  );

  return response.data;
}

module.exports = {
  getAccessToken,
  pollEvents,
  ackEvents,
  getOrderDetails
};