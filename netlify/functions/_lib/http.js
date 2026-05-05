const defaultHeaders = {
  'Content-Type': 'application/json; charset=utf-8',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Webhook-Secret',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
};

const json = (statusCode, payload, extraHeaders = {}) => ({
  statusCode,
  headers: {
    ...defaultHeaders,
    ...extraHeaders
  },
  body: JSON.stringify(payload)
});

const parseJsonBody = (event) => {
  if (!event || !event.body) {
    return {};
  }

  if (typeof event.body === 'object') {
    return event.body;
  }

  try {
    return JSON.parse(event.body);
  } catch (error) {
    return {};
  }
};

module.exports = {
  json,
  parseJsonBody
};
