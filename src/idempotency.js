const crypto = require("crypto");

function makeIdempotencyKey({
  marketplace,
  entityType,
  entityId,
  action,
  rawEventId
}) {
  const base = rawEventId
    ? String(rawEventId)
    : `${marketplace}:${entityType}:${entityId}:${action}`;

  return crypto
    .createHash("sha256")
    .update(base)
    .digest("hex");
}

module.exports = { makeIdempotencyKey };