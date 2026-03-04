const { makeIdempotencyKey } = require("./idempotency");

function normalizeIfoodEvent(raw) {

  const action = raw.fullCode || raw.code || "UNKNOWN";
  const entityId = raw.orderId || "UNKNOWN_ORDER";
  const storeId = raw.merchantId || "UNKNOWN_MERCHANT";

  const idempotencyKey = makeIdempotencyKey({
    marketplace: "IFOOD",
    entityType: "ORDER",
    entityId,
    action,
    rawEventId: raw.id
  });

  return {
    store_id: storeId,
    marketplace: "IFOOD",
    direction: "IN",
    topic: "ORDER",
    action: action,
    entity_type: "ORDER",
    entity_id: entityId,
    idempotency_key: idempotencyKey,
    payload_json: JSON.stringify(raw)
  };
}

module.exports = { normalizeIfoodEvent };