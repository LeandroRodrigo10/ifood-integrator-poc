const pino = require("pino");
require("dotenv").config();

const { db, nowIso } = require("./db");
const { redis, ensureConsumerGroup } = require("./redis");
const { ackEvents, getOrderDetails } = require("./ifood");

const {
  erpCreateOrder,
  erpUpdateOrderStatus,
  erpCancelOrder
} = require("./erpMock");

const logger = pino({ level: "info" });

const STREAM_KEY = process.env.STREAM_KEY;
const GROUP = process.env.CONSUMER_GROUP;
const CONSUMER = process.env.CONSUMER_NAME;

const MAX_ATTEMPTS = Number(process.env.MAX_ATTEMPTS || 5);

function insertLog(eventId, level, message, data) {

  db.prepare(`
    INSERT INTO integration_event_logs
    (event_id, level, message, data_json, created_at)
    VALUES (?, ?, ?, ?, ?)
  `).run(
    eventId,
    level,
    message,
    data ? JSON.stringify(data) : null,
    nowIso()
  );

}

function scheduleRetry(event) {

  const attempts = event.attempts + 1;

  if (attempts >= MAX_ATTEMPTS) {

    db.prepare(`
      UPDATE integration_events
      SET status='failed',
          attempts=?,
          updated_at=?
      WHERE id=?
    `).run(attempts, nowIso(), event.id);

    insertLog(event.id, "error", "MAX_ATTEMPTS_REACHED");

    return;

  }

  db.prepare(`
    UPDATE integration_events
    SET status='retry',
        attempts=?,
        updated_at=?
    WHERE id=?
  `).run(attempts, nowIso(), event.id);

  insertLog(event.id, "warn", "RETRY_SCHEDULED", { attempts });

}

async function processEvent(event) {

  const raw = JSON.parse(event.payload_json);
  const action = event.action;

  insertLog(event.id, "info", "PROCESSING_EVENT", { action });

  if (action === "PLACED") {

    const order = await getOrderDetails(event.entity_id);

    insertLog(event.id, "info", "IFOOD_ORDER_FETCHED", order);

    const erp = await erpCreateOrder(order);

    insertLog(event.id, "info", "ERP_ORDER_CREATED", erp);

  }

  else if (["CONFIRMED","DISPATCHED","CONCLUDED"].includes(action)) {

    const result = await erpUpdateOrderStatus(event.entity_id, action);

    insertLog(event.id, "info", "ERP_STATUS_UPDATED", result);

  }

  else if (["CANCELLATION_REQUESTED","CANCELLED"].includes(action)) {

    const cancel = await erpCancelOrder(event.entity_id);

    insertLog(event.id, "warn", "ERP_ORDER_CANCELLED", cancel);

  }

  else {

    insertLog(event.id, "info", "ACTION_AUDIT_ONLY", { action });

  }

  await ackEvents([raw.id]);

  insertLog(event.id, "info", "IFOOD_EVENT_ACKED");

  db.prepare(`
    UPDATE integration_events
    SET status='done',
        updated_at=?
    WHERE id=?
  `).run(nowIso(), event.id);

}

async function startWorker() {

  await ensureConsumerGroup(STREAM_KEY, GROUP);

  logger.info("worker started");

  while (true) {

    const response = await redis.xreadgroup(
      "GROUP",
      GROUP,
      CONSUMER,
      "COUNT",
      10,
      "BLOCK",
      5000,
      "STREAMS",
      STREAM_KEY,
      ">"
    );

    if (!response) continue;

    const entries = response[0][1];

    for (const entry of entries) {

      const streamId = entry[0];
      const fields = entry[1];

      const eventId = Number(fields[1]);

      const event = db.prepare(`
        SELECT *
        FROM integration_events
        WHERE id=?
      `).get(eventId);

      if (!event) {
        await redis.xack(STREAM_KEY, GROUP, streamId);
        continue;
      }

      try {

        await processEvent(event);

        await redis.xack(STREAM_KEY, GROUP, streamId);

      }

      catch (err) {

        logger.error(err);

        scheduleRetry(event);

        await redis.xack(STREAM_KEY, GROUP, streamId);

      }

    }

  }

}

startWorker();