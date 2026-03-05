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

const logger = pino({ level: process.env.LOG_LEVEL || "info" });

const STREAM_KEY = process.env.STREAM_KEY;
const GROUP = process.env.CONSUMER_GROUP;
const CONSUMER = process.env.CONSUMER_NAME || `worker-${process.pid}`;

const READ_COUNT = Number(process.env.REDIS_READ_COUNT || 10);
const BLOCK_MS = Number(process.env.REDIS_BLOCK_MS || 5000);

const MAX_ATTEMPTS = Number(process.env.MAX_ATTEMPTS || 6);

const BASE_BACKOFF_SECONDS = Number(process.env.BACKOFF_BASE_SECONDS || 5);
const MAX_BACKOFF_SECONDS = Number(process.env.BACKOFF_MAX_SECONDS || 300);

// ===============================
// Helpers
// ===============================

function calcBackoffSeconds(attempts) {
  const exp = Math.max(0, attempts - 1);
  const seconds = BASE_BACKOFF_SECONDS * Math.pow(2, exp);
  return Math.min(seconds, MAX_BACKOFF_SECONDS);
}

function parseIsoOrNull(value) {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

function msUntil(dueDate) {
  const ms = dueDate.getTime() - Date.now();
  return ms > 0 ? ms : 0;
}

function fieldsToObject(fieldsArray) {
  const obj = {};
  for (let i = 0; i < fieldsArray.length; i += 2) {
    const k = fieldsArray[i];
    const v = fieldsArray[i + 1];
    obj[String(k)] = v;
  }
  return obj;
}

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

function scheduleRequeue(eventId, delayMs) {
  setTimeout(async () => {
    try {
      await redis.xadd(STREAM_KEY, "*", "eventId", String(eventId));
      logger.info({ eventId, delayMs }, "requeued after delay");
    } catch (err) {
      logger.error({ eventId, err: err?.message }, "failed to requeue after delay");
    }
  }, delayMs);
}

// ===============================
// DB statements
// ===============================

const getEventStmt = db.prepare(`
  SELECT *
  FROM integration_events
  WHERE id = ?
`);

const markDoneStmt = db.prepare(`
  UPDATE integration_events
  SET status='done',
      next_run_at=NULL,
      updated_at=?
  WHERE id=?
`);

const markRetryStmt = db.prepare(`
  UPDATE integration_events
  SET status='retry',
      attempts=?,
      next_run_at=?,
      updated_at=?
  WHERE id=?
`);

const markFailedStmt = db.prepare(`
  UPDATE integration_events
  SET status='failed',
      attempts=?,
      next_run_at=NULL,
      updated_at=?
  WHERE id=?
`);

// ===============================
// Processor (somente regras de negócio, sem mexer no status no DB)
// ===============================

async function processEvent(event) {
  const raw = JSON.parse(event.payload_json);
  const action = event.action;

  insertLog(event.id, "info", "PROCESSING_EVENT", { action });

  if (action === "PLACED") {
    const order = await getOrderDetails(event.entity_id);
    insertLog(event.id, "info", "IFOOD_ORDER_FETCHED", order);

    const erp = await erpCreateOrder(order);
    insertLog(event.id, "info", "ERP_ORDER_CREATED", erp);
  } else if (["CONFIRMED", "DISPATCHED", "CONCLUDED"].includes(action)) {
    const result = await erpUpdateOrderStatus(event.entity_id, action);
    insertLog(event.id, "info", "ERP_STATUS_UPDATED", result);
  } else if (["CANCELLATION_REQUESTED", "CANCELLED"].includes(action)) {
    const cancel = await erpCancelOrder(event.entity_id);
    insertLog(event.id, "warn", "ERP_ORDER_CANCELLED", cancel);
  } else {
    insertLog(event.id, "info", "ACTION_AUDIT_ONLY", { action });
  }

  await ackEvents([raw.id]);
  insertLog(event.id, "info", "IFOOD_EVENT_ACKED");
}

// ===============================
// Worker loop
// ===============================

async function startWorker() {
  await ensureConsumerGroup(STREAM_KEY, GROUP);

  logger.info(
    {
      STREAM_KEY,
      GROUP,
      CONSUMER,
      READ_COUNT,
      BLOCK_MS,
      MAX_ATTEMPTS,
      BASE_BACKOFF_SECONDS,
      MAX_BACKOFF_SECONDS
    },
    "worker started"
  );

  while (true) {
    let response;

    try {
      response = await redis.xreadgroup(
        "GROUP",
        GROUP,
        CONSUMER,
        "COUNT",
        READ_COUNT,
        "BLOCK",
        BLOCK_MS,
        "STREAMS",
        STREAM_KEY,
        ">"
      );
    } catch (err) {
      logger.error({ err: err?.message }, "xreadgroup failed");
      continue;
    }

    if (!response) continue;

    const entries = response[0][1];

    for (const entry of entries) {
      const streamId = entry[0];
      const fieldsArr = entry[1];
      const fields = fieldsToObject(fieldsArr);

      const eventId = Number(fields.eventId);

      if (!Number.isFinite(eventId)) {
        logger.warn({ streamId, fields }, "invalid eventId, acking");
        await redis.xack(STREAM_KEY, GROUP, streamId);
        continue;
      }

      const event = getEventStmt.get(eventId);

      if (!event) {
        logger.warn({ eventId, streamId }, "event not found in db, acking");
        await redis.xack(STREAM_KEY, GROUP, streamId);
        continue;
      }

      // Skip retry not due
      if (event.status === "retry") {
        const due = parseIsoOrNull(event.next_run_at);
        if (due && due.getTime() > Date.now()) {
          const delayMs = msUntil(due);

          logger.info(
            { eventId, next_run_at: event.next_run_at, delayMs },
            "retry not due, scheduling requeue"
          );

          scheduleRequeue(eventId, delayMs);

          await redis.xack(STREAM_KEY, GROUP, streamId);
          continue;
        }
      }

      try {
        await processEvent(event);

        markDoneStmt.run(nowIso(), eventId);
        insertLog(eventId, "info", "EVENT_DONE");

        await redis.xack(STREAM_KEY, GROUP, streamId);
      } catch (err) {
        const errMsg = err?.message || String(err);

        logger.warn({ eventId, err: errMsg }, "processing failed");

        const nextAttempts = (event.attempts || 0) + 1;

        if (nextAttempts >= MAX_ATTEMPTS) {
          markFailedStmt.run(nextAttempts, nowIso(), eventId);
          insertLog(eventId, "error", "MAX_ATTEMPTS_REACHED", {
            attempts: nextAttempts,
            error: errMsg
          });

          await redis.xack(STREAM_KEY, GROUP, streamId);
          continue;
        }

        const backoffSeconds = calcBackoffSeconds(nextAttempts);
        const nextRunAt = new Date(Date.now() + backoffSeconds * 1000).toISOString();

        markRetryStmt.run(nextAttempts, nextRunAt, nowIso(), eventId);

        insertLog(eventId, "warn", "RETRY_SCHEDULED", {
          attempts: nextAttempts,
          next_run_at: nextRunAt,
          backoff_seconds: backoffSeconds,
          error: errMsg
        });

        scheduleRequeue(eventId, backoffSeconds * 1000);

        await redis.xack(STREAM_KEY, GROUP, streamId);
      }
    }
  }
}

startWorker().catch((err) => {
  logger.error({ err: err?.message }, "worker crashed on startup");
  process.exit(1);
});