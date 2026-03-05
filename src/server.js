const express = require("express");
const pino = require("pino");
require("dotenv").config();

const { db, nowIso } = require("./db");
const { redis, ensureConsumerGroup } = require("./redis");
const { pollEvents } = require("./ifood");
const { normalizeIfoodEvent } = require("./ifoodAdapter");

const logger = pino({ level: process.env.LOG_LEVEL || "info" });

const STREAM_KEY = process.env.STREAM_KEY || "integration_events_stream";
const CONSUMER_GROUP = process.env.CONSUMER_GROUP || "ifood_workers";
const PORT = Number(process.env.PORT || 3000);

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

async function enqueueEvent(eventId) {
  await redis.xadd(STREAM_KEY, "*", "eventId", String(eventId));
}

async function bootstrap() {
  await ensureConsumerGroup(STREAM_KEY, CONSUMER_GROUP);

  const app = express();
  app.use(express.json());

  app.get("/health", async (req, res) => {
    try {
      const redisPing = await redis.ping();
      const dbTest = db.prepare("SELECT 1 as ok").get();

      return res.json({
        status: "ok",
        redis: redisPing,
        sqlite: dbTest.ok
      });
    } catch (err) {
      return res.status(500).json({
        status: "error",
        error: err?.message || String(err)
      });
    }
  });

  app.get("/events", (req, res) => {
    const events = db.prepare(`
      SELECT *
      FROM integration_events
      ORDER BY id DESC
      LIMIT 100
    `).all();

    return res.json(events);
  });

  app.get("/events/:id/logs", (req, res) => {
    const id = Number(req.params.id);

    if (!Number.isFinite(id)) {
      return res.status(400).json({ error: "invalid id" });
    }

    const logs = db.prepare(`
      SELECT *
      FROM integration_event_logs
      WHERE event_id = ?
      ORDER BY id ASC
    `).all(id);

    return res.json(logs);
  });

  app.get("/events/:id", (req, res) => {
    const id = Number(req.params.id);

    if (!Number.isFinite(id)) {
      return res.status(400).json({ error: "invalid id" });
    }

    const event = db.prepare(`
      SELECT *
      FROM integration_events
      WHERE id = ?
    `).get(id);

    if (!event) {
      return res.status(404).json({ error: "event not found" });
    }

    const logs = db.prepare(`
      SELECT *
      FROM integration_event_logs
      WHERE event_id = ?
      ORDER BY id ASC
    `).all(id);

    return res.json({ event, logs });
  });

  app.post("/events/:id/reprocess", async (req, res) => {
    const id = Number(req.params.id);

    if (!Number.isFinite(id)) {
      return res.status(400).json({ error: "invalid id" });
    }

    const event = db.prepare(`
      SELECT *
      FROM integration_events
      WHERE id = ?
    `).get(id);

    if (!event) {
      return res.status(404).json({ error: "event not found" });
    }

    db.prepare(`
      UPDATE integration_events
      SET status = 'queued',
          next_run_at = NULL,
          updated_at = ?
      WHERE id = ?
    `).run(nowIso(), id);

    insertLog(id, "warn", "MANUAL_REPROCESS");

    await enqueueEvent(id);

    return res.json({
      requeued: true,
      eventId: id
    });
  });

  app.post("/ifood/poll", async (req, res) => {
    try {
      const rawEvents = await pollEvents();

      let inserted = 0;
      let skipped = 0;

      for (const raw of rawEvents) {
        const canonical = normalizeIfoodEvent(raw);
        const createdAt = nowIso();

        try {
          const result = db.prepare(`
            INSERT INTO integration_events
            (
              store_id,
              marketplace,
              direction,
              topic,
              action,
              entity_type,
              entity_id,
              idempotency_key,
              payload_json,
              status,
              attempts,
              created_at,
              updated_at
            )
            VALUES
            (
              @store_id,
              @marketplace,
              @direction,
              @topic,
              @action,
              @entity_type,
              @entity_id,
              @idempotency_key,
              @payload_json,
              'queued',
              0,
              @created_at,
              @updated_at
            )
          `).run({
            ...canonical,
            created_at: createdAt,
            updated_at: createdAt
          });

          const eventId = Number(result.lastInsertRowid);

          insertLog(eventId, "info", "IFOOD_EVENT_RECEIVED", raw);

          await enqueueEvent(eventId);

          inserted++;
        } catch (err) {
          const msg = err?.message || String(err);

          if (msg.includes("UNIQUE")) {
            skipped++;
            continue;
          }

          throw err;
        }
      }

      return res.json({
        polled: rawEvents.length,
        inserted,
        skipped
      });
    } catch (err) {
      logger.error(err);
      return res.status(500).json({
        error: err?.message || String(err)
      });
    }
  });

  app.listen(PORT, () => {
    logger.info(`server running on port ${PORT}`);
  });
}

bootstrap().catch((err) => {
  logger.error({ err: err?.message || String(err) }, "server crashed on startup");
  process.exit(1);
});