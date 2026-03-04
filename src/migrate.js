const { db } = require("./db");

db.exec(`
  CREATE TABLE IF NOT EXISTS integration_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    store_id TEXT NOT NULL,
    marketplace TEXT NOT NULL,
    direction TEXT NOT NULL,
    topic TEXT NOT NULL,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_run_at TEXT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE INDEX IF NOT EXISTS idx_integration_events_status
    ON integration_events(status);

  CREATE INDEX IF NOT EXISTS idx_integration_events_next_run_at
    ON integration_events(next_run_at);

  CREATE TABLE IF NOT EXISTS integration_event_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    data_json TEXT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(event_id) REFERENCES integration_events(id)
  );
`);

console.log("Migration OK");