const Database = require("better-sqlite3");
require("dotenv").config();

const db = new Database(process.env.SQLITE_PATH || "./data.db");
db.pragma("journal_mode = WAL");

function nowIso() {
  return new Date().toISOString();
}

module.exports = { db, nowIso };