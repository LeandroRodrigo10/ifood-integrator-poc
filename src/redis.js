const Redis = require("ioredis");
require("dotenv").config();

const redis = new Redis(process.env.REDIS_URL);

async function ensureConsumerGroup(streamKey, groupName) {
  try {
    await redis.xgroup("CREATE", streamKey, groupName, "$", "MKSTREAM");
  } catch (err) {
    if (!String(err.message || "").includes("BUSYGROUP")) throw err;
  }
}

module.exports = { redis, ensureConsumerGroup };