/**
 * mongodbManager.js  —  Optional MongoDB persistence layer for MQTT Monitor
 *
 * SAFE TO DELETE: Server.js loads this file with a try/catch.
 * If this file is absent or MongoDB is disabled, everything continues
 * working exactly as before (JSON-file storage).
 *
 * What it stores:
 *   • mqtt_messages  — every parsed tag payload received (the "tagData")
 *   • alarm_events   — alarm lifecycle events (created / resolved / ack'd)
 *   • status_changes — link-up / link-down transitions
 *
 * Multiple accounts: Save as many connection profiles as you like.
 * Switch the active profile from the UI without restarting the server.
 */

'use strict';

const fs   = require('fs');
const path = require('path');

// ─── Config file for DB profiles ─────────────────────────────────────────────
const DB_CFG_FILE = path.join(__dirname, 'data', 'db_config.json');

const DEFAULT_DB_CONFIG = {
  enabled: false,           // master on/off switch
  activeProfile: null,      // id of the profile currently in use
  profiles: [
    // example — replace with your real credentials
    {
      id: 'profile-default',
      name: 'Local MongoDB',
      uri: 'mongodb://localhost:27017',
      database: 'mqtt_monitor',
      description: 'Local development instance',
      createdAt: new Date().toISOString(),
    },
  ],
  // Retention: how many days to keep raw messages (0 = keep forever)
  retentionDays: 30,
  // How many messages per topic to return on history queries
  defaultLimit: 500,
};

// ─── State ────────────────────────────────────────────────────────────────────
let _mongoClient   = null;   // active MongoClient
let _db            = null;   // active Db handle
let _dbConfig      = null;   // loaded config
let _connected     = false;
let _MongoClient   = null;   // lazy-loaded constructor

// ─── Helpers ─────────────────────────────────────────────────────────────────
function loadConfig() {
  try {
    if (fs.existsSync(DB_CFG_FILE)) {
      return JSON.parse(fs.readFileSync(DB_CFG_FILE, 'utf8'));
    }
  } catch (_) {}
  return { ...DEFAULT_DB_CONFIG };
}

function saveConfig(cfg) {
  try {
    fs.writeFileSync(DB_CFG_FILE, JSON.stringify(cfg, null, 2));
  } catch (e) {
    console.error('[MongoDB] Cannot save db_config.json:', e.message);
  }
}

function getMongoClient() {
  if (_MongoClient) return _MongoClient;
  try {
    _MongoClient = require('mongodb').MongoClient;
  } catch (_) {
    throw new Error('mongodb package not installed. Run: npm install mongodb');
  }
  return _MongoClient;
}

// ─── Connection management ────────────────────────────────────────────────────

/**
 * Connect to MongoDB using the active profile.
 * Returns { ok: true } or { ok: false, error: string }.
 */
async function connect(cfg) {
  cfg = cfg || _dbConfig || loadConfig();
  _dbConfig = cfg;

  if (!cfg.enabled) return { ok: false, error: 'MongoDB disabled in config' };

  const profile = (cfg.profiles || []).find(p => p.id === cfg.activeProfile);
  if (!profile) return { ok: false, error: 'No active profile selected' };

  // Disconnect existing client first
  await disconnect();

  try {
    const MongoClient = getMongoClient();
    const client = new MongoClient(profile.uri, {
      serverSelectionTimeoutMS: 5000,
      connectTimeoutMS: 5000,
    });
    await client.connect();

    _mongoClient = client;
    _db          = client.db(profile.database || 'mqtt_monitor');
    _connected   = true;

    console.log(`[MongoDB] Connected → ${profile.name} (${profile.uri}) / ${profile.database}`);

    // Ensure indexes
    await ensureIndexes();

    return { ok: true, profile: profile.name };
  } catch (err) {
    _connected = false;
    console.error('[MongoDB] Connection failed:', err.message);
    return { ok: false, error: err.message };
  }
}

async function disconnect() {
  if (_mongoClient) {
    try { await _mongoClient.close(); } catch (_) {}
    _mongoClient = null;
    _db          = null;
    _connected   = false;
    console.log('[MongoDB] Disconnected');
  }
}

/** Create useful indexes on first connect */
async function ensureIndexes() {
  if (!_db) return;
  try {
    // Force MongoDB to create the database by creating a metadata doc
    // (MongoDB only shows a database in Compass after the first write)
    const meta = _db.collection('_meta');
    await meta.updateOne(
      { _id: 'init' },
      { $setOnInsert: { createdAt: new Date(), app: 'mqtt_monitor' } },
      { upsert: true }
    );

    // mqtt_messages: query by site, topic, timestamp
    await _db.collection('mqtt_messages').createIndex({ siteId: 1, topicPath: 1, ts: -1 });

    // TTL index — only add if retentionDays > 0 (0 = keep forever)
    const retentionDays = _dbConfig?.retentionDays ?? 30;
    if (retentionDays > 0) {
      await _db.collection('mqtt_messages').createIndex(
        { ts: 1 },
        { expireAfterSeconds: retentionDays * 86400, background: true }
      );
      console.log(`[MongoDB] TTL index set: messages expire after ${retentionDays} days`);
    } else {
      await _db.collection('mqtt_messages').createIndex({ ts: 1 });
      console.log('[MongoDB] TTL index not set — messages kept forever');
    }

    // alarm_events: query by site, key, timestamps
    await _db.collection('alarm_events').createIndex({ siteId: 1, alarmStart: -1 });
    await _db.collection('alarm_events').createIndex({ key: 1 });

    // status_changes
    await _db.collection('status_changes').createIndex({ key: 1, ts: -1 });

    console.log('[MongoDB] Indexes ensured. Database visible in Compass now.');
  } catch (e) {
    console.warn('[MongoDB] Index creation warning:', e.message);
  }
}

// ─── Write helpers ─────────────────────────────────────────────────────────────

/**
 * Store an MQTT tag data snapshot.
 * Called by Server.js on every 'tagDataChange' event.
 *
 * @param {string} siteId
 * @param {string} siteName
 * @param {string} topicPath
 * @param {string} topicLabel
 * @param {object} tags  — { tagName: value, ... }
 */
async function storeMqttMessage(siteId, siteName, topicPath, topicLabel, tags) {
  if (!_connected || !_db) return;
  try {
    await _db.collection('mqtt_messages').insertOne({
      siteId, siteName, topicPath, topicLabel, tags,
      ts: new Date(),
    });
  } catch (e) {
    // Silent fail — don't disrupt real-time data flow
    console.error('[MongoDB] storeMqttMessage error:', e.message);
  }
}

/**
 * Store an alarm lifecycle event.
 * Call this after createAlarm / resolveAlarm / ack.
 *
 * @param {object} alarm  — the full alarm object
 * @param {string} event  — 'created' | 'resolved' | 'acknowledged'
 */
async function storeAlarmEvent(alarm, event) {
  if (!_connected || !_db) return;
  try {
    await _db.collection('alarm_events').insertOne({
      ...alarm,
      event,
      ts: new Date(),
    });
  } catch (e) {
    console.error('[MongoDB] storeAlarmEvent error:', e.message);
  }
}

/**
 * Store a link status change (linked / unlinked).
 * @param {string} key      — `${siteId}||${topicPath}`
 * @param {string} siteId
 * @param {string} siteName
 * @param {string} topicPath
 * @param {boolean} linked
 */
async function storeStatusChange(key, siteId, siteName, topicPath, linked) {
  if (!_connected || !_db) return;
  try {
    await _db.collection('status_changes').insertOne({
      key, siteId, siteName, topicPath,
      linked, ts: new Date(),
    });
  } catch (e) {
    console.error('[MongoDB] storeStatusChange error:', e.message);
  }
}

// ─── Query helpers ─────────────────────────────────────────────────────────────

/**
 * Get historical MQTT messages for a topic.
 * @param {string} siteId
 * @param {string} topicPath
 * @param {object} opts  — { limit, from, to, tags }
 */
async function getMessages(siteId, topicPath, opts = {}) {
  if (!_connected || !_db) return [];
  try {
    const filter = { siteId, topicPath };
    if (opts.from || opts.to) {
      filter.ts = {};
      if (opts.from) filter.ts.$gte = new Date(opts.from);
      if (opts.to)   filter.ts.$lte = new Date(opts.to);
    }
    const limit = opts.limit || _dbConfig?.defaultLimit || 500;
    return await _db.collection('mqtt_messages')
      .find(filter).sort({ ts: -1 }).limit(limit).toArray();
  } catch (e) {
    console.error('[MongoDB] getMessages error:', e.message);
    return [];
  }
}

/**
 * Get alarm events, optionally filtered.
 */
async function getAlarmEvents(opts = {}) {
  if (!_connected || !_db) return [];
  try {
    const filter = {};
    if (opts.siteId) filter.siteId = opts.siteId;
    if (opts.event)  filter.event  = opts.event;
    if (opts.from || opts.to) {
      filter.ts = {};
      if (opts.from) filter.ts.$gte = new Date(opts.from);
      if (opts.to)   filter.ts.$lte = new Date(opts.to);
    }
    const limit = opts.limit || 1000;
    return await _db.collection('alarm_events')
      .find(filter).sort({ ts: -1 }).limit(limit).toArray();
  } catch (e) {
    console.error('[MongoDB] getAlarmEvents error:', e.message);
    return [];
  }
}

/**
 * Get status change history for a topic.
 */
async function getStatusHistory(key, opts = {}) {
  if (!_connected || !_db) return [];
  try {
    const filter = { key };
    if (opts.from || opts.to) {
      filter.ts = {};
      if (opts.from) filter.ts.$gte = new Date(opts.from);
      if (opts.to)   filter.ts.$lte = new Date(opts.to);
    }
    return await _db.collection('status_changes')
      .find(filter).sort({ ts: -1 }).limit(opts.limit || 200).toArray();
  } catch (e) {
    console.error('[MongoDB] getStatusHistory error:', e.message);
    return [];
  }
}

// ─── Profile management ───────────────────────────────────────────────────────

function getConfig() {
  if (!_dbConfig) _dbConfig = loadConfig();
  return _dbConfig;
}

/**
 * Update the full DB config and optionally reconnect.
 */
async function updateConfig(newCfg) {
  _dbConfig = { ...(getConfig()), ...newCfg };
  saveConfig(_dbConfig);

  // Reconnect only if enabled and active profile changed or just enabled
  if (_dbConfig.enabled) {
    return await connect(_dbConfig);
  } else {
    await disconnect();
    return { ok: true, message: 'MongoDB disabled' };
  }
}

/**
 * Add or update a connection profile.
 */
async function upsertProfile(profile) {
  const cfg = getConfig();
  const idx = cfg.profiles.findIndex(p => p.id === profile.id);
  if (idx >= 0) {
    cfg.profiles[idx] = { ...cfg.profiles[idx], ...profile };
  } else {
    const { v4: uuid } = require('uuid');
    cfg.profiles.push({ id: uuid(), createdAt: new Date().toISOString(), ...profile });
  }
  _dbConfig = cfg;
  saveConfig(cfg);
  return cfg.profiles;
}

/**
 * Delete a connection profile.
 */
async function deleteProfile(profileId) {
  const cfg = getConfig();
  cfg.profiles = cfg.profiles.filter(p => p.id !== profileId);
  if (cfg.activeProfile === profileId) cfg.activeProfile = null;
  _dbConfig = cfg;
  saveConfig(cfg);
  return cfg.profiles;
}

/**
 * Switch the active profile (reconnects automatically if enabled).
 */
async function switchProfile(profileId) {
  const cfg = getConfig();
  const profile = cfg.profiles.find(p => p.id === profileId);
  if (!profile) return { ok: false, error: 'Profile not found' };
  cfg.activeProfile = profileId;
  _dbConfig = cfg;
  saveConfig(cfg);
  if (cfg.enabled) {
    return await connect(cfg);
  }
  return { ok: true, message: 'Profile selected (MongoDB is disabled)' };
}

/**
 * Test a connection without making it active.
 */
async function testConnection(profile) {
  let tempClient = null;
  try {
    const MongoClient = getMongoClient();
    tempClient = new MongoClient(profile.uri, {
      serverSelectionTimeoutMS: 5000,
      connectTimeoutMS: 5000,
    });
    await tempClient.connect();
    const dbHandle = tempClient.db(profile.database || 'mqtt_monitor');
    await dbHandle.command({ ping: 1 });
    return { ok: true, message: `Connected to ${profile.name || profile.uri}` };
  } catch (err) {
    return { ok: false, error: err.message };
  } finally {
    if (tempClient) try { await tempClient.close(); } catch (_) {}
  }
}

/** Get quick collection stats for the dashboard */
async function getStats() {
  if (!_connected || !_db) return null;
  try {
    const [messages, alarms, statuses] = await Promise.all([
      _db.collection('mqtt_messages').estimatedDocumentCount(),
      _db.collection('alarm_events').estimatedDocumentCount(),
      _db.collection('status_changes').estimatedDocumentCount(),
    ]);
    return { messages, alarms, statuses, connected: true };
  } catch (e) {
    return { connected: false, error: e.message };
  }
}

// ─── Auto-start ───────────────────────────────────────────────────────────────
(async () => {
  _dbConfig = loadConfig();
  if (_dbConfig.enabled && _dbConfig.activeProfile) {
    await connect(_dbConfig);
  }
})();

// ─── Exports ──────────────────────────────────────────────────────────────────
module.exports = {
  // Connection
  connect,
  disconnect,
  getStats,
  isConnected: () => _connected,

  // Write
  storeMqttMessage,
  storeAlarmEvent,
  storeStatusChange,

  // Read / Query
  getMessages,
  getAlarmEvents,
  getStatusHistory,

  // Profile / Config management
  getConfig,
  updateConfig,
  upsertProfile,
  deleteProfile,
  switchProfile,
  testConnection,
};