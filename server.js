const express   = require('express');
const http      = require('http');
const WebSocket = require('ws');
const path      = require('path');
const fs        = require('fs');
const { v4: uuid } = require('uuid');
const manager   = require('./mqttManager');
const email     = require('./emailnotifier');

// ── Optional MongoDB module (safe — server works fine without it) ──────────────
let db = null;
try {
  db = require('./mongodbManager');
  console.log('[MongoDB] mongodbManager.js loaded');
} catch (_) {
  console.log('[MongoDB] mongodbManager.js not found — using JSON files only');
}

const DATA_DIR       = process.env.DATA_DIR || path.join(__dirname, 'data');
const CFG_FILE       = path.join(DATA_DIR, 'config.json');
const SITES_FILE     = path.join(DATA_DIR, 'sites.json');
const ALARMS_FILE    = path.join(DATA_DIR, 'alarms.json');
const TEMPLATES_FILE = path.join(DATA_DIR, 'templates.json');

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const DEFAULT_CFG = {
  host: '', port: 8883, protocol: 'mqtts',
  clientIdPrefix: 'mqtt-monitor', username: '', password: '',
  rejectUnauthorized: false,
  email: {
    enabled:   false,
    smtp: {
      host:   '',
      port:   587,
      secure: false,
      user:   '',
      pass:   '',
    },
    from:      '',
    to:        [],
    onAlarm:   true,
    onResolve: true,
    onAck:     false,
  },
};

const load = (file, def) => {
  try { if (fs.existsSync(file)) return JSON.parse(fs.readFileSync(file, 'utf8')); } catch (_) {}
  return def;
};
const save = (file, data) => fs.writeFileSync(file, JSON.stringify(data, null, 2));

let globalConfig = { ...DEFAULT_CFG, ...load(CFG_FILE, {}) };
if (!globalConfig.email) globalConfig.email = { ...DEFAULT_CFG.email };

let sites     = load(SITES_FILE,    []);
let alarms    = load(ALARMS_FILE,   []);
let templates = load(TEMPLATES_FILE, {});

// ── Boot email notifier ────────────────────────────────────────────────────────
email.configure(globalConfig.email);

// ── Alarm helpers ──────────────────────────────────────────────────────────────
function getActiveAlarm(key) {
  return alarms.find(a => a.key === key && a.alarmEnd === null) || null;
}
function getSiteTopicByKey(key) {
  const [siteId, topicPath] = key.split('||');
  const site = sites.find(s => s.id === siteId);
  if (!site) return null;
  return { site, topic: (site.topics || []).find(t => t.topic === topicPath) };
}

function createAlarm(key) {
  if (getActiveAlarm(key)) return;
  const info = getSiteTopicByKey(key);
  if (!info) return;
  const { site, topic } = info;
  const alarm = {
    id: uuid(), key, siteId: site.id, siteName: site.name,
    topicLabel: topic ? (topic.label || topic.topic) : key,
    topicPath:  topic ? topic.topic : key,
    alarmStart: Date.now(), alarmEnd: null, duration: null,
    alarmText: 'No data received — link down',
    acknowledged: false, ackName: '', ackTime: null, ackNote: '', statusNote: '',
    history: [],
  };
  alarms.push(alarm);
  save(ALARMS_FILE, alarms);
  console.log(`[ALARM] ▲ ${site.name} / ${topic ? (topic.label || topic.topic) : key}`);

  // Store to MongoDB
  db?.storeAlarmEvent(alarm, 'created');

  // Email
  const siteRecipients = globalConfig.email?.siteEmails?.[site.id];
  email.notifyAlarm(alarm, siteRecipients).catch(err => console.error('[Email] notifyAlarm error:', err.message));
}

function resolveAlarm(key) {
  const a = getActiveAlarm(key);
  if (!a) return;
  a.alarmEnd   = Date.now();
  a.duration   = Math.floor((a.alarmEnd - a.alarmStart) / 1000);
  a.alarmText  = 'Link restored — data receiving normally';
  a.statusNote = 'Auto-resolved: data resumed';
  save(ALARMS_FILE, alarms);
  console.log(`[ALARM] ▼ ${a.siteName} / ${a.topicLabel} (${a.duration}s)`);

  // Store to MongoDB
  db?.storeAlarmEvent(a, 'resolved');

  // Email
  const resolveRecipients = globalConfig.email?.siteEmails?.[a.siteId];
  email.notifyResolve(a, resolveRecipients).catch(err => console.error('[Email] notifyResolve error:', err.message));
}

// ── Express / WebSocket setup ──────────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const wss = new WebSocket.Server({ server });
const broadcast = p => {
  const m = JSON.stringify(p);
  for (const c of wss.clients) if (c.readyState === WebSocket.OPEN) c.send(m);
};

wss.on('connection', ws => {
  ws.send(JSON.stringify({
    type:      'init',
    sites,
    config:    globalConfig,
    statuses:  manager.getStatus(),
    alarms,
    tagData:   manager.getTagData(),
    templates,
    dbConfig:  db ? db.getConfig() : null,
  }));
});

// ── MQTT event listeners ───────────────────────────────────────────────────────
let prevStatuses = {};

manager.on('statusChange', () => {
  const cur = manager.getStatus();

  for (const key of Object.keys(cur)) {
    const p = prevStatuses[key], c = cur[key];
    if (!p || p.waiting || c.waiting || c.startedAt === null) continue;

    if  (p.linked && !c.linked) createAlarm(key);
    if (!p.linked &&  c.linked) resolveAlarm(key);

    // Store status change to MongoDB
    if (p.linked !== c.linked) {
      const info = getSiteTopicByKey(key);
      db?.storeStatusChange(
        key,
        info?.site?.id   || key.split('||')[0],
        info?.site?.name || key.split('||')[0],
        info?.topic?.topic || key.split('||')[1],
        c.linked
      );
    }
  }

  prevStatuses = { ...cur };
  broadcast({ type: 'status', data: cur });
  broadcast({ type: 'alarms', data: alarms });
});

manager.on('tagDataChange', () => {
  broadcast({ type: 'tagData', data: manager.getTagData() });

  // Store snapshots to MongoDB
  if (db?.isConnected()) {
    const tagMap = manager.getTagData();
    for (const [key, topicData] of Object.entries(tagMap)) {
      const [siteId, topicPath] = key.split('||');
      const site  = sites.find(s => s.id === siteId) || {};
      const topic = (site.topics || []).find(t => t.topic === topicPath) || {};
      const snap  = (topicData.snapshots || []).slice(-1)[0];
      if (snap) {
        db.storeMqttMessage(
          siteId, site.name || siteId, topicPath,
          topic.label || topicPath, snap.tags || snap
        );
      }
    }
  }
});

setInterval(() => {
  for (const c of wss.clients) if (c.readyState === WebSocket.OPEN) c.ping();
}, 30_000);

// ══════════════════════════════════════════════════════════════════════════════
// API ROUTES
// ══════════════════════════════════════════════════════════════════════════════

// ── Sites ──────────────────────────────────────────────────────────────────────
app.get('/api/sites', (_, res) => res.json(sites));
app.post('/api/sites', (req, res) => {
  const s = { id: uuid(), createdAt: new Date().toISOString(), ...req.body };
  sites.push(s); save(SITES_FILE, sites);
  if (globalConfig.host) manager.connectSite(s, globalConfig);
  broadcast({ type: 'sites_updated', sites }); res.status(201).json(s);
});
app.put('/api/sites/:id', (req, res) => {
  const i = sites.findIndex(s => s.id === req.params.id);
  if (i === -1) return res.status(404).json({ error: 'Not found' });
  sites[i] = { ...sites[i], ...req.body, id: req.params.id }; save(SITES_FILE, sites);
  manager.disconnectSite(req.params.id);
  if (globalConfig.host) manager.connectSite(sites[i], globalConfig);
  broadcast({ type: 'sites_updated', sites }); res.json(sites[i]);
});
app.delete('/api/sites/:id', (req, res) => {
  if (!sites.find(s => s.id === req.params.id)) return res.status(404).json({ error: 'Not found' });
  manager.disconnectSite(req.params.id);
  sites = sites.filter(s => s.id !== req.params.id); save(SITES_FILE, sites);
  broadcast({ type: 'sites_updated', sites }); res.json({ ok: true });
});

// ── Config ─────────────────────────────────────────────────────────────────────
app.get('/api/config', (_, res) => res.json(globalConfig));
app.put('/api/config', (req, res) => {
  globalConfig = { ...DEFAULT_CFG, ...req.body };
  if (!globalConfig.email) globalConfig.email = { ...DEFAULT_CFG.email };
  save(CFG_FILE, globalConfig);
  email.configure(globalConfig.email);
  manager.disconnectAll();
  if (globalConfig.host) sites.forEach(s => manager.connectSite(s, globalConfig));
  broadcast({ type: 'config_updated', config: globalConfig });
  broadcast({ type: 'sites_updated', sites }); res.json(globalConfig);
});

// ── Email config ───────────────────────────────────────────────────────────────
app.get('/api/email-config', (_, res) => res.json(globalConfig.email || DEFAULT_CFG.email));
app.put('/api/email-config', (req, res) => {
  globalConfig.email = { ...DEFAULT_CFG.email, ...req.body };
  save(CFG_FILE, globalConfig);
  email.configure(globalConfig.email);
  broadcast({ type: 'config_updated', config: globalConfig });
  res.json(globalConfig.email);
});
app.post('/api/email-config/test', async (req, res) => {
  try {
    const { configure: cfgFn, notifyAlarm } = require('./emailnotifier');
    const testAlarm = {
      siteName:   'Test Site',
      topicLabel: 'Test/Topic',
      topicPath:  'test/topic',
      alarmStart: Date.now(),
      alarmText:  'This is a test notification from MQTT Monitor',
    };
    cfgFn(globalConfig.email);
    await notifyAlarm(testAlarm);
    res.json({ ok: true, message: 'Test email sent — check your inbox' });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ── Alarms ─────────────────────────────────────────────────────────────────────
app.get('/api/alarms', (_, res) => res.json(alarms));
app.put('/api/alarms/:id', (req, res) => {
  const a = alarms.find(a => a.id === req.params.id);
  if (!a) return res.status(404).json({ error: 'Not found' });
  const { ackName, ackNote, statusNote } = req.body;
  a.history.push({
    time: Date.now(), who: ackName || a.ackName || 'Unknown',
    note: ackNote || '', statusNote: statusNote || '',
    action: a.acknowledged ? 'update' : 'acknowledge',
  });
  a.acknowledged = true;
  a.ackName      = ackName    || a.ackName;
  a.ackTime      = a.ackTime  || Date.now();
  a.ackNote      = ackNote    || a.ackNote;
  a.statusNote   = statusNote || a.statusNote;
  save(ALARMS_FILE, alarms);
  broadcast({ type: 'alarms', data: alarms });

  // Store to MongoDB
  db?.storeAlarmEvent(a, 'acknowledged');

  // Email
  const ackRecipients = globalConfig.email?.siteEmails?.[a.siteId];
  email.notifyAck(a, ackRecipients).catch(err => console.error('[Email] notifyAck error:', err.message));

  res.json(a);
});
app.delete('/api/alarms/:id', (req, res) => {
  alarms = alarms.filter(a => a.id !== req.params.id);
  save(ALARMS_FILE, alarms); broadcast({ type: 'alarms', data: alarms }); res.json({ ok: true });
});
app.delete('/api/alarms', (_, res) => {
  alarms = alarms.filter(a => !(a.alarmEnd !== null && a.acknowledged));
  save(ALARMS_FILE, alarms); broadcast({ type: 'alarms', data: alarms }); res.json({ ok: true });
});

// ── Templates ──────────────────────────────────────────────────────────────────
app.get('/api/templates', (_, res) => res.json(templates));
app.post('/api/templates/:key', (req, res) => {
  const key = decodeURIComponent(req.params.key);
  templates[key] = req.body;
  save(TEMPLATES_FILE, templates);
  broadcast({ type: 'templates', data: templates });
  res.json(templates[key]);
});
app.delete('/api/templates/:key', (req, res) => {
  const key = decodeURIComponent(req.params.key);
  delete templates[key];
  save(TEMPLATES_FILE, templates);
  broadcast({ type: 'templates', data: templates });
  res.json({ ok: true });
});

app.get('/api/health', (_, res) =>
  res.json({ ok: true, sites: sites.length, time: new Date().toISOString() }));

// ══════════════════════════════════════════════════════════════════════════════
// MONGODB API ROUTES  (/api/db/...)
// All routes are safe — if mongodbManager.js is missing, db is null → 503
// ══════════════════════════════════════════════════════════════════════════════

// GET /api/db/config — full DB config (profiles, active profile, enabled flag)
app.get('/api/db/config', (_, res) => {
  if (!db) return res.status(503).json({ error: 'MongoDB module not loaded', moduleLoaded: false });
  res.json({ ...db.getConfig(), moduleLoaded: true });
});

// PUT /api/db/config — update enabled / retentionDays / activeProfile
app.put('/api/db/config', async (req, res) => {
  if (!db) return res.status(503).json({ error: 'MongoDB module not loaded' });
  const result = await db.updateConfig(req.body);
  broadcast({ type: 'db_config', data: db.getConfig() });
  res.json({ ...result, config: db.getConfig() });
});

// GET /api/db/stats — collection document counts + live connection status
app.get('/api/db/stats', async (_, res) => {
  if (!db) return res.json({ connected: false, moduleLoaded: false });
  const stats = await db.getStats();
  res.json({ ...stats, moduleLoaded: true });
});

// GET /api/db/profiles — list all saved connection profiles
app.get('/api/db/profiles', (_, res) => {
  if (!db) return res.status(503).json({ error: 'MongoDB module not loaded' });
  res.json(db.getConfig().profiles || []);
});

// POST /api/db/profiles — add or update a profile
app.post('/api/db/profiles', async (req, res) => {
  if (!db) return res.status(503).json({ error: 'MongoDB module not loaded' });
  const profiles = await db.upsertProfile(req.body);
  broadcast({ type: 'db_config', data: db.getConfig() });
  res.json(profiles);
});

// DELETE /api/db/profiles/:id — remove a profile
app.delete('/api/db/profiles/:id', async (req, res) => {
  if (!db) return res.status(503).json({ error: 'MongoDB module not loaded' });
  const profiles = await db.deleteProfile(req.params.id);
  broadcast({ type: 'db_config', data: db.getConfig() });
  res.json(profiles);
});

// POST /api/db/profiles/:id/activate — switch the active profile (reconnects)
app.post('/api/db/profiles/:id/activate', async (req, res) => {
  if (!db) return res.status(503).json({ error: 'MongoDB module not loaded' });
  const result = await db.switchProfile(req.params.id);
  broadcast({ type: 'db_config', data: db.getConfig() });
  res.json(result);
});

// POST /api/db/connect — connect using the currently active profile
app.post('/api/db/connect', async (_, res) => {
  if (!db) return res.status(503).json({ error: 'MongoDB module not loaded' });
  const result = await db.connect();
  broadcast({ type: 'db_config', data: db.getConfig() });
  res.json(result);
});

// POST /api/db/disconnect — disconnect from MongoDB
app.post('/api/db/disconnect', async (_, res) => {
  if (!db) return res.status(503).json({ error: 'MongoDB module not loaded' });
  await db.disconnect();
  broadcast({ type: 'db_config', data: db.getConfig() });
  res.json({ ok: true, message: 'Disconnected from MongoDB' });
});

// POST /api/db/test — test a connection profile without making it active
app.post('/api/db/test', async (req, res) => {
  if (!db) return res.status(503).json({ error: 'MongoDB module not loaded' });
  const result = await db.testConnection(req.body);
  res.json(result);
});

// GET /api/db/history — query stored MQTT messages
//   ?siteId=...&topicPath=...&from=ISO&to=ISO&limit=500
app.get('/api/db/history', async (req, res) => {
  if (!db) return res.status(503).json({ error: 'MongoDB module not loaded' });
  const { siteId, topicPath, from, to, limit } = req.query;
  if (!siteId || !topicPath) return res.status(400).json({ error: 'siteId and topicPath required' });
  const data = await db.getMessages(siteId, topicPath, { from, to, limit: parseInt(limit) || 500 });
  res.json(data);
});

// GET /api/db/alarm-events — query stored alarm lifecycle events
//   ?siteId=...&event=created|resolved|acknowledged&from=ISO&to=ISO&limit=1000
app.get('/api/db/alarm-events', async (req, res) => {
  if (!db) return res.status(503).json({ error: 'MongoDB module not loaded' });
  const data = await db.getAlarmEvents(req.query);
  res.json(data);
});

// GET /api/db/status-history — query link up/down history for a topic
//   ?key=siteId||topicPath&from=ISO&to=ISO&limit=200
app.get('/api/db/status-history', async (req, res) => {
  if (!db) return res.status(503).json({ error: 'MongoDB module not loaded' });
  const { key, from, to, limit } = req.query;
  if (!key) return res.status(400).json({ error: 'key required' });
  const data = await db.getStatusHistory(key, { from, to, limit: parseInt(limit) || 200 });
  res.json(data);
});

// ══════════════════════════════════════════════════════════════════════════════
// USERS / AUTH
// ══════════════════════════════════════════════════════════════════════════════
const USERS_FILE = path.join(DATA_DIR, 'users.json');

// ── FIX: 'db-settings' added so admin has access to every page ────────────────
const DEFAULT_USERS = [
  {
    id: 'admin-default',
    username: 'admin',
    password: 'admin123',
    role: 'admin',
    displayName: 'Administrator',
    allowedPages: [
      'dashboard',
      'stats',
      'alarms',
      'alarm-trend',
      'sites',
      'mqtt-settings',
      'email-settings',
      'db-settings',          // ← was missing — now included
      'user-management',
    ],
    createdAt: new Date().toISOString(),
  },
];

let users = load(USERS_FILE, DEFAULT_USERS);
if (!users.length) { users = DEFAULT_USERS; save(USERS_FILE, users); }

// ── Migrate existing admin: add db-settings if it is missing ──────────────────
const adminIdx = users.findIndex(u => u.id === 'admin-default');
if (adminIdx !== -1 && !users[adminIdx].allowedPages.includes('db-settings')) {
  users[adminIdx].allowedPages.push('db-settings');
  save(USERS_FILE, users);
  console.log('[Auth] Migrated admin-default: added db-settings permission');
}

// POST /api/auth/login
app.post('/api/auth/login', (req, res) => {
  const { username, password } = req.body || {};
  const user = users.find(u => u.username === username && u.password === password);
  if (!user) return res.status(401).json({ error: 'Invalid username or password' });
  const { password: _, ...safeUser } = user;
  res.json({ ok: true, user: safeUser });
});

// GET /api/users
app.get('/api/users', (_, res) => {
  res.json(users.map(({ password: _, ...u }) => u));
});

// POST /api/users
app.post('/api/users', (req, res) => {
  const { username, password, displayName, role, allowedPages } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'Username and password required' });
  if (users.find(u => u.username === username)) return res.status(409).json({ error: 'Username already exists' });
  const user = {
    id: uuid(),
    username,
    password,
    role: role || 'viewer',
    displayName: displayName || username,
    allowedPages: allowedPages || ['dashboard'],
    createdAt: new Date().toISOString(),
  };
  users.push(user);
  save(USERS_FILE, users);
  const { password: _, ...safeUser } = user;
  broadcast({ type: 'users_updated', users: users.map(({ password: _, ...u }) => u) });
  res.status(201).json(safeUser);
});

// PUT /api/users/:id
app.put('/api/users/:id', (req, res) => {
  const idx = users.findIndex(u => u.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  const updated = { ...users[idx], ...req.body, id: req.params.id };
  users[idx] = updated;
  save(USERS_FILE, users);
  broadcast({ type: 'users_updated', users: users.map(({ password: _, ...u }) => u) });
  const { password: _, ...safeUser } = updated;
  res.json(safeUser);
});

// DELETE /api/users/:id
app.delete('/api/users/:id', (req, res) => {
  if (req.params.id === 'admin-default') return res.status(403).json({ error: 'Cannot delete default admin' });
  users = users.filter(u => u.id !== req.params.id);
  save(USERS_FILE, users);
  broadcast({ type: 'users_updated', users: users.map(({ password: _, ...u }) => u) });
  res.json({ ok: true });
});

// ── Start server ───────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n  MQTT Site Monitor  →  http://localhost:${PORT}`);
  console.log(`  MongoDB  →  ${db ? (db.isConnected() ? 'Connected' : 'Module loaded, not connected') : 'Not available'}`);
  console.log(`  Sites: ${sites.length}  |  Active alarms: ${alarms.filter(a => !a.alarmEnd).length}\n`);
  prevStatuses = manager.getStatus();
  if (globalConfig.host) sites.forEach(s => manager.connectSite(s, globalConfig));
  manager.start();
});