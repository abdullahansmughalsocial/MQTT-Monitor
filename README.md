# MQTT Monitor — Complete Technical Documentation

> **Version:** 6 May 2026  
> **Runtime:** Node.js  
> **Protocol:** MQTT / MQTTS  
> **Author:** Abdullah Ans

---

## Table of Contents

1. [What Is This Tool?](#1-what-is-this-tool)
2. [Features (In Detail)](#2-features-in-detail)
3. [How the Code Hierarchy Is Built](#3-how-the-code-hierarchy-is-built)
4. [IP, Port & Broker Configuration](#4-ip-port--broker-configuration)
5. [Complete Code Architecture — Each Module](#5-complete-code-architecture--each-module)
   - [mqttManager.js](#51-mqttmanagerjs)
   - [mongodbManager.js](#52-mongodbmanagerjs)
   - [Emailnotifier.js](#53-emailnotifierjs)
6. [Data Files Architecture](#6-data-files-architecture)
   - [config.json](#61-configjson)
   - [sites.json](#62-sitesjson)
   - [users.json](#63-usersjson)
   - [alarms.json](#64-alarmsjson)
   - [db_config.json](#65-db_configjson)
   - [templates.json](#66-templatesjson)
7. [MQTT Payload Wire Formats](#7-mqtt-payload-wire-formats)
8. [Alarm Lifecycle](#8-alarm-lifecycle)
9. [User Roles & Page Permissions](#9-user-roles--page-permissions)
10. [MongoDB Collections Schema](#10-mongodb-collections-schema)
11. [Email Notification System](#11-email-notification-system)
12. [How to Run the Project](#12-how-to-run-the-project)
13. [Dependency Map](#13-dependency-map)

---

## 1. What Is This Tool?

**MQTT Monitor** is a real-time industrial IoT monitoring server built in Node.js. It was built to supervise MQTT links between remote industrial sites (factories, cold storages, malls) and a central cloud broker. Each "site" has one or more PLC (Programmable Logic Controller) devices that push sensor/electrical data over MQTT topics.

The tool serves these core purposes:

- **Link health monitoring** — Continuously watches every configured MQTT topic and immediately raises an alarm if data stops arriving within a configurable timeout window.
- **Structured tag-data parsing** — Understands two industrial payload formats (Native and Wago SCADA) and assembles incoming collection messages into coherent snapshots of PLC tag values.
- **Alarm management** — Creates, stores, and resolves alarms automatically. Operators can acknowledge alarms with notes and names, and a full history is kept per alarm.
- **Email alerting** — Sends formatted HTML emails on alarm creation, resolution, and acknowledgement, with per-site recipient overrides.
- **Optional MongoDB persistence** — All tag data, alarm events, and status transitions can be written to MongoDB for historical query and trend analysis, with configurable TTL-based retention.
- **Role-based web UI** — A browser frontend (served by a `server.js` entry point) with login, dashboard, alarm trend charts, site management, MQTT settings, email settings, user management, and template import pages. Access is controlled per-user by an `allowedPages` whitelist.

The tool is currently deployed with **6 real industrial sites** across Lahore, Pakistan — including a shopping mall (Packages Mall), a cotton factory (Suraj Cotton), two cold storages (Friends Cold Storage, Prime Cold Storage), and two rice processing areas (East Rice Area1 and Area2).

---

## 2. Features (In Detail)

### 2.1 Multi-Site MQTT Connection Management

Each site in `sites.json` gets its own dedicated MQTT client connection. The system:

- Creates one `mqtt.Client` per site, keyed by `site.id`.
- Each site subscribes to one or more topics (e.g., `Packages`, `Packages01`).
- A **global MQTT broker config** is used by default (defined in `config.json`), but any individual site can override the broker entirely using `mqttOverride` — meaning different sites can connect to different brokers simultaneously.
- Connections auto-reconnect every 5 seconds if dropped (`reconnectPeriod: 5000`).
- Each client gets a unique `clientId` built from `clientIdPrefix-siteId(first 8 chars)-timestamp` to avoid MQTT broker conflicts.

### 2.2 Per-Topic Link Status Tracking

For every `(siteId, topicPath)` pair a status object is maintained in memory:

| Field | Meaning |
|---|---|
| `linked` | `true` if data arrived within the timeout window |
| `waiting` | `true` from creation until the MQTT TCP connection is established |
| `startedAt` | Timestamp when TCP connection was confirmed |
| `lastSeen` | Timestamp of the most recent MQTT message on this topic |
| `lastMessage` | The raw payload text (truncated to 300 chars) |
| `timeout` | Seconds before declaring a link dead (default: 60s) |
| `label` | Human-readable name for the topic (e.g., "PLC1") |
| `snapshotLimit` | Max number of tag data snapshots to keep in memory |

A background timer fires **every second**, calculates elapsed time since `lastSeen` (or `startedAt` if never seen), and flips `linked` true/false based on whether elapsed time is within the configured timeout. This is how the "link down" condition is detected automatically without needing any broker-side features.

### 2.3 Structured Tag-Data Parsing and Snapshot System

When a message arrives on a topic, the system attempts to JSON-parse it. If it matches either the **Native** or **Wago** structured format, it extracts:

- A **collection ID** (integer, e.g., `1`, `2`, `3` — representing groups of PLC tags)
- A **timestamp** (`Time` field)
- A flat **key-value map** of tag names to sensor values (the `Values` object)

These are accumulated into a **working state** per topic. The key insight is:

> When **Collection ID 1** arrives with a **new timestamp**, the previous working state is considered a completed data cycle. It is frozen as an immutable **snapshot** and pushed to the front of the snapshots array.

This means the snapshots array always holds the last N complete cycles of all PLC tag values, ordered newest first. `snapshotLimit` (per site) controls how many snapshots are kept:

- `0` → unlimited (Infinity)
- `null` or `undefined` → default of 30
- Any positive integer → keep that many

### 2.4 Alarm Engine

The alarm system tracks link failures:

- When a topic transitions from `linked: true` → `linked: false`, a new alarm document is created with `alarmStart` timestamp, written to `alarms.json`, and optionally to MongoDB.
- When a topic transitions back to `linked: true`, the open alarm is **auto-resolved**: `alarmEnd` and `duration` are filled in, and `alarmText` is updated to "Link restored".
- Alarms can be **acknowledged** by operators with a name, timestamp, and optional note. Acknowledgement is appended to the `history` array inside the alarm document.
- The UI exposes separate views for unacknowledged, resolved, and all alarms.
- Bulk actions are available: clear all unacknowledged, clear all resolved.

### 2.5 Dual MQTT Payload Format Support

The parser supports two distinct wire formats coming from PLCs without any reconfiguration:

**Native Format** (custom JSON):
```json
{ "collectionId": 1, "payload": [{ "Time": "2026-05-06T10:00:00Z", "Values": { "TagA": 230.5, "TagB": 48.2 } }] }
```

**Wago Format** (Wago PLC SCADA protocol v1.5.0):
```json
{ "MessageType": "TagValues", "WagoProtocol": "1.5.0", "CollectionId": 1, "TagData": [{ "Time": "...", "Values": {...} }] }
```

Both are normalised internally to the same structure before processing. Plain-text or non-JSON messages are silently skipped — they still update `lastSeen` for link-health purposes.

### 2.6 Template System

For each site/topic combination a **template** can be uploaded (from an `.xlsx` file). Templates define:

- Which **collections** (e.g., Collection ID 1) contain which tag names
- Logical groupings called **areas** (e.g., EM01, EM02, CO2, Temperature)
- Higher-level groupings called **devices** (e.g., Device1 contains EM01 and EM02)

This allows the frontend to display tag data in a structured, labelled layout rather than a raw flat list. The `templates.json` file stores these definitions keyed by `siteId||topicPath`.

### 2.7 MongoDB Optional Persistence

MongoDB integration is completely optional. If the `mongodb` npm package is not installed or if `enabled: false` in `db_config.json`, everything runs purely from JSON files. When enabled, it writes to three collections:

- `mqtt_messages` — every tag-data snapshot
- `alarm_events` — every alarm lifecycle event
- `status_changes` — every link-up/link-down transition

Multiple named **connection profiles** can be saved. The active profile can be switched from the UI without restarting the server. TTL indexes auto-expire old messages after a configurable number of days (default: 30).

### 2.8 Email Notification System

Uses Nodemailer to send rich HTML emails via any SMTP server (Gmail-ready). Three triggers are configurable independently:

| Trigger | `config.json` field | Description |
|---|---|---|
| Link goes DOWN | `onAlarm: true` | Red alarm email with site, topic, start time |
| Link is RESTORED | `onResolve: true` | Green resolution email with downtime duration |
| Alarm acknowledged | `onAck: false` | Amber acknowledgement email with operator name/note |

Recipients can be set globally (`to` array) and overridden per-site (`siteEmails` map).

### 2.9 Role-Based Access Control

Users are stored in `users.json` with a `role` (admin/viewer) and an `allowedPages` array. Every page route in the web UI checks whether the logged-in user has that page in their whitelist before serving it. The admin account has access to all pages; viewer accounts are restricted to read-only views.

### 2.10 Debounced WebSocket Broadcasting

Both status changes and tag-data changes are emitted through the Node.js `EventEmitter` system with an **80 ms debounce**. This prevents flooding the WebSocket connection to the browser when many topics change state simultaneously (e.g., on initial broker connect, all subscriptions succeed in rapid succession).

---

## 3. How the Code Hierarchy Is Built

```
MQTT_Monitor (6 May 2026)/
│
├── server.js                    ← (Entry point — NOT in RAR, runs Express + WebSocket)
│
├── mqttManager.js               ← Core MQTT engine (EventEmitter singleton)
├── mongodbManager.js            ← Optional MongoDB persistence layer (singleton)
├── Emailnotifier.js             ← Email notification module (stateful singleton)
│
├── node_modules/
│   └── .bin/
│       ├── mqtt                 ← MQTT CLI tools (mqtt_pub, mqtt_sub)
│       └── mime                 ← MIME type utility
│
└── data/
    ├── config.json              ← Global MQTT broker config + email SMTP settings
    ├── sites.json               ← Site registry (sites, topics, overrides)
    ├── users.json               ← User accounts + role + page permissions
    ├── alarms.json              ← Persistent alarm log (all historical alarms)
    ├── db_config.json           ← MongoDB connection profiles + on/off switch
    └── templates.json           ← Tag layout templates per site/topic
```

### Dependency & Call Flow

```
server.js (Express + WebSocket)
    │
    ├── loads sites.json, config.json, users.json, alarms.json, templates.json
    │
    ├── requires mqttManager.js ──────────────────────────────────────────────┐
    │       │                                                                  │
    │       │  mqttManager.connectSite(site, globalConfig)                    │
    │       │      → creates mqtt.Client per site                             │
    │       │      → subscribes to site.topics[]                              │
    │       │      → on 'message': parses payload, updates statuses/tagData   │
    │       │      → _checkTimeouts() every 1s → flips linked flag            │
    │       │      → emits 'statusChange' (debounced 80ms)                   │
    │       │      └── emits 'tagDataChange' (debounced 80ms)                │
    │       │                                                                  │
    │       │  server.js listens for these events → broadcasts via WebSocket  │
    │                                                                          │
    ├── requires mongodbManager.js (try/catch — optional)                    │
    │       │                                                                  │
    │       │  auto-starts on load if enabled + activeProfile set            │
    │       │  server.js calls storeMqttMessage() on tagDataChange           │
    │       │  server.js calls storeAlarmEvent() on alarm create/resolve/ack │
    │       └── server.js calls storeStatusChange() on statusChange          │
    │                                                                          │
    └── requires Emailnotifier.js                                             │
            │                                                                  │
            │  configure(config.email) called on startup + config change     │
            │  server.js calls notifyAlarm() when alarm created              │
            │  server.js calls notifyResolve() when alarm auto-resolved      │
            └── server.js calls notifyAck() when operator acknowledges       │
                                                                               │
Browser (WebSocket client) ←──────────────────────────────────────────────────┘
    receives: { type: 'status', data: mqttManager.getStatus() }
    receives: { type: 'tagData', data: mqttManager.getTagData() }
```

### In-Memory Data Structures (mqttManager.js)

```
MQTTManager {
  clients   Map<siteId, mqtt.Client>

  statuses  Map<"siteId||topicPath", {
                linked, waiting, startedAt, lastSeen,
                lastMessage, timeout, label, snapshotLimit
            }>

  tagData   Map<"siteId||topicPath", {
                working: {
                    col1Time: string|null,
                    collections: {
                        [collectionId]: { tags: { [tagName]: value } }
                    }
                },
                snapshots: [
                    {
                        col1Time: string,
                        receivedAt: timestamp,
                        collections: { ... }   // deep-copied, immutable
                    },
                    ...  // newest first, capped by snapshotLimit
                ]
            }>
}
```

---

## 4. IP, Port & Broker Configuration

### Global MQTT Broker (config.json)

| Setting | Value | Description |
|---|---|---|
| `host` | `b-e6439b20-3a80-4688-86b3-0658535e5f2f-1.mq.ap-south-1.amazonaws.com` | AWS Amazon MQ broker in Asia Pacific (Mumbai) |
| `port` | `8883` | Standard MQTTS (TLS-encrypted MQTT) port |
| `protocol` | `mqtts` | Encrypted MQTT over TLS |
| `username` | `Maria_123` | MQTT broker username |
| `password` | `Maria_1234567` | MQTT broker password |
| `clientIdPrefix` | `Abdullah_Ans` | Prefix for auto-generated client IDs |
| `rejectUnauthorized` | `false` | TLS certificate validation disabled (self-signed cert support) |

**Full client ID format at runtime:**
```
Abdullah_Ans-{first-8-chars-of-siteId}-{unix-timestamp-ms}
```
Example: `Abdullah_Ans-44139f20-1746518400000`

### Per-Site MQTT Override

Any site in `sites.json` can set `mqttOverride` to an object with the same fields as the global config. If `mqttOverride.host` is present, that object replaces the global config for that site's connection. This allows different sites to use completely separate brokers.

```json
"mqttOverride": {
  "host": "broker2.example.com",
  "port": 1883,
  "protocol": "mqtt",
  "username": "site2user",
  "password": "site2pass"
}
```

### Email SMTP (config.json → email.smtp)

| Setting | Value | Description |
|---|---|---|
| `host` | `smtp.gmail.com` | Gmail SMTP server |
| `port` | `587` | STARTTLS port |
| `secure` | `false` | Use STARTTLS (not SSL) |
| `user` | `abdullahansmughal@gmail.com` | Gmail account |
| `pass` | `sdpv ksjc eguq asqi` | Gmail App Password (16-char) |

> **Important:** Gmail requires an App Password, not your regular account password. Generate one at https://myaccount.google.com/apppasswords

### MongoDB Connection (db_config.json)

| Setting | Default Value | Description |
|---|---|---|
| `enabled` | `false` | Master switch — set `true` to activate |
| `uri` | `mongodb://localhost:27017` | MongoDB connection string |
| `database` | `mqtt_monitor` | Database name |
| `retentionDays` | `30` | Days to keep messages (0 = keep forever) |
| `defaultLimit` | `500` | Max documents returned per history query |

To change the MongoDB host/credentials, edit the `uri` field in the active profile inside `db_config.json`, or use the DB Settings page in the UI.

### Web Server Port

The web server port is configured in `server.js` (entry point). It is typically set to `3000` or `8080`. To change it, edit the `PORT` constant or environment variable at the top of `server.js`.

---

## 5. Complete Code Architecture — Each Module

### 5.1 mqttManager.js

**Role:** The heart of the system. Manages all MQTT connections and is the single source of truth for real-time link status and tag data.

**Pattern:** Singleton exported as `module.exports = new MQTTManager()`. All other modules import the same instance.

**Extends:** Node.js `EventEmitter` — so `server.js` can simply do `mqttManager.on('statusChange', handler)`.

#### Class: MQTTManager

##### Internal State

```javascript
this.clients       = new Map()   // siteId → mqtt.Client instance
this.statuses      = new Map()   // "siteId||topic" → StatusObject
this.tagData       = new Map()   // "siteId||topic" → TagDataEntry
this._timer        = null        // setInterval handle for timeout checker
this._debounce     = null        // setTimeout for status emit throttle
this._tagDebounce  = null        // setTimeout for tagData emit throttle
```

##### Public Methods

| Method | Arguments | What It Does |
|---|---|---|
| `start()` | — | Starts the 1-second timeout-check loop. Must be called once after server starts. |
| `stop()` | — | Clears the timer and disconnects all sites. |
| `connectSite(site, globalConfig)` | site object, global config | Disconnects any existing client for this site, creates a new `mqtt.Client`, initialises all topic statuses as "waiting", subscribes to all topics on TCP connect. |
| `disconnectSite(siteId)` | string | Ends the MQTT client for this site and clears its status and tag data from both maps. |
| `disconnectAll()` | — | Calls `disconnectSite` for every connected site. |
| `getStatus()` | — | Returns a plain serialisable object of all topic statuses for WebSocket broadcast. |
| `getTagData()` | — | Returns a plain serialisable object of all working states and snapshots for WebSocket broadcast. Both use `deepCopyCollections` so internal mutable state is not exposed. |

##### Internal Methods

| Method | What It Does |
|---|---|
| `_checkTimeouts()` | Called every 1 second. For every status that is not in "waiting" state, calculates elapsed seconds since `lastSeen` (or `startedAt` if never seen) and compares to `timeout`. If `linked` state needs to flip, sets it and marks `changed = true`. If changed, calls `_emitStatus()`. |
| `_emitStatus()` | Clears any pending debounce timer and sets a new 80ms one. After 80ms, emits `'statusChange'`. This collapses bursts of rapid updates into a single event. |
| `_emitTagData()` | Same debounce pattern for `'tagDataChange'`. |

##### MQTT Client Event Handlers (per site)

| Event | Handler Logic |
|---|---|
| `'connect'` | Sets `startedAt = Date.now()` and `waiting = false` on all topic statuses. Subscribes to each topic. Calls `_emitStatus()`. |
| `'message'` | 1. Looks up the status by `siteId||topic`. 2. Updates `linked=true`, `lastSeen`, `lastMessage`. 3. Tries to JSON-parse the payload. 4. If structured (Native or Wago format), updates the `tagData` map and triggers snapshot logic. 5. Calls `_emitStatus()` unconditionally (keeps frontend `lastSeen` current). |
| `'error'` | Logs the error. No state change (MQTT.js handles reconnection). |
| `'close'` | Logs the disconnect. Reconnection is handled by `reconnectPeriod`. |

##### Snapshot Cycle Logic (inside `'message'` handler)

```
Incoming message on topic T at site S:
    └── Parse JSON
    └── Normalise to { collectionId, payload }
    └── Get or create tagData entry for key "S||T"
    └── IF collectionId === 1 AND payload[0].Time is NEW:
            └── Working state has a new col1Time → previous working state is complete
            └── Deep-copy working.collections → push to snapshots[0] (newest first)
            └── If snapshots.length > snapshotLimit → pop oldest
            └── Reset working to empty
            └── Set working.col1Time = new timestamp
    └── Ensure collections[collectionId] bucket exists in working state
    └── Merge all Values from payload into working.collections[collectionId].tags
    └── Save back to tagData map
    └── Call _emitTagData()
```

##### deepCopyCollections (module-level function)

```javascript
function deepCopyCollections(collections) {
    // Iterates all collection IDs
    // For each collection, creates a new object with a shallow copy of tags {}
    // This ensures snapshots and getTagData() responses are independent
    // of ongoing mutations to the live working state
}
```

---

### 5.2 mongodbManager.js

**Role:** Optional persistence layer. Server.js wraps the `require()` in a try/catch — if this file is deleted or `mongodb` is not installed, everything continues working with JSON-file-only storage.

**Pattern:** Stateful module (not a class). Exports individual functions. Auto-connects on load if configured.

#### Module-Level State

```javascript
let _mongoClient   = null    // Active MongoClient instance
let _db            = null    // Active Db handle
let _dbConfig      = null    // Currently loaded config object
let _connected     = false   // Boolean connection flag
let _MongoClient   = null    // Lazily loaded MongoClient constructor
```

#### Connection Functions

| Function | Signature | Description |
|---|---|---|
| `connect(cfg?)` | async | Reads active profile from config, creates MongoClient, connects, calls `ensureIndexes()`. Returns `{ ok, profile }` or `{ ok, error }`. |
| `disconnect()` | async | Closes MongoClient, nulls all state. |
| `ensureIndexes()` | async (internal) | Creates compound indexes on `mqtt_messages`, `alarm_events`, `status_changes`. Adds TTL index on `mqtt_messages.ts` if `retentionDays > 0`. Also inserts a `_meta` document to force the DB to appear in MongoDB Compass. |

#### Write Functions

| Function | Signature | When Called |
|---|---|---|
| `storeMqttMessage(siteId, siteName, topicPath, topicLabel, tags)` | async | On every `tagDataChange` event from mqttManager |
| `storeAlarmEvent(alarm, event)` | async | When alarm is created (`'created'`), resolved (`'resolved'`), or acknowledged (`'acknowledged'`) |
| `storeStatusChange(key, siteId, siteName, topicPath, linked)` | async | On every link state transition |

All write functions silently log errors and return without throwing — they never interrupt the real-time data flow.

#### Query Functions

| Function | Returns | Filters Available |
|---|---|---|
| `getMessages(siteId, topicPath, opts)` | Array of documents | `limit`, `from` (ISO date), `to` (ISO date) |
| `getAlarmEvents(opts)` | Array of documents | `siteId`, `event`, `from`, `to`, `limit` |
| `getStatusHistory(key, opts)` | Array of documents | `from`, `to`, `limit` |
| `getStats()` | `{ messages, alarms, statuses, connected }` | — |

#### Profile Management Functions

| Function | Description |
|---|---|
| `getConfig()` | Returns current config (loads from file if not cached) |
| `updateConfig(newCfg)` | Merges new config, saves to `db_config.json`, reconnects if enabled |
| `upsertProfile(profile)` | Adds a new profile or updates an existing one (matched by `id`) |
| `deleteProfile(profileId)` | Removes a profile; clears `activeProfile` if it was the active one |
| `switchProfile(profileId)` | Sets a profile as active, reconnects if MongoDB is enabled |
| `testConnection(profile)` | Opens a temporary connection to test credentials without making it active |

#### Auto-Start IIFE

```javascript
(async () => {
    _dbConfig = loadConfig();
    if (_dbConfig.enabled && _dbConfig.activeProfile) {
        await connect(_dbConfig);
    }
})();
```

This runs immediately when the module is `require()`d. If MongoDB was previously enabled and has an active profile, it connects automatically on server start.

---

### 5.3 Emailnotifier.js

**Role:** Sends HTML email notifications when alarm lifecycle events occur.

**Pattern:** Stateful module. Must call `configure(emailCfg)` before sending. Exports three `notify*` functions.

#### Module-Level State

```javascript
let nodemailer     // Lazily required (warns if missing)
let _transporter   // Nodemailer transport instance (null if not configured)
let _emailCfg      // Current email config object
```

#### configure(emailCfg)

Called by `server.js` every time `config.json` is saved. Tears down the old transporter and creates a new one from the current SMTP settings. Does nothing if `nodemailer` is not installed or `emailCfg.enabled` is false.

```javascript
_transporter = nodemailer.createTransport({
    host:   cfg.smtp.host,
    port:   cfg.smtp.port  || 587,
    secure: cfg.smtp.secure || false,
    auth: { user: cfg.smtp.user, pass: cfg.smtp.pass },
    tls: { rejectUnauthorized: false }
})
```

#### Internal `_send(subject, html, toOverride)` Helper

- Merges `toOverride` (per-site recipients) with the global `_emailCfg.to` list (per-site takes priority if non-empty).
- Calls `_transporter.sendMail(...)`.
- Logs success (message ID) or failure to console. Never throws.

#### HTML Template Functions (internal)

| Function | Email Header Color | Trigger Condition |
|---|---|---|
| `_alarmHtml(alarm)` | Red `#dc2626` | Link goes down |
| `_resolveHtml(alarm)` | Green `#16a34a` | Link restored |
| `_ackHtml(alarm)` | Amber `#d97706` | Operator acknowledges |

All templates produce a responsive single-column HTML layout with a coloured header and a data table showing site, topic, timestamps, and status.

#### Public API

| Function | Guard Condition | Email Subject Pattern |
|---|---|---|
| `notifyAlarm(alarm, siteRecipients)` | `_emailCfg.onAlarm` must be true | `🔴 ALARM: {siteName} — {topicLabel} link down` |
| `notifyResolve(alarm, siteRecipients)` | `_emailCfg.onResolve` must be true | `✅ RESOLVED: {siteName} — {topicLabel} link restored` |
| `notifyAck(alarm, siteRecipients)` | `_emailCfg.onAck` must be true | `🟡 ACK: {siteName} — {topicLabel} acknowledged by {ackName}` |

---

## 6. Data Files Architecture

All data files live in the `data/` subdirectory and are JSON. The server reads them on startup and writes them back on any config/data change.

### 6.1 config.json

The master runtime configuration file. Contains two top-level sections:

```json
{
  "host":              "...",         // MQTT broker hostname
  "port":              8883,          // MQTT broker port (8883 = MQTTS)
  "protocol":          "mqtts",       // mqtt | mqtts | ws | wss
  "clientIdPrefix":    "Abdullah_Ans",
  "username":          "...",         // Broker username
  "password":          "...",         // Broker password
  "rejectUnauthorized": false,        // Set true for valid TLS certs

  "email": {
    "enabled":   false,               // Master switch for email
    "smtp": {
      "host":    "smtp.gmail.com",
      "port":    587,
      "secure":  false,               // true = port 465 SSL
      "user":    "...",
      "pass":    "..."                // Gmail App Password
    },
    "from":     "MQTT<...>",          // Sender display name and address
    "to":       [],                   // Global recipients list
    "onAlarm":  true,
    "onResolve": true,
    "onAck":    false,
    "siteEmails": {}                  // { "siteId": ["email1", "email2"] }
  }
}
```

### 6.2 sites.json

Array of site objects. Each site corresponds to one physical location with one or more PLCs.

```json
[
  {
    "id":          "uuid-v4",             // Internal unique identifier
    "createdAt":   "ISO-8601 timestamp",
    "name":        "Packages Mall Lahore",
    "description": "",
    "topics": [
      {
        "id":      "uuid-v4",             // Unique ID for this topic slot
        "label":   "PLC1",                // Display name shown in UI
        "topic":   "Packages",            // Actual MQTT topic string to subscribe
        "timeout": 60                     // Seconds before declaring link down
      }
    ],
    "mqttOverride": null,                 // null = use global config
    "snapshotLimit": 5                    // How many data snapshots to keep in memory
  }
]
```

#### Current Sites Configured

| Site Name | Topic(s) | Timeout | Snapshot Limit |
|---|---|---|---|
| Packages Mall Lahore | `Packages`, `Packages01` | 60s each | 5 |
| Suraj Cotton Lahore | `SurajCotton` | 60s | default (30) |
| Friends Cold Storage | `FreindsCold_Storage` | 60s | 5 |
| Prime Cold Storage | `PrimeCold` | 60s | default (30) |
| East Rice Area1 | `1.5.0/EastRice_Area1/TagValues` | 60s | default (30) |
| East Rice Area2 | `1.5.0/EastRice_Area3/TagValues` | 60s | default (30) |

### 6.3 users.json

Array of user account objects. Passwords are stored in plain text (consider hashing for production).

```json
[
  {
    "id":           "admin-default",
    "username":     "admin",
    "password":     "admin123",
    "role":         "admin",
    "displayName":  "Administrator",
    "allowedPages": ["dashboard","stats","alarms","alarm-trend","sites",
                     "mqtt-settings","email-settings","user-management",
                     "import-template","remove-template","clear-unacknowledged",
                     "clear-resolved","db-settings"],
    "createdAt":    "ISO-8601"
  }
]
```

**Available Page Tokens:**

| Token | Access Level | Description |
|---|---|---|
| `dashboard` | All users | Real-time link status grid |
| `stats` | All users | Statistics and summary view |
| `alarms` | All users | Alarm list with acknowledge actions |
| `alarm-trend` | All users | Historical alarm trend charts |
| `sites` | Admin | Add/edit/delete sites and topics |
| `mqtt-settings` | Admin | Edit broker host, port, credentials |
| `email-settings` | Admin | Configure SMTP and notification rules |
| `user-management` | Admin | Create/edit/delete user accounts |
| `import-template` | Admin/selective | Upload XLSX tag template for a site/topic |
| `remove-template` | Admin | Remove a saved template |
| `clear-unacknowledged` | Admin | Bulk-clear unacknowledged alarms |
| `clear-resolved` | Admin/selective | Bulk-clear resolved alarms |
| `db-settings` | Admin | Configure MongoDB profiles and enable/disable |

### 6.4 alarms.json

Array of alarm objects. Appended to whenever a new alarm is created. Updated in-place when an alarm is resolved or acknowledged.

```json
{
  "id":           "uuid-v4",
  "key":          "siteId||topicPath",
  "siteId":       "uuid-v4",
  "siteName":     "Packages Mall Lahore",
  "topicLabel":   "PLC1",
  "topicPath":    "Packages",
  "alarmStart":   1746518400000,          // Unix timestamp (ms)
  "alarmEnd":     1746518600000,          // null if still active
  "duration":     200,                    // seconds, null if still active
  "alarmText":    "No data received — link down",
  "acknowledged": false,
  "ackName":      "",
  "ackTime":      null,
  "ackNote":      "",
  "statusNote":   "",                     // "Auto-resolved: data resumed" when auto-resolved
  "history": [
    {
      "time":       1746518700000,
      "who":        "admin",
      "note":       "Checked and fixed",
      "statusNote": "",
      "action":     "acknowledge"
    }
  ]
}
```

### 6.5 db_config.json

```json
{
  "enabled":       false,               // true to activate MongoDB writes
  "activeProfile": "uuid-v4",           // ID of the profile currently in use
  "profiles": [
    {
      "id":          "uuid-v4",
      "name":        "Local MongoDB",
      "uri":         "mongodb://localhost:27017",
      "database":    "mqtt_monitor",
      "description": "",
      "createdAt":   "ISO-8601"
    }
  ],
  "retentionDays": 30,                  // 0 = keep all data forever
  "defaultLimit":  500,                 // Max docs returned per query
  "moduleLoaded":  true                 // Set by mongodbManager.js on successful require()
}
```

### 6.6 templates.json

Object keyed by `"siteId||topicPath"`. Each entry defines the tag layout for displaying structured data in the UI.

```json
{
  "siteId||topicPath": {
    "fileName": "Template.xlsx",         // Source file name (informational)
    "collections": {
      "Collection ID 1": ["TagName1", "TagName2", ...]
    },
    "areas": {
      "EM01": ["TagName1", "TagName2"],  // Logical grouping of tags
      "EM02": ["TagName3", "TagName4"],
      "CO2":  ["TagName5"],
      "Temprature": ["TagName6", "TagName7"]
    },
    "devices": {
      "Device1": ["EM01", "EM02"],       // Grouping of areas into devices
      "Device2": ["CO2", "Temprature"]
    }
  }
}
```

---

## 7. MQTT Payload Wire Formats

### Native Format

Produced by custom firmware or Node-RED bridges:

```json
{
  "collectionId": 1,
  "payload": [
    {
      "Time": "2026-05-06T10:00:00.000Z",
      "Values": {
        "FCS_CMP1_EM01_V_L1_L2": 230.5,
        "FCS_CMP1_EM01_CURR_L1": 48.2,
        "FCS_CMP1_EM01_TOTAL_KW": 11.04
      }
    }
  ]
}
```

### Wago Format

Produced by Wago PLC SCADA controllers running WagoProtocol 1.5.0:

```json
{
  "MessageType": "TagValues",
  "WagoProtocol": "1.5.0",
  "CollectionId": 1,
  "TagData": [
    {
      "Time": "2026-05-06T10:00:00.000Z",
      "Values": {
        "EM01_Voltage_L1": 229.8,
        "EM01_Current_L1": 47.5
      }
    }
  ]
}
```

### normaliseMessage() mapping

```
Native:  json.collectionId  →  collectionId
         json.payload        →  payload

Wago:    json.CollectionId  →  collectionId
         json.TagData        →  payload
```

---

## 8. Alarm Lifecycle

```
Topic receives data regularly
    │
    ▼ (elapsed > timeout)
ALARM CREATED
    • id: uuid-v4
    • alarmStart: Date.now()
    • alarmEnd: null
    • acknowledged: false
    • Written to alarms.json
    • storeMqttMessage() → MongoDB (if enabled)
    • notifyAlarm() → email sent (if onAlarm: true)
    │
    ├─── Operator acknowledges in UI:
    │       • acknowledged: true
    │       • ackName, ackTime, ackNote recorded
    │       • history[] entry pushed
    │       • storeAlarmEvent(alarm, 'acknowledged')
    │       • notifyAck() → email sent (if onAck: true)
    │
    └─── Data resumes on topic (elapsed < timeout):
            ALARM AUTO-RESOLVED
            • alarmEnd: Date.now()
            • duration: seconds of downtime
            • alarmText: "Link restored — data receiving normally"
            • statusNote: "Auto-resolved: data resumed"
            • storeAlarmEvent(alarm, 'resolved')
            • notifyResolve() → email sent (if onResolve: true)
```

---

## 9. User Roles & Page Permissions

### Admin (full access)
Username: `admin` | Password: `admin123`

Has access to all 14 page tokens. Can manage sites, broker settings, email, users, templates, MongoDB, and perform bulk alarm operations.

### Viewer (read-only by default)
Can be customised with any subset of pages. By default viewers (`Abdullah`, `Ali`) get dashboard, stats, alarms, alarm-trend — and optionally import-template and clear-resolved if explicitly granted.

---

## 10. MongoDB Collections Schema

### mqtt_messages

```json
{
  "_id":        "ObjectId",
  "siteId":     "uuid",
  "siteName":   "string",
  "topicPath":  "string",
  "topicLabel": "string",
  "tags":       { "tagName": value, ... },
  "ts":         "ISODate"               // TTL field — auto-deleted after retentionDays
}
```
**Indexes:** `{ siteId, topicPath, ts: -1 }`, `{ ts: 1 }` (TTL)

### alarm_events

```json
{
  "_id":        "ObjectId",
  ...alarmFields,                       // all fields from the alarm object
  "event":      "created|resolved|acknowledged",
  "ts":         "ISODate"
}
```
**Indexes:** `{ siteId, alarmStart: -1 }`, `{ key: 1 }`

### status_changes

```json
{
  "_id":        "ObjectId",
  "key":        "siteId||topicPath",
  "siteId":     "uuid",
  "siteName":   "string",
  "topicPath":  "string",
  "linked":     true | false,
  "ts":         "ISODate"
}
```
**Index:** `{ key: 1, ts: -1 }`

---

## 11. Email Notification System

### How to Enable

1. Open `data/config.json` (or use the Email Settings page in the UI).
2. Set `email.enabled: true`.
3. Fill in `smtp.host`, `smtp.port`, `smtp.user`, `smtp.pass`.
4. Add recipient addresses to the `to` array.
5. Set `onAlarm`, `onResolve`, `onAck` to `true` or `false` as needed.
6. Save. The server calls `emailNotifier.configure()` automatically.

### Per-Site Recipients

To send alarms for a specific site only to specific people, add to `siteEmails`:

```json
"siteEmails": {
  "44139f20-e3a1-4ae4-aa97-cab441682a31": ["manager@mall.com", "ops@mall.com"]
}
```

The `siteId` (UUID) is the key. If this list is non-empty, it overrides the global `to` list for that site.

### Gmail App Password Setup

1. Go to https://myaccount.google.com/apppasswords
2. Create a new App Password for "Mail" on "Other (Custom name)"
3. Copy the 16-character password (spaces are ignored)
4. Paste into `smtp.pass` in `config.json`

---

## 12. How to Run the Project

### Prerequisites

- Node.js v18 or higher
- npm v9 or higher
- (Optional) MongoDB 6.x for persistence

### Installation

```bash
# 1. Clone or extract the project folder
cd "MQTT_Monitor (6 May 2026)"

# 2. Install dependencies
npm install

# 3. (Optional) Install email support
npm install nodemailer

# 4. (Optional) Install MongoDB support
npm install mongodb

# 5. Start the server
node server.js
```

### Configuration Before First Run

Open `data/config.json` and update:

```json
{
  "host":     "YOUR-MQTT-BROKER-HOST",
  "port":     8883,
  "protocol": "mqtts",
  "username": "YOUR-USERNAME",
  "password": "YOUR-PASSWORD"
}
```

Open `data/sites.json` and define your sites and topics.

Open `data/users.json` and change the default admin password.

### Accessing the UI

After starting the server, open your browser:

```
http://localhost:3000
```

(Replace `3000` with the port configured in `server.js`.)

Login with: `admin` / `admin123` (change this immediately after first login).

---

## 13. Dependency Map

| Package | Purpose | Required? |
|---|---|---|
| `mqtt` | MQTT client library | **Yes** — core |
| `express` | HTTP/web server (in server.js) | **Yes** — core |
| `ws` or `socket.io` | WebSocket for real-time push (in server.js) | **Yes** — core |
| `uuid` | UUID generation for site/user/alarm IDs | **Yes** |
| `nodemailer` | Email sending | Optional (`npm install nodemailer`) |
| `mongodb` | MongoDB driver | Optional (`npm install mongodb`) |
| `multer` | File upload for template XLSX (in server.js) | Optional |
| `xlsx` | Parse uploaded XLSX template files (in server.js) | Optional |

---

*README generated from full source code analysis — MQTT Monitor, 6 May 2026.*