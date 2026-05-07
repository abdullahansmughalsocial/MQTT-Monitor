const mqtt = require('mqtt');
const { EventEmitter } = require('events');

/** Deep-copy a collections map so snapshots are independent of working state */
function deepCopyCollections(collections) {
  const out = {};
  for (const [id, col] of Object.entries(collections)) {
    out[id] = { tags: { ...col.tags } };
  }
  return out;
}

/**
 * MQTTManager — manages one MQTT client per site,
 * tracks per-topic link status, parses structured tag payloads,
 * and emits statusChange / tagDataChange events.
 */
class MQTTManager extends EventEmitter {
  constructor() {
    super();
    this.clients       = new Map(); // siteId → mqtt.Client
    this.statuses      = new Map(); // `${siteId}||${topic}` → StatusObj
    this.tagData       = new Map(); // `${siteId}||${topic}` → { working, snapshots[] }
    this._timer        = null;
    this._debounce     = null;
    this._tagDebounce  = null;
  }

  /** Call once after server starts to begin the timeout-check loop */
  start() {
    this._timer = setInterval(() => this._checkTimeouts(), 1000);
  }

  stop() {
    if (this._timer) clearInterval(this._timer);
    this.disconnectAll();
  }

  disconnectAll() {
    for (const id of [...this.clients.keys()]) {
      this.disconnectSite(id);
    }
  }

  /**
   * Connect (or reconnect) MQTT for a site.
   * Uses site.mqttOverride if provided; falls back to globalConfig.
   */
  connectSite(site, globalConfig) {
    this.disconnectSite(site.id);

    const cfg = (site.mqttOverride && site.mqttOverride.host)
      ? site.mqttOverride
      : globalConfig;

    if (!cfg || !cfg.host) {
      console.warn(`[${site.name}] No broker host — skipping`);
      return;
    }

    const opts = {
      host:             cfg.host,
      port:             cfg.port    || 8883,
      protocol:         cfg.protocol || 'mqtt',
      clientId:         `${(globalConfig.clientIdPrefix || 'mqtt-monitor')}-${site.id.slice(0,8)}-${Date.now()}`,
      clean:            true,
      connectTimeout:   30_000,
      keepalive:        60,
      reconnectPeriod:  5_000,
      rejectUnauthorized: cfg.rejectUnauthorized ?? false,
    };

    if (cfg.username) {
      opts.username = cfg.username;
      if (cfg.password) opts.password = cfg.password;
    }

    console.log(`[${site.name}] Connecting → ${cfg.protocol || 'mqtt'}://${cfg.host}:${opts.port}`);

    const client = mqtt.connect(opts);
    this.clients.set(site.id, client);

    // Initialise all topics as "waiting" (MQTT not yet connected)
    for (const t of (site.topics || [])) {
      const key = `${site.id}||${t.topic}`;
      // snapshotLimit: 0 = unlimited (Infinity), undefined/null = default 30, N = keep last N
      const rawLimit = site.snapshotLimit;
      const snapLimit = (rawLimit === 0) ? Infinity : (rawLimit || 30);

      this.statuses.set(key, {
        linked:        false,
        waiting:       true,   // true until MQTT connects
        startedAt:     null,   // set on MQTT connect
        lastSeen:      null,
        lastMessage:   null,
        timeout:       t.timeout || 60,
        label:         t.label || t.topic,
        snapshotLimit: snapLimit,  // ← configurable per-site; honours site.snapshotLimit
      });
    }
    this._emitStatus();

    client.on('connect', () => {
      console.log(`[${site.name}] ✅ Connected`);
      const connectedAt = Date.now();

      for (const t of (site.topics || [])) {
        const key = `${site.id}||${t.topic}`;
        const st  = this.statuses.get(key);
        if (st) {
          st.startedAt = connectedAt;
          st.waiting   = false;   // clock starts ticking
        }

        client.subscribe(t.topic, (err) => {
          if (err) console.error(`[${site.name}] Subscribe error (${t.topic}):`, err.message);
          else     console.log(`[${site.name}]   ↳ subscribed: ${t.topic}`);
        });
      }
      this._emitStatus();
    });

    client.on('message', (topic, message) => {
      const key = `${site.id}||${topic}`;
      const st  = this.statuses.get(key);
      if (!st) return;

      const wasLinked  = st.linked;
      st.linked        = true;
      st.waiting       = false;
      st.lastSeen      = Date.now();

      const rawStr = message.toString();
      st.lastMessage   = rawStr.slice(0, 300);

      // ── Structured tag-data parsing ──────────────────────────────────
      // Supports two wire formats:
      //
      // Native:  { "collectionId": 1, "payload": [{ "Time": "...", "Values": {...} }] }
      // Wago:    { "MessageType": "TagValues", "WagoProtocol": "1.5.0",
      //            "CollectionId": 1, "TagData": [{ "Time": "...", "Values": {...} }] }
      //
      // normaliseMessage() maps both to { collectionId, payload } before processing.
      function normaliseMessage(json) {
        if (!json || typeof json !== 'object') return null;
        // Native format (lowercase collectionId, payload array)
        if (typeof json.collectionId !== 'undefined' && Array.isArray(json.payload))
          return { collectionId: json.collectionId, payload: json.payload };
        // Wago format (uppercase CollectionId, TagData array)
        if (typeof json.CollectionId !== 'undefined' && Array.isArray(json.TagData))
          return { collectionId: json.CollectionId, payload: json.TagData };
        return null;
      }

      try {
        const json = JSON.parse(rawStr);
        const norm = normaliseMessage(json);
        if (norm && norm.payload.length > 0) {
          const entry = this.tagData.get(key) || {
            working:   { col1Time: null, collections: {} },
            snapshots: [],
          };
          const colId = norm.collectionId;

          // ── Collection 1 signals a new cycle ─────────────────────────
          // When col1 arrives with a NEW timestamp, the previous working
          // state is a completed cycle → freeze it as a snapshot.
          if (colId === 1 && norm.payload[0] && norm.payload[0].Time) {
            const newTime = norm.payload[0].Time;
            if (entry.working.col1Time && entry.working.col1Time !== newTime) {
              entry.snapshots.unshift({
                col1Time:    entry.working.col1Time,
                collections: deepCopyCollections(entry.working.collections),
                receivedAt:  Date.now(),
              });
              // Use the per-site configurable limit (Infinity = unlimited)
              const limit = st.snapshotLimit ?? 30;
              if (limit !== Infinity && entry.snapshots.length > limit) entry.snapshots.pop();
              entry.working = { col1Time: null, collections: {} };
            }
            entry.working.col1Time = newTime;
          }

          // Ensure a collection bucket in working state
          if (!entry.working.collections[colId]) entry.working.collections[colId] = { tags: {} };

          // Merge values (preserves insertion order for display sequence)
          for (const p of norm.payload) {
            if (p.Values && typeof p.Values === 'object') {
              for (const [tagName, tagValue] of Object.entries(p.Values)) {
                entry.working.collections[colId].tags[tagName] = tagValue;
              }
            }
          }

          this.tagData.set(key, entry);
          this._emitTagData();
        }
      } catch (_) { /* not structured JSON — plain text topic, ignore */ }

      // Always emit so frontend lastSeen stays current (not just on link transition)
      this._emitStatus();
    });

    client.on('error', (err) => {
      console.error(`[${site.name}] MQTT error:`, err.message);
    });

    client.on('close', () => {
      console.log(`[${site.name}] Connection closed`);
    });
  }

  disconnectSite(siteId) {
    if (this.clients.has(siteId)) {
      try { this.clients.get(siteId).end(true); } catch (_) {}
      this.clients.delete(siteId);
    }
    for (const key of this.statuses.keys()) {
      if (key.startsWith(`${siteId}||`)) this.statuses.delete(key);
    }
    for (const key of this.tagData.keys()) {
      if (key.startsWith(`${siteId}||`)) this.tagData.delete(key);
    }
  }

  /** Called every second; detects link-down conditions */
  _checkTimeouts() {
    const now     = Date.now();
    let   changed = false;

    for (const st of this.statuses.values()) {
      if (st.waiting || st.startedAt === null) continue;

      const reference = st.lastSeen ?? st.startedAt; // if never seen, use connect time
      const elapsed   = (now - reference) / 1000;
      const shouldBeLinked = elapsed < st.timeout;

      if (shouldBeLinked !== st.linked) {
        st.linked = shouldBeLinked;
        changed   = true;
      }
    }

    if (changed) this._emitStatus();
  }

  /** Debounced status emit so we don't flood WebSocket */
  _emitStatus() {
    clearTimeout(this._debounce);
    this._debounce = setTimeout(() => this.emit('statusChange'), 80);
  }

  /** Debounced tag-data emit */
  _emitTagData() {
    clearTimeout(this._tagDebounce);
    this._tagDebounce = setTimeout(() => this.emit('tagDataChange'), 80);
  }

  /** Snapshot of all topic statuses (sent to frontend) */
  getStatus() {
    const out = {};
    for (const [key, st] of this.statuses.entries()) {
      out[key] = {
        linked:      st.linked,
        waiting:     st.waiting,
        lastSeen:    st.lastSeen,
        lastMessage: st.lastMessage,
      };
    }
    return out;
  }

  /** Snapshot of all aggregated tag data (sent to frontend) */
  getTagData() {
    const out = {};
    for (const [key, entry] of this.tagData.entries()) {
      out[key] = {
        working: {
          col1Time:    entry.working.col1Time,
          collections: deepCopyCollections(entry.working.collections),
        },
        snapshots: entry.snapshots.map(s => ({
          col1Time:    s.col1Time,
          receivedAt:  s.receivedAt || null,
          collections: deepCopyCollections(s.collections),
        })),
      };
    }
    return out;
  }
}

module.exports = new MQTTManager();