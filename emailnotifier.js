/**
 * emailNotifier.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Nodemailer-based email notification module for MQTT Monitor.
 *
 * Install once:
 *   npm install nodemailer
 *
 * Email config is stored alongside the broker config inside data/config.json:
 * {
 *   "email": {
 *     "enabled": true,
 *     "smtp": {
 *       "host":    "smtp.gmail.com",
 *       "port":    587,
 *       "secure":  false,
 *       "user":    "you@gmail.com",
 *       "pass":    "your-app-password"
 *     },
 *     "from":       "MQTT Monitor <you@gmail.com>",
 *     "to":         ["ops@example.com", "manager@example.com"],
 *     "onAlarm":    true,   // send when link goes DOWN
 *     "onResolve":  true,   // send when link comes back UP
 *     "onAck":      false   // send when an alarm is acknowledged
 *   }
 * }
 *
 * For Gmail you MUST use an App Password (not your normal password).
 * Generate one at: https://myaccount.google.com/apppasswords
 * ─────────────────────────────────────────────────────────────────────────────
 */

let nodemailer;
try {
  nodemailer = require('nodemailer');
} catch (_) {
  console.warn('[Email] nodemailer not installed — run: npm install nodemailer');
}

let _transporter = null;
let _emailCfg    = null;

/** Call this whenever config changes so the transporter is always fresh */
function configure(emailCfg) {
  _emailCfg    = emailCfg || null;
  _transporter = null;

  if (!nodemailer)             return;
  if (!emailCfg?.enabled)      return;
  if (!emailCfg?.smtp?.host)   return;

  _transporter = nodemailer.createTransport({
    host:   emailCfg.smtp.host,
    port:   emailCfg.smtp.port  || 587,
    secure: emailCfg.smtp.secure || false,  // true for port 465
    auth: {
      user: emailCfg.smtp.user,
      pass: emailCfg.smtp.pass,
    },
    tls: { rejectUnauthorized: false },
  });

  console.log(`[Email] Transporter ready → ${emailCfg.smtp.host}:${emailCfg.smtp.port || 587}`);
}

/** Internal send helper — toOverride takes priority over config.to */
async function _send(subject, html, toOverride) {
  if (!_transporter || !_emailCfg?.enabled) return;
  // Per-site list overrides global list; fall back to global if empty/absent
  const recipients = (toOverride && toOverride.length) ? toOverride : _emailCfg.to;
  const to = Array.isArray(recipients) ? recipients.join(',') : recipients;
  if (!to) return;

  try {
    const info = await _transporter.sendMail({
      from:    _emailCfg.from || _emailCfg.smtp.user,
      to,
      subject,
      html,
    });
    console.log(`[Email] ✉  Sent "${subject}" → ${to}  (${info.messageId})`);
  } catch (err) {
    console.error(`[Email] ✖  Failed to send "${subject}":`, err.message);
  }
}

// ─── Email templates ──────────────────────────────────────────────────────────

function _alarmHtml(alarm) {
  const start = new Date(alarm.alarmStart).toLocaleString();
  return `
  <div style="font-family:Arial,sans-serif;max-width:600px;margin:auto">
    <div style="background:#dc2626;color:#fff;padding:16px 24px;border-radius:8px 8px 0 0">
      <h2 style="margin:0">🔴 MQTT Link Alarm</h2>
    </div>
    <div style="border:1px solid #e5e7eb;border-top:none;padding:24px;border-radius:0 0 8px 8px">
      <table style="width:100%;border-collapse:collapse">
        <tr><td style="padding:8px 0;color:#6b7280;width:140px">Site</td>
            <td style="padding:8px 0;font-weight:bold">${alarm.siteName}</td></tr>
        <tr><td style="padding:8px 0;color:#6b7280">Topic</td>
            <td style="padding:8px 0">${alarm.topicLabel} <code style="color:#6b7280;font-size:12px">(${alarm.topicPath})</code></td></tr>
        <tr><td style="padding:8px 0;color:#6b7280">Alarm Start</td>
            <td style="padding:8px 0">${start}</td></tr>
        <tr><td style="padding:8px 0;color:#6b7280">Status</td>
            <td style="padding:8px 0;color:#dc2626;font-weight:bold">${alarm.alarmText}</td></tr>
      </table>
      <hr style="border:none;border-top:1px solid #e5e7eb;margin:16px 0">
      <p style="color:#6b7280;font-size:13px;margin:0">
        This alarm was raised automatically by MQTT Monitor because no data was
        received within the configured timeout period.
        Please investigate the link and acknowledge the alarm in the dashboard.
      </p>
    </div>
  </div>`;
}

function _resolveHtml(alarm) {
  const start    = new Date(alarm.alarmStart).toLocaleString();
  const end      = new Date(alarm.alarmEnd).toLocaleString();
  const duration = alarm.duration != null
    ? `${Math.floor(alarm.duration / 60)}m ${alarm.duration % 60}s`
    : 'unknown';

  return `
  <div style="font-family:Arial,sans-serif;max-width:600px;margin:auto">
    <div style="background:#16a34a;color:#fff;padding:16px 24px;border-radius:8px 8px 0 0">
      <h2 style="margin:0">✅ MQTT Link Restored</h2>
    </div>
    <div style="border:1px solid #e5e7eb;border-top:none;padding:24px;border-radius:0 0 8px 8px">
      <table style="width:100%;border-collapse:collapse">
        <tr><td style="padding:8px 0;color:#6b7280;width:140px">Site</td>
            <td style="padding:8px 0;font-weight:bold">${alarm.siteName}</td></tr>
        <tr><td style="padding:8px 0;color:#6b7280">Topic</td>
            <td style="padding:8px 0">${alarm.topicLabel} <code style="color:#6b7280;font-size:12px">(${alarm.topicPath})</code></td></tr>
        <tr><td style="padding:8px 0;color:#6b7280">Alarm Start</td>
            <td style="padding:8px 0">${start}</td></tr>
        <tr><td style="padding:8px 0;color:#6b7280">Resolved At</td>
            <td style="padding:8px 0">${end}</td></tr>
        <tr><td style="padding:8px 0;color:#6b7280">Down Duration</td>
            <td style="padding:8px 0;font-weight:bold">${duration}</td></tr>
        <tr><td style="padding:8px 0;color:#6b7280">Status</td>
            <td style="padding:8px 0;color:#16a34a;font-weight:bold">${alarm.alarmText}</td></tr>
      </table>
    </div>
  </div>`;
}

function _ackHtml(alarm) {
  const ackTime = alarm.ackTime ? new Date(alarm.ackTime).toLocaleString() : 'Unknown';
  return `
  <div style="font-family:Arial,sans-serif;max-width:600px;margin:auto">
    <div style="background:#d97706;color:#fff;padding:16px 24px;border-radius:8px 8px 0 0">
      <h2 style="margin:0">🟡 Alarm Acknowledged</h2>
    </div>
    <div style="border:1px solid #e5e7eb;border-top:none;padding:24px;border-radius:0 0 8px 8px">
      <table style="width:100%;border-collapse:collapse">
        <tr><td style="padding:8px 0;color:#6b7280;width:140px">Site</td>
            <td style="padding:8px 0;font-weight:bold">${alarm.siteName}</td></tr>
        <tr><td style="padding:8px 0;color:#6b7280">Topic</td>
            <td style="padding:8px 0">${alarm.topicLabel}</td></tr>
        <tr><td style="padding:8px 0;color:#6b7280">Acknowledged By</td>
            <td style="padding:8px 0;font-weight:bold">${alarm.ackName || 'Unknown'}</td></tr>
        <tr><td style="padding:8px 0;color:#6b7280">Acknowledged At</td>
            <td style="padding:8px 0">${ackTime}</td></tr>
        <tr><td style="padding:8px 0;color:#6b7280">Note</td>
            <td style="padding:8px 0">${alarm.ackNote || '—'}</td></tr>
      </table>
    </div>
  </div>`;
}

// ─── Public API ───────────────────────────────────────────────────────────────

/** Notify when a link goes DOWN */
async function notifyAlarm(alarm, siteRecipients) {
  if (!_emailCfg?.onAlarm) return;
  const subject = `🔴 ALARM: ${alarm.siteName} — ${alarm.topicLabel} link down`;
  await _send(subject, _alarmHtml(alarm), siteRecipients);
}

/** Notify when a link is RESTORED */
async function notifyResolve(alarm, siteRecipients) {
  if (!_emailCfg?.onResolve) return;
  const subject = `✅ RESOLVED: ${alarm.siteName} — ${alarm.topicLabel} link restored`;
  await _send(subject, _resolveHtml(alarm), siteRecipients);
}

/** Notify when an alarm is ACKNOWLEDGED */
async function notifyAck(alarm, siteRecipients) {
  if (!_emailCfg?.onAck) return;
  const subject = `🟡 ACK: ${alarm.siteName} — ${alarm.topicLabel} acknowledged by ${alarm.ackName || 'Unknown'}`;
  await _send(subject, _ackHtml(alarm), siteRecipients);
}

module.exports = { configure, notifyAlarm, notifyResolve, notifyAck };