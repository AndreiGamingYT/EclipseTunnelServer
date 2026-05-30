const WebSocket = require("ws");
const http = require("http");

// ─── Protocol ─────────────────────────────────────────────────────────────────
//
// ALL frames are BINARY. Format:
//
//   [1 byte: msg_type] [payload...]
//
// msg_type values:
//   0x01  CTRL_JSON   — payload is UTF-8 JSON (control messages)
//   0x02  DATA        — [4 bytes: sessionKey BE uint32][...raw TCP bytes]
//
// Why sessionKey instead of slot?
//   A slot (0-254) is fine for the relay↔host leg, but we use a 32-bit key
//   everywhere so host-side code is symmetric and there's no off-by-one risk.
//   The relay assigns each guest a unique 32-bit sessionKey on connect.
//
// Data path (guest → host):
//   Guest TCP recv → Guest packs [0x02][sessionKey 4B][bytes] → Relay WS →
//   Relay looks up host WS for that sessionKey → forwards identical frame →
//   Host unpacks sessionKey, finds TCP socket, writes raw bytes → MC server
//
// Data path (host → guest):
//   MC server → Host TCP recv → Host packs [0x02][sessionKey 4B][bytes] →
//   Relay WS → Relay looks up guest WS for that sessionKey →
//   forwards identical frame → Guest unpacks, writes raw bytes → MC client
//
// The relay forwards DATA frames byte-for-byte without touching payload.
// The TCP bytes are NEVER base64'd, never JSON-wrapped. Zero copies of the
// payload beyond what Buffer/net gives us.

// ─── Code Generator ───────────────────────────────────────────────────────────
const WORDS_A = [
  "WOLF","IRON","DARK","VOID","STORM","FROST","EMBER","NOVA",
  "STEEL","ASH","BONE","CROW","DUSK","ECHO","FLUX","GRIM",
  "HAWK","JADE","KITE","LAVA","MIST","NEON","ONYX","PINE",
  "RUNE","SAGE","THORN","VALE","WREN","ZEAL"
];
const WORDS_B = [
  "KRAKEN","FORGE","REALM","RIDGE","SPIRE","CRYPT","DRIFT",
  "FLARE","GRIND","HAVEN","IGNITE","JOLT","KNELL","LURK",
  "MARCH","NEXUS","ORBIT","PULSE","QUAKE","RISEN","SHADE",
  "TITAN","UNDER","VENOM","WRATH","XENON","YIELD","ZONED"
];

function generateCode() {
  const a = WORDS_A[Math.floor(Math.random() * WORDS_A.length)];
  const b = WORDS_B[Math.floor(Math.random() * WORDS_B.length)];
  const n = String(Math.floor(Math.random() * 9000) + 1000);
  return `${a}-${n}-${b}`;
}

// ─── Frame helpers ────────────────────────────────────────────────────────────
const TYPE_CTRL = 0x01;
const TYPE_DATA = 0x02;

function sendCtrl(ws, obj) {
  if (ws.readyState !== WebSocket.OPEN) return;
  const json = Buffer.from(JSON.stringify(obj), "utf8");
  const frame = Buffer.allocUnsafe(1 + json.length);
  frame[0] = TYPE_CTRL;
  json.copy(frame, 1);
  ws.send(frame);
}

// Forward a DATA frame as-is (relay never modifies payload)
function forwardData(ws, frame) {
  if (ws.readyState === WebSocket.OPEN) ws.send(frame);
}

// Build a DATA frame: [0x02][sessionKey 4B BE][payload]
function buildDataFrame(sessionKey, payload) {
  const frame = Buffer.allocUnsafe(5 + payload.length);
  frame[0] = TYPE_DATA;
  frame.writeUInt32BE(sessionKey, 1);
  payload.copy(frame, 5);
  return frame;
}

// ─── State ────────────────────────────────────────────────────────────────────
// hosts: code → { ws, sessions: Map<sessionKey, { guestWs }> }
const hosts = new Map();

let keyCounter = 1;
function newSessionKey() { return keyCounter++; }

// ─── HTTP ─────────────────────────────────────────────────────────────────────
const httpServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      status: "ok",
      hosts: hosts.size,
      uptime: Math.floor(process.uptime())
    }));
  } else {
    res.writeHead(200);
    res.end("MC Tunnel Relay\n");
  }
});

// ─── WebSocket ────────────────────────────────────────────────────────────────
const wss = new WebSocket.Server({ server: httpServer });

wss.on("connection", (ws) => {
  let role       = null;   // "host" | "guest"
  let code       = null;
  let sessionKey = null;   // guest only

  // Send hello immediately so client knows server is ready
  sendCtrl(ws, { type: "hello", version: "3.0.0" });

  ws.on("message", (data) => {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (buf.length === 0) return;

    const msgType = buf[0];

    // ── DATA frame ──────────────────────────────────────────────────────────
    if (msgType === TYPE_DATA) {
      if (buf.length < 5) return; // malformed

      const key     = buf.readUInt32BE(1);
      const host    = hosts.get(code);
      if (!host) return;

      if (role === "guest") {
        // guest→host: forward the whole frame to the host
        forwardData(host.ws, buf);

      } else if (role === "host") {
        // host→guest: find the guest by sessionKey embedded in frame
        const session = host.sessions.get(key);
        if (session) forwardData(session.guestWs, buf);
      }
      return;
    }

    // ── CTRL frame ──────────────────────────────────────────────────────────
    if (msgType === TYPE_CTRL) {
      let msg;
      try { msg = JSON.parse(buf.slice(1).toString("utf8")); }
      catch { ws.close(1008, "Bad JSON in CTRL frame"); return; }

      // ── Handshake (role not set yet) ───────────────────────────────────
      if (role === null) {
        if (msg.type === "host") {
          role = "host";
          let attempts = 0;
          do {
            code = generateCode();
            if (++attempts > 50) { ws.close(1013, "Server full"); return; }
          } while (hosts.has(code));

          hosts.set(code, { ws, sessions: new Map() });
          console.log(`[HOST] registered code=${code}`);
          sendCtrl(ws, { type: "hosted", code });

        } else if (msg.type === "guest") {
          const host = hosts.get(msg.code);
          if (!host) {
            sendCtrl(ws, { type: "error", message: "Invalid or expired code." });
            ws.close(1008, "Invalid code");
            return;
          }
          role       = "guest";
          code       = msg.code;
          sessionKey = newSessionKey();
          host.sessions.set(sessionKey, { guestWs: ws });
          console.log(`[GUEST] joined code=${code} sessionKey=${sessionKey}`);
          sendCtrl(ws,      { type: "connected", sessionKey });
          sendCtrl(host.ws, { type: "guest_joined", sessionKey });

        } else {
          ws.close(1008, "Unknown role");
        }
        return;
      }

      // ── Post-handshake control ─────────────────────────────────────────
      if (msg.type === "ping") {
        sendCtrl(ws, { type: "pong" });
        return;
      }

      // Host notifies relay that a guest's TCP side closed cleanly
      if (role === "host" && msg.type === "tcp_closed") {
        const host = hosts.get(code);
        if (!host) return;
        const session = host.sessions.get(msg.sessionKey);
        if (session) {
          sendCtrl(session.guestWs, { type: "host_tcp_closed" });
          host.sessions.delete(msg.sessionKey);
        }
        return;
      }

      // Guest notifies relay that its TCP side closed cleanly
      if (role === "guest" && msg.type === "tcp_closed") {
        const host = hosts.get(code);
        if (host) {
          sendCtrl(host.ws, { type: "guest_tcp_closed", sessionKey });
          host.sessions.delete(sessionKey);
        }
        return;
      }

      // Guest's MC client reconnected — re-register session and tell host to
      // open a fresh TCP socket to its local Minecraft server.
      if (role === "guest" && msg.type === "tcp_opened") {
        const host = hosts.get(code);
        if (!host) return;
        host.sessions.set(sessionKey, { guestWs: ws }); // re-add with live ws
        console.log(`[GUEST] TCP reopened code=${code} sessionKey=${sessionKey}`);
        sendCtrl(host.ws, { type: "guest_joined", sessionKey });
        return;
      }

      return;
    }

    // Unknown frame type — ignore silently
  });

  ws.on("close", () => {
    if (role === "host" && code) {
      console.log(`[HOST] disconnected code=${code}`);
      const host = hosts.get(code);
      if (host) {
        for (const [, session] of host.sessions) {
          sendCtrl(session.guestWs, { type: "host_disconnected" });
          session.guestWs.close();
        }
        hosts.delete(code);
      }
    } else if (role === "guest" && code) {
      console.log(`[GUEST] disconnected code=${code} sessionKey=${sessionKey}`);
      const host = hosts.get(code);
      if (host) {
        host.sessions.delete(sessionKey);
        sendCtrl(host.ws, { type: "guest_left", sessionKey });
      }
    }
  });

  ws.on("error", (err) =>
    console.error(`[WS ERR] role=${role} code=${code}:`, err.message));
});

// ─── Start ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => {
  console.log(`MC Tunnel Relay v3 running on port ${PORT}`);
  console.log(`Health: http://localhost:${PORT}/health`);
});