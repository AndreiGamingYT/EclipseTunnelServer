const WebSocket = require("ws");
const http      = require("http");

// ─── Protocol ─────────────────────────────────────────────────────────────────
// Every WebSocket frame is binary:
//
//   [0x01] [utf-8 json...]   — control/signalling
//   [0x02] [uint32 BE key] [raw TCP bytes...]  — data, forwarded as-is
//
// The relay never touches the TCP payload bytes. Zero base64, zero JSON wrap.

// ─── Code generator ───────────────────────────────────────────────────────────
const WORDS_A = ["WOLF","IRON","DARK","VOID","STORM","FROST","EMBER","NOVA",
  "STEEL","ASH","BONE","CROW","DUSK","ECHO","FLUX","GRIM","HAWK","JADE",
  "KITE","LAVA","MIST","NEON","ONYX","PINE","RUNE","SAGE","THORN","VALE","WREN","ZEAL"];
const WORDS_B = ["KRAKEN","FORGE","REALM","RIDGE","SPIRE","CRYPT","DRIFT",
  "FLARE","GRIND","HAVEN","IGNITE","JOLT","KNELL","LURK","MARCH","NEXUS",
  "ORBIT","PULSE","QUAKE","RISEN","SHADE","TITAN","UNDER","VENOM","WRATH","XENON","YIELD","ZONED"];

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
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  const json  = Buffer.from(JSON.stringify(obj), "utf8");
  const frame = Buffer.allocUnsafe(1 + json.length);
  frame[0]    = TYPE_CTRL;
  json.copy(frame, 1);
  ws.send(frame);
}

function forwardData(ws, frame) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(frame);
}

// ─── State ────────────────────────────────────────────────────────────────────
// hosts: code → { ws: WebSocket, sessions: Map<key, { guestWs }> }
const hosts = new Map();
let keyCounter = 1;
function newKey() { return keyCounter++; }

// ─── HTTP ─────────────────────────────────────────────────────────────────────
const httpServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", hosts: hosts.size, uptime: Math.floor(process.uptime()) }));
  } else {
    res.writeHead(200); res.end("MC Tunnel Relay\n");
  }
});

// ─── WebSocket ────────────────────────────────────────────────────────────────
const wss = new WebSocket.Server({ server: httpServer });

wss.on("connection", (ws) => {
  let role       = null;   // "host" | "guest"
  let code       = null;
  let sessionKey = null;   // guest only — assigned once on initial handshake, never changes

  sendCtrl(ws, { type: "hello", version: "4.0.0" });

  ws.on("message", (data) => {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (buf.length === 0) return;

    // ── DATA frame ────────────────────────────────────────────────────────────
    if (buf[0] === TYPE_DATA) {
      if (buf.length < 5) return;
      const host = hosts.get(code);
      if (!host) return;

      if (role === "guest") {
        // guest → host: forward whole frame (host reads the key from bytes 1-4)
        forwardData(host.ws, buf);

      } else if (role === "host") {
        // host → guest: look up the guest by the key in the frame
        const key     = buf.readUInt32BE(1);
        const session = host.sessions.get(key);
        if (session) forwardData(session.guestWs, buf);
        // if session is gone, silently drop — guest already disconnected
      }
      return;
    }

    // ── CTRL frame ────────────────────────────────────────────────────────────
    if (buf[0] === TYPE_CTRL) {
      let msg;
      try { msg = JSON.parse(buf.slice(1).toString("utf8")); }
      catch { ws.close(1008, "Bad JSON"); return; }

      // Handshake
      if (role === null) {
        if (msg.type === "host") {
          role = "host";
          let attempts = 0;
          do {
            code = generateCode();
            if (++attempts > 50) { ws.close(1013, "Server full"); return; }
          } while (hosts.has(code));
          hosts.set(code, { ws, sessions: new Map() });
          console.log(`[HOST] code=${code}`);
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
          sessionKey = newKey();  // assigned ONCE, reused for the lifetime of this WS connection
          host.sessions.set(sessionKey, { guestWs: ws });
          console.log(`[GUEST] joined code=${code} key=${sessionKey}`);
          sendCtrl(ws,      { type: "connected", sessionKey });
          sendCtrl(host.ws, { type: "guest_joined", sessionKey });

        } else {
          ws.close(1008, "Unknown role");
        }
        return;
      }

      // Post-handshake
      if (msg.type === "ping") { sendCtrl(ws, { type: "pong" }); return; }

      // Guest's MC client opened a new TCP connection (first time or reconnect).
      // Re-register the session (it may have been removed by a prior tcp_closed)
      // and tell the host to open a fresh TCP socket to Minecraft.
      if (role === "guest" && msg.type === "tcp_opened") {
        const host = hosts.get(code);
        if (!host) return;
        // Increment generation so any in-flight tcp_closed from the previous
        // connection can detect it's stale and not kill this new session.
        const gen = ((host.sessions.get(sessionKey) || {}).gen || 0) + 1;
        host.sessions.set(sessionKey, { guestWs: ws, gen });
        console.log(`[GUEST] tcp_opened code=${code} key=${sessionKey} gen=${gen}`);
        sendCtrl(host.ws, { type: "guest_joined", sessionKey, gen });
        return;
      }

      if (role === "guest" && msg.type === "tcp_closed") {
        const host = hosts.get(code);
        if (host) {
          const session = host.sessions.get(sessionKey);
          // Only act if the generation matches — if gen is higher, a new
          // tcp_opened already arrived and we must not kill that new session.
          if (session && session.gen === msg.gen) {
            host.sessions.delete(sessionKey);
            sendCtrl(host.ws, { type: "guest_tcp_closed", sessionKey });
          }
        }
        console.log(`[GUEST] tcp_closed code=${code} key=${sessionKey} gen=${msg.gen}`);
        return;
      }

      // Host's TCP socket to Minecraft closed (MC server dropped the connection).
      // Tell the guest so it can close the MC client socket.
      if (role === "host" && msg.type === "tcp_closed") {
        const host = hosts.get(code);
        if (!host) return;
        const session = host.sessions.get(msg.sessionKey);
        if (session) {
          host.sessions.delete(msg.sessionKey);
          sendCtrl(session.guestWs, { type: "host_tcp_closed" });
        }
        return;
      }

      return;
    }
    // unknown frame type — ignore
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
      // WS itself closed (tab closed, network gone, etc.)
      // This is separate from tcp_closed — clean up whatever remains.
      console.log(`[GUEST] WS closed code=${code} key=${sessionKey}`);
      const host = hosts.get(code);
      if (host && host.sessions.has(sessionKey)) {
        host.sessions.delete(sessionKey);
        sendCtrl(host.ws, { type: "guest_left", sessionKey });
      }
    }
  });

  ws.on("error", (err) => console.error(`[WS ERR] role=${role} code=${code}:`, err.message));
});

// ─── Start ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => {
  console.log(`MC Tunnel Relay v4 on port ${PORT}`);
  console.log(`Health: http://localhost:${PORT}/health`);
});
