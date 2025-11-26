const WebSocket = require('ws');
const http = require('http');
const crypto = require('crypto');

// Mapări globale
const hosts = new Map(); // host_id -> Host_WebSocket
const hostGuestMap = new Map(); // host_id -> Map<guest_id, Guest_WebSocket>
const guestWsToHostId = new Map(); // guest_ws -> { host_id: string, guest_id: string }

// CONSTANTE PENTRU PROTOCOLUL BINAR (TIP DE MESAJ - 1 OCTET)
const MSG_TYPE_NEW_GUEST = 0;
const MSG_TYPE_DATA = 1;
const MSG_TYPE_CLOSE_GUEST = 2;
const HEADER_SIZE = 7; // 1 octet (Tip) + 6 octeți (G_ID)

const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('WebSocket Proxy Server Running (Binary Protocol)\n');
});

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
    const url = req.url.split('?')[0];

    if (url === '/host') {
        handleHostConnection(ws);
    } else if (url.startsWith('/guest/')) {
        const hostId = url.substring('/guest/'.length);
        handleGuestConnection(ws, hostId);
    } else {
        ws.close(1008, 'Ruta necunoscută.');
    }
});

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
    console.log(`Serverul WebSocket rulează pe portul ${PORT}`);
});

// --- Funcții de Gestiune ---

function handleHostConnection(ws) {
    const hostId = crypto.randomBytes(4).toString('hex');
    hosts.set(hostId, ws);
    hostGuestMap.set(hostId, new Map());

    console.log(`[HOST] Conectat. ID: ${hostId}`);

    // Mesaj inițial de control (rămâne JSON/String, este o excepție)
    ws.send(JSON.stringify({ type: 'HOST_ID', hostId: hostId }));

    ws.on('message', (message) => {
        // Host-ul trimite pachete binare (Buffer) către Guest-i
        if (Buffer.isBuffer(message)) {
            routeDataToGuest(hostId, message);
        }
        // Ignorăm orice alt tip de mesaj neașteptat (ex: string-uri)
    });

    ws.on('close', () => {
        console.log(`[HOST] ${hostId} deconectat. Închid conexiunile Guest.`);
        
        const currentGuests = hostGuestMap.get(hostId) || new Map();
        for (const guestWs of currentGuests.values()) {
            guestWs.close(1000, 'Host deconectat.');
            guestWsToHostId.delete(guestWs);
        }

        hosts.delete(hostId);
        hostGuestMap.delete(hostId);
    });
}

function handleGuestConnection(ws, hostId) {
    const hostWs = hosts.get(hostId);

    if (!hostWs || hostWs.readyState !== WebSocket.OPEN) {
        ws.close(1000, `Hostul cu ID-ul ${hostId} nu este online.`);
        return;
    }

    const guestId = crypto.randomBytes(6).toString('hex'); // G_ID are 6 octeți (12 caractere hex)
    
    // Mapează Guest-ul
    const guestMap = hostGuestMap.get(hostId);
    guestMap.set(guestId, ws);
    guestWsToHostId.set(ws, { hostId, guestId });

    console.log(`[GUEST] Conectat la Host ${hostId}. G_ID: ${guestId}`);

    // Semnalizează Host-ul că un Guest nou s-a conectat (PACHET BINAR TIP 0)
    const newGuestBuffer = Buffer.alloc(HEADER_SIZE);
    newGuestBuffer.writeUInt8(MSG_TYPE_NEW_GUEST, 0); 
    newGuestBuffer.write(guestId, 1, 6, 'hex'); // Scrie G_ID de 6 octeți (12 caractere)
    hostWs.send(newGuestBuffer);

    ws.on('message', (message) => {
        // Guest-ul trimite date brute (Buffer) - le trimitem Host-ului
        if (Buffer.isBuffer(message)) {
            routeDataToHost(hostId, guestId, message);
        }
    });

    ws.on('close', () => {
        console.log(`[GUEST] ${guestId} deconectat.`);
        
        // Notifică Host-ul pentru a închide socket-ul TCP local (PACHET BINAR TIP 2)
        if (hostWs.readyState === WebSocket.OPEN) {
            const closeBuffer = Buffer.alloc(HEADER_SIZE);
            closeBuffer.writeUInt8(MSG_TYPE_CLOSE_GUEST, 0); 
            closeBuffer.write(guestId, 1, 6, 'hex');
            hostWs.send(closeBuffer);
        }

        const currentGuests = hostGuestMap.get(hostId);
        if (currentGuests) {
            currentGuests.delete(guestId);
        }
        guestWsToHostId.delete(ws);
    });
}

// --- Funcții de Rutare ---

/**
 * Trimite date de la Host (pachet binar complet) către un Guest specific (payload TCP pur).
 */
function routeDataToGuest(hostId, buffer) {
    // Buffer așteptat: [1 octet: Tip] + [6 octeți: G_ID] + [N octeți: Date TCP]
    if (buffer.length < HEADER_SIZE) return;

    const type = buffer.readUInt8(0);
    const guestId = buffer.toString('hex', 1, HEADER_SIZE); // Citeste 6 octeți (12 caractere)
    const payload = buffer.slice(HEADER_SIZE); 

    const guestMap = hostGuestMap.get(hostId);
    if (!guestMap) return;

    if (type === MSG_TYPE_DATA) {
        const guestWs = guestMap.get(guestId);
        if (guestWs && guestWs.readyState === WebSocket.OPEN) {
            // Trimite payload-ul TCP pur (Buffer) către Guest
            guestWs.send(payload); 
        } 
    } else if (type === MSG_TYPE_CLOSE_GUEST) {
        closeGuestConnection(hostId, guestId);
    }
}

/**
 * Trimite date de la Guest (payload TCP pur) către Host (pachet binar complet).
 */
function routeDataToHost(hostId, guestId, data) {
    const hostWs = hosts.get(hostId);
    if (hostWs && hostWs.readyState === WebSocket.OPEN) {
        // Creează header-ul: [1 octet: Tip (DATA=1)] + [6 octeți: G_ID]
        const header = Buffer.alloc(HEADER_SIZE);
        header.writeUInt8(MSG_TYPE_DATA, 0); 
        header.write(guestId, 1, 6, 'hex'); 

        // Concatenăm header-ul cu datele TCP
        const message = Buffer.concat([header, data]);
        
        hostWs.send(message);
    }
}

/**
 * Închide o conexiune Guest inițiată de Host.
 */
function closeGuestConnection(hostId, guestId) {
    const guestMap = hostGuestMap.get(hostId);
    if (guestMap) {
        const guestWs = guestMap.get(guestId);
        if (guestWs) {
            guestWs.close(1000, 'Hostul a închis conexiunea TCP locală.');
            guestMap.delete(guestId);
            guestWsToHostId.delete(guestWs);
        }
    }
}