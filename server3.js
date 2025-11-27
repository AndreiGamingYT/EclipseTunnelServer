const WebSocket = require('ws');
const http = require('http');
const express = require('express');
const { URL } = require('url');

// --- Configurare ---
const SERVER_PORT = process.env.PORT || 8080;
const app = express();

// --- Constante Protocol Binar ---
const MSG_TYPE_INIT_HOST = 0x00;
const HEADER_SIZE = 7;
// TCP
const MSG_TYPE_NEW_GUEST = 0x01;
const MSG_TYPE_DATA = 0x02;
const MSG_TYPE_CLOSE = 0x03;
// HTTP
const MSG_TYPE_HTTP_REQ = 0x04; // Cerere HTTP (Server Central -> Host)
const MSG_TYPE_HTTP_RES = 0x05; // Răspuns HTTP (Host -> Server Central)

// --- Mape de Stare ---
const hostWsMap = new Map(); // Key: Host ID (e.g., 'b660f23e'), Value: Host WebSocket
const guestWsMap = new Map(); // Key: Guest ID (e.g., 'a1b2c3d4e5f6'), Value: Guest WebSocket (pentru TCP)
// SCHIMBARE: Acum stocăm {res: ExpressResponse, hostId: string}
const httpResponseMap = new Map(); // Key: Request ID, Value: {res: ExpressResponse, hostId: string} 
let nextRequestId = 1; // NOU: Contor pentru a urmări cererile HTTP

// --- Middleware CORS (pentru a permite accesul de pe web.stremio.com) ---
app.use((req, res, next) => {
    let hostId = null;

    // Dacă URL include /u/:hostId/ îl folosim
    const match = req.path.match(/^\/u\/([0-9a-f]+)(\/.*|$)/);
    if (match) {
        hostId = match[1];
        req.url = match[2] || '/';
    } else {
        // Dacă nu include, alegem un host activ (poți modifica după cum vrei)
        const hosts = Array.from(hostWsMap.keys());
        if (hosts.length === 0) return res.status(404).send('No hosts connected');
        hostId = hosts[0];
    }

    const hostWs = hostWsMap.get(hostId);
    if (!hostWs || hostWs.readyState !== WebSocket.OPEN) return res.status(404).send('Host not available');

    req.res = res;
    console.log(`[PROXY] Forwarding ${req.method} ${req.originalUrl} -> Host ${hostId}${req.url}`);
    sendHttpTunnelRequest(hostWs, req, hostId);
});

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use((req, res, next) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
});

// --- Funcții Helper ---

/**
 * Encapsulează cererea HTTP Express într-un pachet WebSocket (baza 11 octeți + JSON).
 */
function sendHttpTunnelRequest(hostWs, req) {
    const requestId = nextRequestId++;
    const guestId = Math.random().toString(16).substring(2, 14); // Guest/Session ID pentru HTTP (12 caractere hex)

    // 1. Corpul JSON al cererii
    const requestPayload = {
        method: req.method,
        // req.url este deja curățat în tunnelHandler și conține doar calea relativă + query string
        url: req.url, 
        headers: req.headers,
        body: req.body ? req.body.toString('base64') : null,
    };

    // 2. Antetul binar (11 octeți)
    const header = Buffer.alloc(11);
    header.writeUInt8(MSG_TYPE_HTTP_REQ, 0);
    header.write(guestId, 1, 6, 'hex'); // Folosim Guest ID pentru consistency
    header.writeUInt32BE(requestId, 7); // ID unic al cererii

    // 3. Mesajul final
    const message = Buffer.concat([header, Buffer.from(JSON.stringify(requestPayload), 'utf8')]);

    // SCHIMBARE #1: Salvează obiectul Express Response și Host ID-ul
    httpResponseMap.set(requestId, { res: req.res, hostId: hostWs.hostId });

    hostWs.send(message, (err) => {
        if (err) {
            console.error(`[SERVER HTTP] Eroare la trimiterea către Host ${hostWs.hostId}: ${err.message}`);
            // Curățare și răspuns 500 imediat
            const entry = httpResponseMap.get(requestId);
            if (entry && entry.res && !entry.res.headersSent) {
                entry.res.status(500).send('Tunneling Error: Failed to send request to Host.');
                httpResponseMap.delete(requestId);
            }
        } else {
            console.log(`[SERVER HTTP] Cerere #${requestId} trimisă către Host ${hostWs.hostId}.`);
        }
    });
}

/**
 * Extrage răspunsul HTTP din pachetul WebSocket și îl trimite înapoi către Browser (Express).
 */
function handleHttpTunnelResponse(data) {
    // 1. Decapsulare Antet
    const requestId = data.readUInt32BE(7);
    const guestId = data.toString('hex', 1, 7);
    
    const entry = httpResponseMap.get(requestId);
    if (!entry || entry.res.headersSent) {
        console.warn(`[SERVER HTTP] Răspuns #${requestId} (Guest: ${guestId}) primit, dar Express Response nu mai este disponibil.`);
        return;
    }

    const res = entry.res; // Obiectul Express Response
    const hostId = entry.hostId; // Host ID-ul asociat

    // 2. Decapsulare Corp JSON
    try {
        const jsonPayload = data.slice(11).toString('utf8');
        const resData = JSON.parse(jsonPayload);
        
        // 3. Trimiterea Răspunsului către Browser
        if (resData.headers) {
            // Setează headerele primite de la serverul web local
            for (const key in resData.headers) {
                const lowerKey = key.toLowerCase();
                const value = resData.headers[key];

                // Evită headerele de conexiune care nu trebuie proxy-ate (de ex. 'connection', 'keep-alive')
                if (!['connection', 'keep-alive', 'content-length', 'transfer-encoding'].includes(lowerKey)) {
                    
                    // SCHIMBARE #2: Modifică headerul Location dacă este o redirecționare relativă
                    if (lowerKey === 'location' && value.startsWith('/')) {
                        // Re-scrie calea pentru a include prefixul tunelului
                        res.setHeader(key, `/u/${hostId}${value}`);
                    } else {
                        res.setHeader(key, value);
                    }
                }
            }
            // Re-aplică headerele CORS pentru siguranță (deși au fost setate și în middleware)
            res.setHeader('Access-Control-Allow-Origin', '*');
            res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
            res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With');
        }

        const body = resData.body ? Buffer.from(resData.body, 'base64') : Buffer.alloc(0);
        
        // Setează Status Code și trimite corpul
        res.status(resData.statusCode || 200).send(body);

    } catch (e) {
        console.error(`[SERVER HTTP] Eroare la parsarea răspunsului #${requestId}: ${e.message}`);
        res.status(500).send('Internal Server Error: Failed to parse tunnel response.');
    } finally {
        // Șterge referința la Express Response (pentru a evita memory leaks)
        httpResponseMap.delete(requestId);
    }
}

/**
 * * Gestiunea Mesajelor WebSocket (Host -> Server)
 * */
function handleWebSocketMessage(hostWs, data) {
    if (typeof data === 'string' || data.length < HEADER_SIZE) return;

    const messageType = data.readUInt8(0);
    const guestId = data.toString('hex', 1, 7);
    
    const hostId = hostWs.hostId;

    // 1. Răspuns HTTP (MODUL NOU)
    if (messageType === MSG_TYPE_HTTP_RES) {
        handleHttpTunnelResponse(data);
        return;
    }
    
    // 2. Trafic de date TCP (MODUL VECHI)
    if (messageType === MSG_TYPE_DATA /* 0x02 */) {
        const payload = data.slice(HEADER_SIZE);
        const guestWs = guestWsMap.get(guestId);
        if (guestWs && guestWs.readyState === WebSocket.OPEN) {
            guestWs.send(payload);
        }
    }

    if (messageType === MSG_TYPE_CLOSE /* 0x03 */) {
        const guestWs = guestWsMap.get(guestId);
        if (guestWs) {
            guestWs.close();
            guestWsMap.delete(guestId);
            console.log(`[SERVER TCP] Guest ${guestId} închis de Host.`);
        }
    }
}

/**
 * * Handler comun pentru rutele HTTP tunelate.
 * * Folosim req.path pentru a obține calea rămasă (ex: /index.html)
 * */
function tunnelHandler(req, res) {
    const hostId = req.params.hostId;
    const hostWs = hostWsMap.get(hostId);

    if (!hostWs || hostWs.readyState !== WebSocket.OPEN) {
        return res.status(404).send(`Host ID "${hostId}" is not currently connected.`);
    }

    // --- Construiește path-ul corect pentru host ---
    // req.originalUrl include tot URL-ul cerut de browser (ex: /u/69210be4/api/hello?x=1)
    // Eliminăm prefixul '/u/:hostId' din orice request
    let path = req.originalUrl;

    // Asigură-te că eliminăm exact prefixul `/u/:hostId` la început
    const prefix = `/u/${hostId}`;
    if (path.startsWith(prefix)) {
        path = path.slice(prefix.length) || '/';
    }

    // Salvăm body-ul pentru request (funcționează pentru JSON și form-urlencoded)
    // Dacă req.body este obiect, îl convertim în string
    if (typeof req.body === 'object' && req.body !== null && !(req.body instanceof Buffer)) {
        req.body = Buffer.from(JSON.stringify(req.body));
    } else if (!req.body) {
        req.body = null;
    }

    // Salvăm res pentru răspuns
    req.res = res;

    console.log(`[TUNNEL] Host ${hostId} -> Path trimis: ${path}`);
    sendHttpTunnelRequest(hostWs, req);
}

/**
 * * Rute HTTP pentru tunelarea Web.
 * * Folosim app.use() care este mult mai robust pentru un reverse proxy (handlează 
 * * automat toate sub-căile după /u/:hostId) și evită erorile de PathError.
 * * Ex: /u/HOSTID/ sau /u/HOSTID/cale/sub-cale
 * */
// app.use('/u/:hostId/', express.raw({ type: '*/*' }), tunnelHandler);

// Middleware catch-all pentru /u/:hostId/ 
app.use('/u/:hostId/', (req, res) => {
    const hostId = req.params.hostId;
    const hostWs = hostWsMap.get(hostId);
    if (!hostWs || hostWs.readyState !== WebSocket.OPEN) {
        return res.status(404).send('Host not available');
    }

    // Citește corpul request-ului complet
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => {
        const bodyBuffer = Buffer.concat(chunks);

        const requestPayload = {
            method: req.method,
            url: req.originalUrl.replace(`/u/${hostId}`, '') || '/',
            headers: req.headers,
            body: bodyBuffer.length ? bodyBuffer.toString('base64') : null
        };

        const guestId = Math.random().toString(16).slice(2, 14);
        const requestId = Date.now() & 0xffffffff;

        const header = Buffer.alloc(11);
        header.writeUInt8(0x04, 0); // MSG_TYPE_HTTP_REQ
        header.write(guestId, 1, 6, 'hex');
        header.writeUInt32BE(requestId, 7);

        const message = Buffer.concat([header, Buffer.from(JSON.stringify(requestPayload), 'utf8')]);

        httpResponseMap.set(requestId, { res, hostId });

        hostWs.send(message, err => {
            if (err) {
                console.error(`[PROXY] Error sending request to host: ${err.message}`);
                res.status(500).send('Failed to send request to host');
                httpResponseMap.delete(requestId);
            }
        });
    });

    req.on('error', (err) => {
        console.error(`[PROXY] Request error: ${err.message}`);
        res.status(400).send('Bad Request');
    });
});


// app.use('/u/:hostId', tunnelHandler);

// app.use('/u/:hostId/',
//     express.json(),                  // parsează JSON
//     express.urlencoded({ extended: true }), // parsează x-www-form-urlencoded
//     tunnelHandler
// );


// Middleware (pentru orice altceva)
app.get('/', (req, res) => {
    res.status(200).send('Eclipse Open Worlds Central Server. Utilizați /host sau /guest/:id pentru WebSocket, sau /u/:id pentru tunelare HTTP.');
});

// --- Configurare WebSocket Server (WSS) ---
const wss = new WebSocket.Server({ noServer: true });

// --- Rularea Serverului ---
// Mutați inițializarea httpServer înainte de a-i atașa listeneri.
const httpServer = http.createServer(app);

httpServer.on('upgrade', (request, socket, head) => {
    if (request.headers['upgrade'] !== 'websocket') {
        socket.end('HTTP/1.1 400 Bad Request');
        return;
    }

    const url = new URL(request.url, `http://${request.headers.host}`);
    const parts = url.pathname.split('/');
    
    // RUTA HOST (fără ID în URL, alocă ID dinamic)
    if (url.pathname.toLowerCase() === '/host') {
        wss.handleUpgrade(request, socket, head, (hostWs) => {
            const hostId = Math.random().toString(16).substring(2, 10);
            hostWs.hostId = hostId;
            hostWsMap.set(hostId, hostWs);
            
            // Trimite ID-ul înapoi la Host
            const initMessage = Buffer.alloc(5);
            initMessage.writeUInt8(MSG_TYPE_INIT_HOST, 0);
            initMessage.write(hostId, 1, 4, 'hex');
            hostWs.send(initMessage);
            
            console.log(`\n[SERVER] Host nou conectat. ID: ${hostId}`);

            hostWs.on('message', (data) => handleWebSocketMessage(hostWs, data));
            hostWs.on('close', () => {
                hostWsMap.delete(hostId);
                console.log(`[SERVER] Host ${hostId} deconectat.`);
            });
            hostWs.on('error', (err) => { console.error(`[SERVER] Eroare Host ${hostId}: ${err.message}`); });
        });
        return;
    }
    
    // RUTA GUEST (TCP) (/guest/:hostId)
    if (parts[1] && parts[1].toLowerCase() === 'guest' && parts[2]) {
        const targetHostId = parts[2].toLowerCase();
        
        wss.handleUpgrade(request, socket, head, (guestWs) => {
            const hostWs = hostWsMap.get(targetHostId);
            
            if (!hostWs || hostWs.readyState !== WebSocket.OPEN) {
                guestWs.close(4067, `Host ID ${targetHostId} indisponibil.`);
                return;
            }

            let guestId = null;
            
            guestWs.on('message', (data) => {
                // Toate mesajele de la Guest trebuie să fie binare și să conțină Guest ID în antet
                if (typeof data === 'string' || data.length < HEADER_SIZE) return;
                
                const messageType = data.readUInt8(0);
                const currentGuestId = data.toString('hex', 1, 7);

                if (messageType === MSG_TYPE_NEW_GUEST /* 0x01 */) {
                    guestId = currentGuestId;
                    guestWsMap.set(guestId, guestWs);
                    
                    if (hostWs.readyState === WebSocket.OPEN) { hostWs.send(data); }
                    console.log(`[SERVER TCP] Guest ${guestId} s-a stocat pentru Host ${targetHostId}.`);

                } else if (messageType === MSG_TYPE_DATA /* 0x02 */ || messageType === MSG_TYPE_CLOSE /* 0x03 */) {
                    if (hostWs.readyState === WebSocket.OPEN) { hostWs.send(data); }
                }
            });

            guestWs.on('close', () => {
                if (guestId) {
                    // Când Guest se deconectează, notifică Host-ul (prin pachet CLOSE)
                    const closeHeader = Buffer.alloc(HEADER_SIZE);
                    closeHeader.writeUInt8(MSG_TYPE_CLOSE, 0);
                    closeHeader.write(guestId, 1, 6, 'hex');
                    if (hostWs && hostWs.readyState === WebSocket.OPEN) { hostWs.send(closeHeader); }
                    
                    guestWsMap.delete(guestId);
                    console.log(`[SERVER TCP] Guest ${guestId} deconectat.`);
                }
            });
            guestWs.on('error', (err) => { console.error(`[SERVER] Eroare Guest ${guestId}: ${err.message}`); guestWs.close(); });
        });
        return;
    }
    
    // Dacă nu este nici HTTP, nici o rută WS validă, închide socket-ul
    socket.end('HTTP/1.1 404 Not Found');
});

httpServer.listen(SERVER_PORT, () => {
    console.log(`Eclipse Open Worlds Infrastructure`);
    console.log(`Serverul Central rulează pe portul ${SERVER_PORT}`);
    console.log(`Ascultă Cereri HTTP pe rutele: /u/:hostId/`);
    console.log(`Ascultă Conexiuni WebSocket pe rutele: /host și /guest/:hostId`);
});
