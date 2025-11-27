const WebSocket = require('ws');
const http = require('http');
const { URL } = require('url'); 

// --- Configurare ---
const SERVER_PORT = process.env.PORT || 8080;

// --- Constante Protocol Binar ---
const MSG_TYPE_INIT_HOST = 0x00; // NOU: Pachet de Inițializare pentru Host
const HEADER_SIZE = 7; 
// 0x01 = NEW_GUEST, 0x02 = DATA, 0x03 = CLOSE

const hostWsMap = new Map();
const guestWsMap = new Map(); 

function handleWebSocketMessage(hostWs, data) {
    if (typeof data === 'string' || data.length < HEADER_SIZE) return; 

    const messageType = data.readUInt8(0);
    const guestId = data.toString('hex', 1, 7); 
    const payload = data.slice(HEADER_SIZE);
    
    const hostId = hostWs.hostId;

    if (messageType === 0x02 /* DATA */) {
        const guestWs = guestWsMap.get(guestId); 
        if (guestWs && guestWs.readyState === WebSocket.OPEN) {
             guestWs.send(payload); 
        }
    }

    if (messageType === 0x03 /* CLOSE */) {
        const guestWs = guestWsMap.get(guestId); 
        if (guestWs) {
             guestWs.close();
             guestWsMap.delete(guestId);
        }
    }
}

const httpServer = http.createServer((req, res) => {
    res.writeHead(400, { 'Content-Type': 'text/plain' });
    res.end('Serverul Central: Acceptă doar upgrade-uri WebSocket.');
});
const wss = new WebSocket.Server({ noServer: true });

httpServer.on('upgrade', (request, socket, head) => {
    
    if (request.headers['upgrade'] !== 'websocket') {
        socket.end('HTTP/1.1 400 Bad Request');
        return;
    }

    const url = new URL(request.url, `http://${request.headers.host}`);
    
    const parts = url.pathname.split('/');
    const targetHostId = parts[2] ? parts[2].toLowerCase() : null; 

    // RUTA GUEST (/guest/:hostId)
    if (parts[1] && parts[1].toLowerCase() === 'guest' && targetHostId) {
        wss.handleUpgrade(request, socket, head, (guestWs) => {
            const hostWs = hostWsMap.get(targetHostId); 

            if (!hostWs || hostWs.readyState !== WebSocket.OPEN) {
                guestWs.close(4067, `Host ID ${targetHostId} indisponibil.`);
                return;
            }

            let guestId = null;

            guestWs.on('message', (data) => {
                if (typeof data === 'string' || data.length < HEADER_SIZE) return; 
                
                const messageType = data.readUInt8(0);
                const currentGuestId = data.toString('hex', 1, 7); 

                if (messageType === 0x01 /* NEW_GUEST */) {
                    guestId = currentGuestId;
                    guestWsMap.set(guestId, guestWs);
                    
                    if (hostWs.readyState === WebSocket.OPEN) {
                         hostWs.send(data); 
                    }
                    console.log(`[SERVER] Guest ${guestId} s-a stocat pentru Host ${targetHostId}.`);

                } else if (messageType === 0x02 /* DATA */) {
                    if (hostWs.readyState === WebSocket.OPEN) {
                         hostWs.send(data); 
                    }
                }
            });

            guestWs.on('close', () => {
                if (guestId) {
                    guestWsMap.delete(guestId);
                }
            });
            
            guestWs.on('error', (err) => { console.error(`[SERVER] Eroare Guest ${guestId}: ${err.message}`); guestWs.close(); });
        });
        
    } else if (url.pathname.toLowerCase() === '/host') {
        // RUTA HOST (fără ID în URL, alocă ID dinamic)
        wss.handleUpgrade(request, socket, head, (hostWs) => {
            // Generare Host ID (8 caractere hex = 4 bytes)
            const hostId = Math.random().toString(16).substring(2, 10); 
            hostWs.hostId = hostId;
            hostWsMap.set(hostId, hostWs); 

            // Trimite ID-ul înapoi la Host (pachet INIT_HOST: 1 byte Type + 4 bytes ID)
            const initMessage = Buffer.alloc(5); 
            initMessage.writeUInt8(MSG_TYPE_INIT_HOST, 0);
            initMessage.write(hostId, 1, 4, 'hex'); 
            hostWs.send(initMessage);
            
            console.log(`\n[SERVER] Host nou conectat. I-am alocat ID: ${hostId}`);

            hostWs.on('message', (data) => handleWebSocketMessage(hostWs, data));
            hostWs.on('close', () => hostWsMap.delete(hostId));
            hostWs.on('error', (err) => { console.error(`[SERVER] Eroare Host ${hostId}: ${err.message}`); });
        });
    } else {
        socket.end('HTTP/1.1 404 Not Found');
    }
});

httpServer.listen(SERVER_PORT, () => {
    console.log(`Eclipse Open Worlds Infrastructure \n`);
    console.log(`Serverul Central rulează pe portul ${SERVER_PORT}`);
});
