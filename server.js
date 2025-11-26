const WebSocket = require('ws');
const http = require('http');

// Configurare
// Render va seta automat process.env.PORT, care este portul nostru public
const SERVER_PORT = process.env.PORT || 8080; 

// --- Constantele Protocolului Binar ---
const HEADER_SIZE = 7;
const MSG_TYPE_NEW_GUEST = 0x01;
const MSG_TYPE_DATA = 0x02;
const MSG_TYPE_CLOSE = 0x03;

// Harta conexiunilor Host (Host ID -> Obiect WebSocket)
const hostWsMap = new Map();

// --- 1. Funcția de Rutare a Traficului HTTP (Reverse Proxy) ---

function handleHttpRequest(req, res) {
    console.log(`[PROXY] Cerere HTTP primită: ${req.method} ${req.url}`);

    // Verifică dacă un Host (tu) este conectat
    if (hostWsMap.size === 0) {
        res.writeHead(503, { 'Content-Type': 'text/plain' });
        res.end('Eroare 503: Serverul țintă (Host) nu este conectat la tunel.');
        return;
    }

    // Luăm prima (și singura) conexiune de Host
    const [hostId, hostWs] = hostWsMap.entries().next().value;
    const guestId = Math.random().toString(16).substring(2, 8);
    
    // A. Reconstruirea Headerelor HTTP (pentru a simula clientul)
    const rawHeaders = [];
    rawHeaders.push(`${req.method} ${req.url} HTTP/${req.httpVersion}`);
    
    for (const key in req.headers) {
        // Excludem headerul 'connection' și 'upgrade' care nu sunt necesare serverului Express
        if (key.toLowerCase() !== 'connection' && key.toLowerCase() !== 'upgrade') {
            rawHeaders.push(`${key}: ${req.headers[key]}`);
        }
    }
    const headerString = rawHeaders.join('\r\n') + '\r\n\r\n';
    const headerBuffer = Buffer.from(headerString, 'utf8');

    // B. Colectarea Corpului Cererii (Chunks)
    const requestBodyChunks = [];
    req.on('data', (chunk) => {
        requestBodyChunks.push(chunk);
    });

    req.on('end', () => {
        // Aici se formează header-ul, corpul și httpBuffer (Cererea HTTP Brute)
        const bodyBuffer = Buffer.concat(requestBodyChunks);
        const httpBuffer = Buffer.concat([headerBuffer, bodyBuffer]);
    
        // B. Simularea Conexiunii Guest (Clientului)
        const shortGuestId = Math.random().toString(16).substring(2, 8); 
        
        // ATENȚIE: GENERĂM AICI CHEIA COMPLETĂ (12 CARACTERE)
        const tempHeader = Buffer.alloc(HEADER_SIZE);
        tempHeader.write(shortGuestId, 1, 6, 'hex'); // Scrie ID-ul scurt în Buffer (3 bytes)
        const mapKey = tempHeader.toString('hex', 1, 7); // Citește ID-ul complet de 6 bytes (12 caractere hex)
    
        // C. Trimiterea Mesajului NEW_GUEST 
        const newGuestHeader = Buffer.alloc(HEADER_SIZE);
        newGuestHeader.writeUInt8(MSG_TYPE_NEW_GUEST, 0);
        newGuestHeader.write(shortGuestId, 1, 6, 'hex'); 
    
        if (hostWs.readyState === WebSocket.OPEN) {
            hostWs.send(newGuestHeader);
        }
    
        // D. Trimiterea Pachetului de DATE 
        const dataHeader = Buffer.alloc(HEADER_SIZE);
        dataHeader.writeUInt8(MSG_TYPE_DATA, 0);
        dataHeader.write(shortGuestId, 1, 6, 'hex'); 
    
        const message = Buffer.concat([dataHeader, httpBuffer]);
        if (hostWs.readyState === WebSocket.OPEN) {
            hostWs.send(message);
            console.log(`[PROXY ${shortGuestId}] Am trimis Cererea HTTP (${httpBuffer.length} octeți) la Host.`);
        }
    
        // E. Așteptarea Răspunsului
        // ACUM STOCĂM CHEIA COMPLETĂ!
        hostWs._pendingResponses = hostWs._pendingResponses || new Map();
        hostWs._pendingResponses.set(mapKey, res); // Folosim mapKey (12 caractere)
    });
    
    // Asigură-te că gestionăm timeout-ul pentru a nu bloca conexiunea
    req.on('error', (err) => {
        console.error(`[PROXY ${guestId}] Eroare la cererea HTTP: ${err.message}`);
        res.end();
        // Nu ștergem guestId de aici, așteptăm un CLOSE de la Host sau un alt mecanism de timeout.
    });
}

// --- 2. Funcția de Rutare a Traficului WebSocket (Rutare Binară) ---

function handleWebSocketMessage(hostWs, data) {
    if (typeof data === 'string') {
        console.log(`[WS] Mesaj TEXT primit de la Host: ${data}. Ignorat.`);
        return;
    }

    if (data.length < HEADER_SIZE) {
        console.error('[WS] Pachet prea scurt, ignorat.');
        return;
    }

    const messageType = data.readUInt8(0);
    const guestId = data.toString('hex', 1, 7);

    // Verificăm dacă este un răspuns pentru o cerere HTTP deschisă (Reverse Proxy)
    if (hostWs._pendingResponses && hostWs._pendingResponses.has(guestId)) {
        const res = hostWs._pendingResponses.get(guestId);

        // Extragem Buffer-ul de date (Răspunsul HTTP de la Express)
        const httpResponseBuffer = data.slice(HEADER_SIZE);

        if (messageType === MSG_TYPE_DATA) {
            // F. Trimiterea Răspunsului înapoi la Browser
            res.write(httpResponseBuffer);
            console.log(`[PROXY ${guestId}] Primit ${httpResponseBuffer.length} octeți, trimiși la Browser.`);
        }

        if (messageType === MSG_TYPE_CLOSE) {
            // G. FINALIZAREA RĂSPUNSULUI (Rezolvă problema "loading forever")
            res.end();
            hostWs._pendingResponses.delete(guestId);
            console.log(`[PROXY ${guestId}] Socket închis de Host. Răspuns HTTP finalizat.`);
        }
    } else {
        console.log(`[WS] Mesaj pentru ID Guest necunoscut: ${guestId}. Ignorat.`);
    }
}

// --- 3. Inițializarea Serverului ---

const httpServer = http.createServer(handleHttpRequest);
const wss = new WebSocket.Server({ noServer: true });

// Ataşăm WebSocket Server la Serverul HTTP pentru evenimentul 'upgrade'
httpServer.on('upgrade', (request, socket, head) => {
    if (request.headers['upgrade'] !== 'websocket') {
        socket.end('HTTP/1.1 400 Bad Request');
        return;
    }

    wss.handleUpgrade(request, socket, head, (hostWs) => {
        const hostId = Math.random().toString(16).substring(2, 8);
        hostWs.hostId = hostId;
        hostWsMap.set(hostId, hostWs);
        hostWs._pendingResponses = new Map(); // Harta pentru răspunsurile HTTP deschise

        console.log(`\n[HOST ${hostId}] S-a conectat la Serverul Central. Gata de rutare.`);

        hostWs.on('message', (data) => handleWebSocketMessage(hostWs, data));

        hostWs.on('close', () => {
            console.log(`[HOST ${hostId}] Conexiunea închisă.`);
            hostWsMap.delete(hostId);
            // ATENȚIE: Dacă hostWs se închide, toate cererile HTTP rămase vor eșua
        });

        hostWs.on('error', (err) => {
            console.error(`[HOST ${hostId}] Eroare WebSocket: ${err.message}`);
        });
    });
});

// Pornirea Serverului
httpServer.listen(SERVER_PORT, () => {
    console.log(`Serverul Central (Render) rulează pe portul ${SERVER_PORT}`);
    console.log(`Așteaptă conexiuni WebSocket (Host) ȘI cereri HTTP (Browser).`);
});

