const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || 'http://localhost:3000').split(',').map(o => o.trim());

// Historial en memoria (últimos 20 mensajes)
const messageHistory = [];
const MAX_HISTORY = 20;

// Estado de clientes: Map<ws, { username: string, isAlive: boolean, lastMessageTime: number }>
const clients = new Map();

// Crear servidor HTTP para el endpoint /health y el Upgrade de WebSockets
const server = http.createServer((req, res) => {
  if (req.url === '/health' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', uptime: process.uptime() }));
  } else {
    res.writeHead(404);
    res.end();
  }
});

const wss = new WebSocketServer({ noServer: true });

// Validación de Origen en el Handshake (Upgrade)
server.on('upgrade', (request, socket, head) => {
  const origin = request.headers.origin;

  if (ALLOWED_ORIGINS.includes('*') || ALLOWED_ORIGINS.includes(origin)) {
    console.log(`[upgrade] Conexión permitida desde origen: ${origin}`);
    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit('connection', ws, request);
    });
  } else {
    console.log(`[cierre] Conexión rechazada por origen no permitido: ${origin}`);
    socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');
    socket.destroy();
  }
});

// Helper para emitir eventos a todos o a una selección
function broadcast(data) {
  const payload = JSON.stringify(data);
  for (const [clientWs] of clients) {
    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(payload);
    }
  }
}

function getConnectedUsers() {
  return Array.from(clients.values())
    .map(c => c.username)
    .filter(Boolean);
}

function broadcastUserList() {
  broadcast({
    type: 'user_list',
    users: getConnectedUsers()
  });
}

// Manejo de conexiones WebSocket
wss.on('connection', (ws) => {
  // Inicializamos metadata de la conexión
  clients.set(ws, { username: null, isAlive: true, lastMessageTime: 0 });

  ws.on('pong', () => {
    const clientData = clients.get(ws);
    if (clientData) clientData.isAlive = true;
  });

  ws.on('message', (rawData) => {
    let parsed;
    try {
      parsed = JSON.parse(rawData.toString());
    } catch (e) {
      return; // Ignorar paquetes malformados
    }

    const clientData = clients.get(ws);
    if (!clientData) return;

    // 1. REGISTRO DE USUARIO
    if (parsed.type === 'join') {
      const username = (parsed.username || '').trim();
      if (!username || username.length > 20) {
        ws.send(JSON.stringify({ type: 'error', message: 'Nombre inválido (1-20 caracteres).' }));
        return;
      }

      // Confirmación de nombre
      clientData.username = username;
      ws.send(JSON.stringify({ type: 'join_ack', username }));

      // Enviar historial reciente
      ws.send(JSON.stringify({ type: 'history', messages: messageHistory }));

      // Notificar a todos sobre la entrada
      broadcast({
        type: 'system',
        text: `${username} se ha unido al chat.`
      });

      // Actualizar lista global de usuarios
      broadcastUserList();
      return;
    }

    // A partir de aquí requiere estar registrado
    if (!clientData.username) {
      ws.send(JSON.stringify({ type: 'error', message: 'Debes registrar un nombre primero.' }));
      return;
    }

    // 2. EVENTO "ESTÁ ESCRIBIENDO"
    if (parsed.type === 'typing') {
      broadcast({
        type: 'typing',
        username: clientData.username,
        isTyping: !!parsed.isTyping
      });
      return;
    }

    // 3. MENSAJE DE CHAT
    if (parsed.type === 'message') {
      const text = (parsed.text || '').trim();

      // Validación de mensaje vacío o larguísimo
      if (!text || text.length > 500) {
        ws.send(JSON.stringify({ type: 'error', message: 'El mensaje debe tener entre 1 y 500 caracteres.' }));
        return;
      }

      // Límite de ritmo (Rate Limiting: máx 1 mensaje cada 500ms)
      const now = Date.now();
      if (now - clientData.lastMessageTime < 500) {
        ws.send(JSON.stringify({ type: 'error', message: 'Estás enviando mensajes demasiado rápido.' }));
        return;
      }
      clientData.lastMessageTime = now;

      const msgObj = {
        type: 'message',
        username: clientData.username,
        text: text,
        timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
      };

      // Guardar en historial
      messageHistory.push(msgObj);
      if (messageHistory.length > MAX_HISTORY) messageHistory.shift();

      // Difundir mensaje a todos
      broadcast(msgObj);
    }
  });

  ws.on('close', (code, reason) => {
    const clientData = clients.get(ws);
    console.log(`[cierre] Conexión cerrada. Código: ${code}, Razón: ${reason || 'Sin razón'}`);

    if (clientData && clientData.username) {
      const username = clientData.username;
      clients.delete(ws);

      // Notificar salida
      broadcast({
        type: 'system',
        text: `${username} ha salido del chat.`
      });

      // Actualizar lista de conectados
      broadcastUserList();
    } else {
      clients.delete(ws);
    }
  });

  ws.on('error', (err) => {
    console.error('Error en WebSocket:', err.message);
  });
});

// Ping/Pong para detectar y cortar conexiones muertas
const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    const clientData = clients.get(ws);
    if (!clientData) return;

    if (clientData.isAlive === false) {
      console.log(`[cierre] Terminando conexión inactiva de: ${clientData.username || 'Anónimo'}`);
      return ws.terminate();
    }

    clientData.isAlive = false;
    ws.ping();
  });
}, 30000); // Se verifica cada 30 segundos

wss.on('close', () => {
  clearInterval(interval);
});

server.listen(PORT, () => {
  console.log(`Servidor de Chat listo en puerto ${PORT}`);
  console.log(`ALLOWED_ORIGINS configurados: ${ALLOWED_ORIGINS.join(', ')}`);
});