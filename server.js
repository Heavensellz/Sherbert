const http = require('http');
const WebSocket = require('ws');

const MAX_SLOTS_PER_GROUP = 4;
const VALID_SLOTS = ['account1', 'account2', 'account3', 'account4'];
const HEARTBEAT_INTERVAL_MS = 5000; // now checks every 5 seconds

// groups: Map<groupPassword, Map<slot, ws>>
const groups = new Map();

const server = http.createServer((req, res) => {
    if (req.url === '/' || req.url === '/alive') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('WebSocket server is alive and running.\n');
        return;
    }

    if (req.url === '/clients') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(getAllGroupsSnapshot(), null, 2));
        return;
    }

    res.writeHead(404);
    res.end('Not Found');
});

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
    console.log('✅ Client connected');

    ws.isAlive = true;
    ws.clientId = generateClientId();
    ws.clientName = null;
    ws.clientSlot = null;
    ws.clientGroup = null;

    ws.on('pong', () => { ws.isAlive = true; });

    ws.send(JSON.stringify({
        type: 'connected',
        message: 'Please register with your name, slot, and group password',
        clientId: ws.clientId
    }));

    ws.on('message', (message) => {
        const raw = typeof message === 'string' ? message : message.toString();

        let data;
        try {
            data = JSON.parse(raw);
        } catch {
            ws.send(JSON.stringify({ type: 'error', message: 'Invalid JSON' }));
            return;
        }

        switch (data.type) {
            case 'register': {
                handleRegister(ws, data);
                break;
            }

            case 'groupBroadcast': {
                handleGroupBroadcast(ws, data);
                break;
            }

            case 'groupMessage': {
                handleGroupMessage(ws, data);
                break;
            }

            case 'getClients': {
                sendClientList(ws);
                break;
            }

            case 'pong': {
                // Application-level heartbeat reply (see setInterval below).
                // Useful for clients whose WebSocket implementation doesn't
                // transparently answer protocol-level ping/pong frames
                // (e.g. some Roblox WebSocket libraries).
                ws.isAlive = true;
                break;
            }

            default: {
                ws.send(JSON.stringify({ type: 'error', message: `Unknown type: ${data.type}` }));
            }
        }
    });

    ws.on('close', () => {
        console.log(`❌ Disconnected: ${ws.clientName || 'Anonymous'} (group: ${ws.clientGroup}, slot: ${ws.clientSlot})`);
        removeFromGroup(ws);
        broadcastClientList(ws.clientGroup);
    });
});

// ---------------- Registration ----------------

function handleRegister(ws, data) {
    const { name, slot, group } = data;

    if (!name || !slot || !group) {
        ws.send(JSON.stringify({ type: 'error', message: 'Missing name, slot, or group' }));
        return;
    }

    if (!VALID_SLOTS.includes(slot)) {
        ws.send(JSON.stringify({
            type: 'error',
            message: `Invalid slot. Must be one of: ${VALID_SLOTS.join(', ')}`
        }));
        return;
    }

    if (ws.clientGroup) {
        removeFromGroup(ws);
    }

    if (!groups.has(group)) {
        groups.set(group, new Map());
        console.log(`🆕 Group created: ${group}`);
    }
    const groupMap = groups.get(group);

    // Reject if slot already taken by a live connection
    const existing = groupMap.get(slot);
    if (existing && existing.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
            type: 'error',
            message: `Slot "${slot}" is already taken in this group`
        }));
        ws.close();
        return;
    }

    // Reject if group is full
    if (groupMap.size >= MAX_SLOTS_PER_GROUP && !groupMap.has(slot)) {
        ws.send(JSON.stringify({
            type: 'error',
            message: 'This group already has 4 accounts registered'
        }));
        ws.close();
        return;
    }

    ws.clientName = name;
    ws.clientSlot = slot;
    ws.clientGroup = group;
    groupMap.set(slot, ws);

    console.log(`👤 Registered: ${name} | group: ${group} | slot: ${slot}`);

    ws.send(JSON.stringify({
        type: 'registered',
        name: ws.clientName,
        slot: ws.clientSlot,
        group: ws.clientGroup,
        clientId: ws.clientId
    }));

    broadcastClientList(group);
}

function removeFromGroup(ws) {
    if (!ws.clientGroup) return;
    const groupMap = groups.get(ws.clientGroup);
    if (!groupMap) return;

    if (groupMap.get(ws.clientSlot) === ws) {
        groupMap.delete(ws.clientSlot);
    }
    if (groupMap.size === 0) {
        groups.delete(ws.clientGroup);
        console.log(`🗑️ Group emptied and removed: ${ws.clientGroup}`);
    }
}

// ---------------- Messaging ----------------

// Broadcast an event to every client in the same group
function handleGroupBroadcast(ws, data) {
    if (!ws.clientGroup) {
        ws.send(JSON.stringify({ type: 'error', message: 'Not registered to a group' }));
        return;
    }
    if (!data.event) {
        ws.send(JSON.stringify({ type: 'error', message: 'Missing event field' }));
        return;
    }

    console.log(`📢 Group broadcast [${ws.clientGroup}] from ${ws.clientName}: ${data.event}`);
    broadcastToGroup(ws.clientGroup, {
        type: 'groupBroadcast',
        from: ws.clientSlot,
        event: data.event,
        payload: data.payload || null
    });
}

// Send a message to a specific slot within the same group
function handleGroupMessage(ws, data) {
    if (!ws.clientGroup) {
        ws.send(JSON.stringify({ type: 'error', message: 'Not registered to a group' }));
        return;
    }
    if (!data.target || !data.event) {
        ws.send(JSON.stringify({ type: 'error', message: 'Missing target or event field' }));
        return;
    }

    const groupMap = groups.get(ws.clientGroup);
    if (!groupMap) return;

    const target = groupMap.get(data.target);
    if (!target || target.readyState !== WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'error', message: `Slot "${data.target}" is not connected` }));
        return;
    }

    console.log(`💬 Group message [${ws.clientGroup}] ${ws.clientSlot} → ${data.target}: ${data.event}`);
    target.send(JSON.stringify({
        type: 'groupMessage',
        from: ws.clientSlot,
        event: data.event,
        payload: data.payload || null
    }));
}

function broadcastToGroup(group, payload) {
    const groupMap = groups.get(group);
    if (!groupMap) return;
    const msg = JSON.stringify(payload);
    groupMap.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(msg); });
}

// ---------------- Client list helpers ----------------

function getGroupClientList(group) {
    const groupMap = groups.get(group);
    if (!groupMap) return [];
    return Array.from(groupMap.values())
        .filter(c => c.readyState === WebSocket.OPEN)
        .map(c => ({ name: c.clientName, id: c.clientId, slot: c.clientSlot }));
}

function getAllGroupsSnapshot() {
    const snapshot = {};
    for (const [group, groupMap] of groups.entries()) {
        snapshot[group] = Array.from(groupMap.values())
            .filter(c => c.readyState === WebSocket.OPEN)
            .map(c => ({ name: c.clientName, id: c.clientId, slot: c.clientSlot }));
    }
    return snapshot;
}

function sendClientList(ws) {
    if (ws.readyState !== WebSocket.OPEN) return;
    if (!ws.clientGroup) {
        ws.send(JSON.stringify({ type: 'clientList', clients: [] }));
        return;
    }
    ws.send(JSON.stringify({ type: 'clientList', clients: getGroupClientList(ws.clientGroup) }));
}

function broadcastClientList(group) {
    if (!group) return;
    const groupMap = groups.get(group);
    if (!groupMap) return;
    const msg = JSON.stringify({ type: 'clientList', clients: getGroupClientList(group) });
    groupMap.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(msg); });
}

// ---------------- Misc ----------------

function generateClientId() {
    return Date.now().toString(36) + Math.random().toString(36).substr(2);
}

// Every HEARTBEAT_INTERVAL_MS, ping every connected client.
// If a client didn't respond with a pong since the last check, it's
// considered dead: we terminate it, and let the 'close' handler (above)
// do the actual group cleanup + broadcast so there's exactly one code
// path responsible for removing a client from its group.
setInterval(() => {
    wss.clients.forEach((ws) => {
        if (!ws.isAlive) {
            console.log(`⚠️ No heartbeat response — terminating: ${ws.clientName || 'Anonymous'} (group: ${ws.clientGroup}, slot: ${ws.clientSlot})`);
            ws.terminate(); // triggers 'close', which handles removeFromGroup + broadcastClientList
            return;
        }
        ws.isAlive = false;
        ws.ping(); // protocol-level ping (handled automatically by browsers / the `ws` lib)
        ws.send(JSON.stringify({ type: 'ping' })); // app-level ping (client must reply { type: 'pong' })
    });
}, HEARTBEAT_INTERVAL_MS);

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
    console.log(`🚀 WebSocket server running on port ${PORT}`);
});
