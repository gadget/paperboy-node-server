var WebSocket = require('ws');
var redis = require('redis');
var uuid = require('node-uuid');

const REDIS_SERVER_URL = process.env.PAPERBOY_REDIS_SERVER_URL || 'redis://localhost:6379';
const WEB_SOCKET_PORT = process.env.PAPERBOY_WEB_SOCKET_PORT || 3000;
const ALLOWED_ORIGINS = process.env.PAPERBOY_ALLOWED_ORIGINS || "http://localhost:8080";

const authorizedSubscriber = redis.createClient(REDIS_SERVER_URL);
const closeSubscriber = redis.createClient(REDIS_SERVER_URL);
const messageSubscriber = redis.createClient(REDIS_SERVER_URL);
const publisher = redis.createClient(REDIS_SERVER_URL);

// TODO: use WSS for secure/encrypted ws channels
const server = new WebSocket.Server({ port : WEB_SOCKET_PORT });

var sockets = new Map();        // map of (uuid    -> WebSocket object)
var userSockets = new Map();    // map of (userId  -> set of WebSocket objects)
var channelSockets = new Map(); // map of (channel -> set of WebSocket objects)

function noop() {
}

function heartbeat() {
  this.isAlive = true;
}

// closes the given WebSocket connection and maintains internal states used for tracking connections
function disconnect(ws) {
  ws.terminate();
  ws.isAlive = false;
  sockets.delete(ws.id);
  if (ws.userId != undefined) {
    if (userSockets.has(ws.userId)) {
      userSockets.get(ws.userId).delete(ws);
    }
  }
  if (ws.channels != undefined) {
    for (let ch of ws.channels) {
      if (channelSockets.has(ch)) {
        channelSockets.get(ch).delete(ws);
      }
    }
  }
}

// WebSocket connection handler
server.on('connection', function connection(ws, req) {
  ws.isAlive = true;
  ws.authorized = false;
  ws.id = uuid.v4();
  // 'heartbeat' (ping->pong to detect broken connections)
  ws.on('pong', heartbeat);

  const remoteAddress = req.socket.remoteAddress;
  const ipInHeader = req.headers['x-forwarded-for'] != undefined ? req.headers['x-forwarded-for'].split(/\s*,\s*/)[0] : '';
  console.log('WebSocket connection opened by client (remoteAddress: "%s", ipInHeader: "%s").', remoteAddress, ipInHeader);
  const origin = req.headers['origin'];
  // request origin validation
  if (!ALLOWED_ORIGINS.includes(origin)) {
    console.error('Origin header does not match, closing client connection!');
    disconnect(ws);
  } else {
    // receives token from WebSocket client and sends it as a subscription request for the backend
    ws.on('message', function incoming(message) {
      console.log('Token arrived from WebSocket client.');
      const token = message;
      sockets.set(ws.id, ws);
      var msg = {};
      msg.wsId = ws.id;
      msg.token = token;
      publisher.publish('paperboy-subscription-request', JSON.stringify(msg));
      console.log('Subscription request for "%s" was sent to backend.', msg.wsId);
    });
  }
  setTimeout(function() {
    // if the subscription is not authorized by the backend within 5s the WebSocket connection is closed
    // (application messages are not delivered until a successful authorization, not even in that 5s!)
    if (!ws.authorized) {
      console.error('Subscription for "%s" was not authorized whitin timeout, closing client connection!', ws.id);
      disconnect(ws);
    }
  }, 5000);
});

const cleanUpDeadConnectionsInterval = setInterval(function ping() {
  for (let ws of sockets.values()) {
    if (ws.isAlive === false) {
      console.debug('Cleaning up dead connection "%s".', ws.id);
      disconnect(ws);
    } else {
      ws.isAlive = false;
      ws.ping(noop);
    }
  }
}, 10000);

server.on('close', function close() {
  clearInterval(cleanUpDeadConnectionsInterval);
});

// a kind of ACK from the backend, confirming an authorization/subscription to a channel
authorizedSubscriber.on('message', function(channel, messageString) {
  const message = JSON.parse(messageString);
  console.log('Successful authorization for "%s".', message.wsId);
  if (sockets.has(message.wsId)) {
    const ws = sockets.get(message.wsId);
    ws.authorized = true;
    ws.userId = message.userId;
    // registering the WebSocket connection by owning user
    if (!userSockets.has(message.userId)) {
      userSockets.set(message.userId, new Set());
    }
    userSockets.get(message.userId).add(ws);
    if (message.channel != undefined) {
      // registering channel subscription for the WebSocket connection
      if (ws.channels === undefined) {
        ws.channels = new Set();
      }
      ws.channels.add(message.channel);
      // registering the WebSocket connection by channel
      if (!channelSockets.has(message.channel)) {
        channelSockets.set(message.channel, new Set());
      }
      channelSockets.get(message.channel).add(ws);
      ws.send('subscribed:' + message.channel);
    }
  }
});

// close messages are sent by the backend application logic to 'force' close channel subscription
closeSubscriber.on('message', function(channel, messageString) {
  const message = JSON.parse(messageString);
  console.debug('Received message to close subscription for user "%s" on channel "%s".', message.userId, message.channel);
  if (userSockets.has(message.userId)) {
    userSockets.get(message.userId).forEach((ws, idx) => {
      if (ws.channels != undefined && ws.channels.has(message.channel)) {
        ws.channels.delete(message.channel);
      }
      if (channelSockets.has(message.channel)) {
        channelSockets.get(message.channel).delete(ws);
      }
      console.log('Subscription "%s" closed.', ws.id);
    });
  }
});

// application messages
messageSubscriber.on('message', function(channel, messageString) {
  try {
    const message = JSON.parse(messageString);
    if (message.userId != undefined) {
      console.debug('Received user message, forwarding to WebSocket.');
      sendToUser(message.userId, message);
    } else if (message.channel != undefined) {
      console.debug('Received channel message, forwarding to WebSockets.');
      sendToChannel(message.channel, message);
    }
  } catch (e) {
    console.error(e);
  }
});

function sendToUser(userId, message) {
  if (userSockets.has(userId)) {
    for (let ws of userSockets.get(userId)) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(message));
      }
    }
  }
}

function sendToChannel(channel, message) {
  if (channelSockets.has(channel)) {
    for (let ws of channelSockets.get(channel)) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(message));
      }
    }
  }
}

authorizedSubscriber.subscribe('paperboy-subscription-authorized');
closeSubscriber.subscribe('paperboy-subscription-close');
messageSubscriber.subscribe('paperboy-message');

console.log('Paperboy WebSocket server started on port "%d".', WEB_SOCKET_PORT);
