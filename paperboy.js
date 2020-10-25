var WebSocket = require('ws');
var redis = require('redis');
var uuid = require('node-uuid');

const REDIS_SERVER_URL = process.env.PAPERBOY_REDIS_SERVER_URL || 'redis://localhost:6379';
const WEB_SOCKET_PORT = process.env.PAPERBOY_WEB_SOCKET_PORT || 3000;
const ALLOWED_ORIGIN = process.env.PAPERBOY_ALLOWED_ORIGIN || "http://localhost:8080";

const authorizedSubscriber = redis.createClient(REDIS_SERVER_URL);
const messageSubscriber = redis.createClient(REDIS_SERVER_URL);
const publisher = redis.createClient(REDIS_SERVER_URL);

// TODO: use WSS for secure/encrypted ws channels
const server = new WebSocket.Server({ port : WEB_SOCKET_PORT });

var sockets = new Map();
var userSockets = new Map();
var channelSockets = new Map();

function noop() {
}

function heartbeat() {
  this.isAlive = true;
}

server.on('connection', function connection(ws, req) {
  ws.isAlive = true;
  ws.authorized = false;
  ws.id = uuid.v4();
  ws.on('pong', heartbeat);

  const remoteAddress = req.socket.remoteAddress;
  const ipInHeader = req.headers['x-forwarded-for'] != undefined ? req.headers['x-forwarded-for'].split(/\s*,\s*/)[0] : '';
  console.log('WebSocket connection opened by client (remoteAddress: "%s", ipInHeader: "%s").', remoteAddress, ipInHeader);
  const origin = req.headers['origin'];
  if (origin != ALLOWED_ORIGIN) {
    console.error('Origin header does not match, closing client connection!');
    ws.terminate();
    ws.isAlive = false;
  } else {
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
    setTimeout(function() {
      if (!ws.authorized) {
        console.error('Subscription was not authorized whitin timeout, closing client connection!');
        ws.terminate();
        ws.isAlive = false;
        sockets.delete(ws.id);
      }
    }, 5000);
  }
});

const cleanUpDeadConnectionsInterval = setInterval(function ping() {
  for (let ws of sockets.values()) {
    if (ws.isAlive === false) {
      console.debug('Cleaning up dead connection "%s".', ws.id);
      ws.terminate();
      sockets.delete(ws.id);
      if (ws.userId != undefined) {
        userSockets.delete(ws.userId);
      }
      if (ws.channel != undefined) {
        if (channelSockets.has(ws.channel)) {
          channelSockets.get(ws.channel).delete(ws);
        }
      }
    } else {
      ws.isAlive = false;
      ws.ping(noop);
    }
  }
}, 10000);

server.on('close', function close() {
  clearInterval(cleanUpDeadConnectionsInterval);
});

authorizedSubscriber.on('message', function(channel, messageString) {
  const message = JSON.parse(messageString);
  console.log('Successful authorization for "%s".', message.wsId);
  if (sockets.has(message.wsId)) {
    const ws = sockets.get(message.wsId);
    ws.authorized = true;
    ws.userId = message.userId;
    ws.channel = message.channel;
    userSockets.set(message.userId, ws);
    if (message.channel != undefined) {
      if (!channelSockets.has(message.channel)) {
        channelSockets.set(message.channel, new Set());
      }
      channelSockets.get(message.channel).add(ws);
    }
  }
});

messageSubscriber.on('message', function(channel, messageString) {
  const message = JSON.parse(messageString);
  if (message.userId != undefined) {
    console.debug('Received user message, forwarding to WebSocket.');
    sendToUser(message.userId, message);
  } else if (message.channel != undefined) {
    console.debug('Received channel message, forwarding to WebSockets.');
    sendToChannel(message.channel, message);
  }
});

function sendToUser(userId, message) {
  if (userSockets.has(userId)) {
    userSockets.get(userId).send(JSON.stringify(message));
  }
}

function sendToChannel(channel, message) {
  if (channelSockets.has(channel)) {
    for (let ws of channelSockets.get(channel)) {
      ws.send(JSON.stringify(message));
    }
  }
}

authorizedSubscriber.subscribe('paperboy-subscription-authorized');
messageSubscriber.subscribe('paperboy-message');

console.log('Paperboy WebSocket server started on port "%d".', WEB_SOCKET_PORT);
