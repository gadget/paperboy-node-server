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

var socketsPreAuth = new Map();
var userSockets = new Map();
var channelSockets = new Map();

server.on('connection', function connection(ws, req) {
  const remoteAddress = req.socket.remoteAddress;
  const ipInHeader = req.headers['x-forwarded-for'] != undefined ? req.headers['x-forwarded-for'].split(/\s*,\s*/)[0] : '';
  const origin = req.headers['origin'];
  if (origin != ALLOWED_ORIGIN) {
    console.error('Origin header does not match, closing client connection!');
    ws.close();
  } else {
    console.log('WebSocket connection opened by client (remoteAddress: "%s", ipInHeader: "%s").', remoteAddress, ipInHeader);
    ws.on('message', function incoming(message) {
      console.log('Token arrived from WebSocket client.');
      ws.id = uuid.v4();
      const token = message;
      socketsPreAuth.set(ws.id, ws);
      var msg = {};
      msg.wsId = ws.id;
      msg.token = token;
      publisher.publish('paperboy-subscription-request', JSON.stringify(msg));
      console.log('Subscription request for "%s" was sent to backend.', msg.wsId);
      // TODO: set timeout, authorized message has to arrive whitin or otherwise disconnect the websocket
    });
  }
});

authorizedSubscriber.on('message', function(channel, messageString) {
  const message = JSON.parse(messageString);
  console.log('Successful authorization for "%s".', message.wsId);
  if (socketsPreAuth.has(message.wsId)) {
    const ws = socketsPreAuth.get(message.wsId);
    userSockets.set(message.userId, ws);
    if (message.channel != undefined) {
      if (!channelSockets.has(message.channel)) {
        channelSockets.set(message.channel, []);
      }
      channelSockets.get(message.channel).push(ws);
      // TODO: remove dead ws sockets from maps
    }
    socketsPreAuth.delete(message.wsId);
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
    channelSockets.get(channel).forEach(function (ws, index) {
      ws.send(JSON.stringify(message));
    });
  }
}

authorizedSubscriber.subscribe('paperboy-subscription-authorized');
messageSubscriber.subscribe('paperboy-message');

console.log('Paperboy WebSocket server started on port "%d".', WEB_SOCKET_PORT);
