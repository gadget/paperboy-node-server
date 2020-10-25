var WebSocket = require('ws');
var redis = require('redis');
var uuid = require('node-uuid');

// Configuration: adapt to your environment
const REDIS_SERVER = "redis://localhost:6379";
const WEB_SOCKET_PORT = 3000;

const authorizedSubscriber = redis.createClient(REDIS_SERVER);
const messageSubscriber = redis.createClient(REDIS_SERVER);
const publisher = redis.createClient(REDIS_SERVER);

// TODO: use WSS for secure/encrypted ws channels
const server = new WebSocket.Server({ port : WEB_SOCKET_PORT });

var socketsPreAuth = new Map();
var userSockets = new Map();
var channelSockets = new Map();

// Register event for client connection
server.on('connection', function connection(ws, req) {
  console.log('client ws connection opened');
  // TODO: check origin header to avoid hijacking
  ws.on('message', function incoming(message) {
    console.log('incoming ws message');
    console.log('ws.id:' + ws.id); // TODO: make sure ws has no id field yet
    ws.id = uuid.v4();
    const token = message;
    const ip = req.socket.remoteAddress;
    //const ip = req.headers['x-forwarded-for'].split(/\s*,\s*/)[0];
    socketsPreAuth.set(ws.id, ws);
    var msg = {};
    msg.wsId = ws.id;
    msg.token = token;
    publisher.publish('paperboy-connection-request', JSON.stringify(msg)); // TODO: json
    console.log('connection request was sent to backend');
    console.log('token:' + token);
    // TODO: set timeout, authorized message has to arrive whitin or otherwise disconnect the websocket
  });

});

authorizedSubscriber.on('message', function(channel, messageString) {
  console.log('incoming auth msg from redis');
  console.log(messageString);
  const message = JSON.parse(messageString);
  const ws = socketsPreAuth.get(message.wsId);
  if (ws != undefined) {
    // TODO: ws is null if request was handled on another node, ignore
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
  console.log('incoming msg from redis');
  console.log(messageString);
  const message = JSON.parse(messageString);
  if (message.userId != undefined) {
    console.log('sending to user ' + message.userId);
    sendToUser(message.userId, message);
  } else if (message.channel != undefined) {
    console.log('sending to channel ' + message.channel);
    sendToChannel(message.channel, message);
  }
});

function sendToUser(userId, message) {
  // TODO: sanitize payload
  if (userSockets.has(userId)) {
    userSockets.get(userId).send(JSON.stringify(message));
  }
}

function sendToChannel(channel, message) {
  // TODO: sanitize payload
  if (channelSockets.has(channel)) {
    channelSockets.get(channel).forEach(function (ws, index) {
      ws.send(JSON.stringify(message));
    });
  }
}

authorizedSubscriber.subscribe('paperboy-connection-authorized');
messageSubscriber.subscribe('paperboy-message');

console.log("Paperboy WebSocket server started at ws://locahost:"+ WEB_SOCKET_PORT);
