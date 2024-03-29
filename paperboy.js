const { v4: uuidv4 } = require('uuid');
var WebSocket = require('ws');

const WEB_SOCKET_PORT = process.env.PAPERBOY_WEB_SOCKET_PORT || 3000;
const ALLOWED_ORIGINS = process.env.PAPERBOY_ALLOWED_ORIGINS || 'http://localhost:8080';
const MESSAGING_BACKEND = process.env.PAPERBOY_MESSAGING_BACKEND || 'embedded';
const EMBEDDED_BACKEND_PORT = process.env.PAPERBOY_EMBEDDED_BACKEND_PORT || '8060';
const EMBEDDED_BACKEND_TOKEN = process.env.PAPERBOY_EMBEDDED_BACKEND_TOKEN || 'secretToken';

// TODO: use WSS for secure/encrypted ws channels
const server = new WebSocket.Server({ port : WEB_SOCKET_PORT });

function createMessagingBackend() {
  switch (MESSAGING_BACKEND) {
    case 'embedded':
      var embedded = require('./embedded-backend')
      return new embedded.EmbeddedBackend(EMBEDDED_BACKEND_PORT, EMBEDDED_BACKEND_TOKEN);
    case 'redis':
      var redis = require('./redis-backend')
      return new redis.RedisBackend();
    case 'rabbit':
      var rabbit = require('./rabbit-backend')
      return new rabbit.RabbitBackend();
    default:
      throw 'Not implemented yet!';
  }
}

const messagingBackend = createMessagingBackend();
messagingBackend.init(function() {
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
    console.log('WebSocket client disconnected "%s".', ws.id);
  }

  function clearSubscription(userId, channel) {
    if (userSockets.has(userId)) {
      userSockets.get(userId).forEach((ws, idx) => {
        if (ws.channels != undefined && ws.channels.has(channel)) {
          ws.channels.delete(channel);
        }
        if (channelSockets.has(channel)) {
          channelSockets.get(channel).delete(ws);
        }
        console.log('Subscription "%s" closed.', ws.id);
      });
    }
  }

  // WebSocket connection handler
  server.on('connection', function connection(ws, req) {
    ws.isAlive = true;
    ws.authorized = false;
    ws.id = uuidv4();
    // 'heartbeat' (ping->pong to detect broken connections)
    ws.on('pong', heartbeat);

    const remoteAddress = req.socket.remoteAddress;
    const ipInHeader = req.headers['x-forwarded-for'] != undefined ? req.headers['x-forwarded-for'].split(/\s*,\s*/)[0] : '';
    console.log('WebSocket connection opened by client (remoteAddress: "%s", ipInHeader: "%s").', remoteAddress, ipInHeader);
    const origin = req.headers['origin'];
    // request origin validation
    if (ALLOWED_ORIGINS != '*' && !ALLOWED_ORIGINS.includes(origin)) {
      console.error('Origin header does not match, closing client connection "%s"!', ws.id);
      disconnect(ws);
    } else {
      // receives token from WebSocket client and sends it as a subscription request for the backend
      ws.on('message', function incoming(message) {
        if (message.length < 100 && message.startsWith('unsubscribe:')) {
          // handling unsubscribe request from client
          console.log('Unsubscribe request from WebSocket client.');
          const ch = message.substring(12);
          clearSubscription(ws.userId, ch);
        } else {
          // handling subscription token from client
          console.log('Token arrived from WebSocket client.');
          const token = message;
          sockets.set(ws.id, ws);
          messagingBackend.publishSubscriptionRequest(ws.id, token);
          console.log('Subscription request for "%s" was sent to backend.', ws.id);
        }
      });
    }
    setTimeout(function() {
      // if a subscription is not authorized by the backend within 5s the WebSocket connection is closed
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
        console.log('Cleaning up dead connection "%s".', ws.id);
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

  // subscribing for successful authorization messages, confirming an authorization/subscription to a channel
  messagingBackend.subscribeAuthorized(function(messageString) {
    const message = JSON.parse(messageString);
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
        // notifying the client about the successful subscription over WebSocket
        ws.send('subscribed:' + message.channel);
      }
      console.log('Successful authorization for "%s".', message.wsId);
    }
  });

  // subscribing for close messages (sent by the backend application logic to 'force' close a channel subscription)
  messagingBackend.subscribeClose(function(messageString) {
    const message = JSON.parse(messageString);
    console.log('Received message to close subscription for user "%s" on channel "%s".', message.userId, message.channel);
    clearSubscription(message.userId, message.channel);
  });

  // subscribing for application messages
  messagingBackend.subscribeMessage(function(messageString) {
    try {
      const message = JSON.parse(messageString);
      if (message.userId != undefined) {
        //console.log('Received user message, forwarding to WebSocket.');
        sendToUser(message.userId, messageString);
      } else if (message.channel != undefined) {
        //console.log('Received channel message, forwarding to WebSockets.');
        sendToChannel(message.channel, messageString);
      }
    } catch (e) {
      console.error(e);
    }
  });

  function sendToUser(userId, messageString) {
    if (userSockets.has(userId)) {
      for (let ws of userSockets.get(userId)) {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(messageString);
        }
      }
    }
  }

  function sendToChannel(channel, messageString) {
    if (channelSockets.has(channel)) {
      for (let ws of channelSockets.get(channel)) {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(messageString);
        }
      }
    }
  }

  console.log('Paperboy WebSocket server using "%s" backend started on port "%d".', MESSAGING_BACKEND, WEB_SOCKET_PORT);
});
