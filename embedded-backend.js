const bonjour = require('bonjour')();
const express = require('express');
const http = require('http');
const { v4: uuidv4 } = require('uuid');

let app = express();

class EmbeddedBackend {

  constructor(port, embeddedBackendToken) {
    this.port = port; // where the express server is running for REST calls
    this.embeddedBackendToken = embeddedBackendToken; // secret token for backend communication (should be the same in the connector)
  }

  init(callback) {
    this.topicSubscriptions = new Map(); // note: map of maps
    this.messageCallbacks = new Map();
    this.nodes = [];
    this.instanceId = uuidv4();
    let that = this;

    app.use(express.json());

    // instanceId to uniquely identify embedded backend services
    app.get('/instance', function (req, res) {
      res.send(that.instanceId);
    });

    // subscribe to a topic
    app.post('/subscribeTopic/:topic', function (req, res) {
      let caller = req.body;
      console.log('EmbeddedBackend: Topic subscription received for "%s" with callback "%s:%d%s".', req.params.topic, caller.restHostname, caller.restPort, caller.restPath);
      if (that.embeddedBackendToken != (req.get('PaperboyEmbeddedBackendToken'))) {
        throw 'Invalid token for embedded backend!';
      }
      // if this instance has no subscription for the given topic yet -> init with empty map (note: topicSubscriptions is a map of maps)
      if (!that.topicSubscriptions.has(req.params.topic)) {
        that.topicSubscriptions.set(req.params.topic, new Map());
      }
      let subscriptionsMap = that.topicSubscriptions.get(req.params.topic);
      let key = caller.restHostname + ":" + caller.restPort + caller.restPath;
      // save the callback to the subscription map
      subscriptionsMap.set(key, caller);
      return res.send("done");
      // TODO: remove dead callers
    });

    // sending a message to a topic
    app.post('/pushMessage/:topic', function (req, res) {
      let msg = req.body;
      console.log('EmbeddedBackend: Pushing message to "%s".', req.params.topic);
      if (that.embeddedBackendToken != (req.get('PaperboyEmbeddedBackendToken'))) {
        throw 'Invalid token for embedded backend!';
      }
      // if this instance has subscription for the given topic
      if (that.topicSubscriptions.has(req.params.topic)) {
        let subscriptions = that.topicSubscriptions.get(req.params.topic);
        // we iterate over all the subscription callers and execute the callback over REST
        subscriptions.forEach((caller) => {
          console.log('EmbeddedBackend: Calling subscription callback "%s:%d%s".', caller.restHostname, caller.restPort, caller.restPath);
          that._post(caller.restHostname, caller.restPort, caller.restPath, JSON.stringify(msg), function errorCallback(statusCode) {
            let key = caller.restHostname + ":" + caller.restPort + caller.restPath;
            console.log('EmbeddedBackend: Dead subscriber found "%s", removing!', key);
            subscriptions.delete(key);
          });
        });
      }
      return res.send("done");
    });

    // for calling a message callback on this instance
    app.post('/messageCallback/:topic', function (req, res) {
      let msg = req.body;
      console.log('EmbeddedBackend: Message callback received for "%s".', req.params.topic);
      if (that.embeddedBackendToken != (req.get('PaperboyEmbeddedBackendToken'))) {
        throw 'Invalid token for embedded backend!';
      }
      // if this instance has messageCallback registered for the given topic
      if (that.messageCallbacks.has(req.params.topic)) {
        console.log('EmbeddedBackend: Executing callback handler.');
        that.messageCallbacks.get(req.params.topic)(JSON.stringify(msg));
      }
      return res.send("done");
    });

    http.createServer(app).listen(that.port, function() {
      console.log("EmbeddedBackend: server started.");
    });

    // publishing this service with bonjour
    bonjour.publish({ name: 'Paperboy embedded backend #' + that.port, type: 'paperboy-http', port: that.port });
    // discovering all the embedded backend instances with bonjour
    bonjour.find({ type: 'paperboy-http' }, function (service) {
      var serviceObj = {};
      serviceObj.host = service.addresses[0];
      serviceObj.port = service.port;
      console.log('EmbeddedBackend: Discovered embedded backend at "%s:%d".', serviceObj.host, serviceObj.port);
      that.nodes.push(serviceObj);
      // TODO: remove dead instances
    })
    callback();
  }

  publishSubscriptionRequest(wsId, token) {
    var msg = {};
    msg.wsId = wsId;
    msg.token = token;
    var that = this;

    // writes are sent to all nodes
    this.nodes.forEach(function(n) {
      that._post(n.host, n.port, '/pushMessage/paperboy-subscription-request', JSON.stringify(msg));
    });
  }

  subscribeAuthorized(callback) {
    this.messageCallbacks.set('paperboy-subscription-authorized', callback);
    var caller = {};
    caller.restHostname = 'localhost';
    caller.restPort = this.port;
    caller.restPath = '/messageCallback/paperboy-subscription-authorized';
    // subscribing to topic, any new messages will be sent over REST using the messageCallback in caller
    this._post('localhost', this.port, '/subscribeTopic/paperboy-subscription-authorized', JSON.stringify(caller));
  }

  subscribeClose(callback) {
    this.messageCallbacks.set('paperboy-subscription-close', callback);
    var caller = {};
    caller.restHostname = 'localhost';
    caller.restPort = this.port;
    caller.restPath = '/messageCallback/paperboy-subscription-close';
    // subscribing to topic, any new messages will be sent over REST using the messageCallback in caller
    this._post('localhost', this.port, '/subscribeTopic/paperboy-subscription-close', JSON.stringify(caller));
  }

  subscribeMessage(callback) {
    this.messageCallbacks.set('paperboy-message', callback);
    var caller = {};
    caller.restHostname = 'localhost';
    caller.restPort = this.port;
    caller.restPath = '/messageCallback/paperboy-message';
    // subscribing to topic, any new messages will be sent over REST using the messageCallback in caller
    this._post('localhost', this.port, '/subscribeTopic/paperboy-message', JSON.stringify(caller));
  }

  _post(hostname, port, path, data, errorCallback) {
    var options = {
      hostname: hostname,
      port: port,
      path: path,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-length': data.length,
        'PaperboyEmbeddedBackendToken': this.embeddedBackendToken
      }
    };
    var req = http.request(options, function(res) {
      if (res.statusCode != 200) {
        console.error('EmbeddedBackend: unsuccessful service call to "%s:%d%s", response: "%d"', hostname, port, path, res.statusCode);
        if (errorCallback != undefined) {
          errorCallback(res.statusCode);
        }
      }
    });
    req.on('error', error => {
      console.error(error);
    });
    req.write(data);
    req.end();
  }

}

exports.EmbeddedBackend = EmbeddedBackend;
