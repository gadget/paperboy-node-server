const bonjour = require('bonjour')();
const express = require('express');
const http = require('http');

let app = express();

class EmbeddedBackend {

  constructor(port) {
    this.port = port;
  }

  init(callback) {
    this.topicSubscriptions = new Map();
    this.messageCallbacks = new Map();
    this.nodes = [];
    let that = this;

    app.use(express.json());
    app.post('/subscribeTopic/:topic', function (req, res) {
      let caller = req.body;
      console.log('EmbeddedBackend: Topic subscription received for "%s" with callback "%s:%d:%s".', req.params.topic, caller.restHostname, caller.restPort, caller.restPath);
      if (!that.topicSubscriptions.has(req.params.topic)) {
        that.topicSubscriptions.set(req.params.topic, []);
      }
      that.topicSubscriptions.get(req.params.topic).push(caller)
      return res.send("done");
      // TODO: remove dead callers
    });

    app.post('/pushMessage/:topic', function (req, res) {
      let msg = req.body;
      console.log('EmbeddedBackend: Pushing message to "%s".', req.params.topic);
      if (that.topicSubscriptions.has(req.params.topic)) {
        let subscriptions = that.topicSubscriptions.get(req.params.topic);
        subscriptions.forEach((caller, i) => {
          console.log('EmbeddedBackend: Calling subscription callback "%s:%d:%s".', caller.restHostname, caller.restPort, caller.restPath);
          that._post(caller.restHostname, caller.restPort, caller.restPath, JSON.stringify(msg));
        });
      }
      return res.send("done");
    });

    app.post('/messageCallback/:topic', function (req, res) {
      let msg = req.body;
      console.log('EmbeddedBackend: Message callback received for "%s".', req.params.topic);
      if (that.messageCallbacks.has(req.params.topic)) {
        console.log('EmbeddedBackend: Executing callback handler.');
        that.messageCallbacks.get(req.params.topic)(JSON.stringify(msg));
      }
      return res.send("done");
    });

    http.createServer(app).listen(that.port, function() {
      console.log("EmbeddedBackend: server started.");
    });

    bonjour.publish({ name: 'Paperboy embedded backend #' + that.port, type: 'paperboy-http', port: that.port });
    bonjour.find({ type: 'paperboy-http' }, function (service) {
      var serviceObj = {};
      serviceObj.host = service.addresses[0];
      serviceObj.port = service.port;
      console.log('EmbeddedBackend: Discovered embedded backend at "%s:%d".', serviceObj.host, serviceObj.port);
      that.nodes.push(serviceObj);
    })
    callback();
  }

  publishSubscriptionRequest(wsId, token) {
    var msg = {};
    msg.wsId = wsId;
    msg.token = token;
    var that = this;

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
    this._post('localhost', this.port, '/subscribeTopic/paperboy-subscription-authorized', JSON.stringify(caller));
  }

  subscribeClose(callback) {
    this.messageCallbacks.set('paperboy-subscription-close', callback);
    var caller = {};
    caller.restHostname = 'localhost';
    caller.restPort = this.port;
    caller.restPath = '/messageCallback/paperboy-subscription-close';
    this._post('localhost', this.port, '/subscribeTopic/paperboy-subscription-close', JSON.stringify(caller));
  }

  subscribeMessage(callback) {
    this.messageCallbacks.set('paperboy-message', callback);
    var caller = {};
    caller.restHostname = 'localhost';
    caller.restPort = this.port;
    caller.restPath = '/messageCallback/paperboy-message';
    this._post('localhost', this.port, '/subscribeTopic/paperboy-message', JSON.stringify(caller));
  }

  _post(hostname, port, path, data) {
    var options = {
      hostname: hostname,
      port: port,
      path: path,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-length': data.length
      }
    };
    var req = http.request(options, function(res) {
      res.setEncoding('utf8');
      res.on('data', function (chunk) {
      });
    });
    req.on('error', error => {
      console.error(error);
    });
    req.write(data);
    req.end();
  }

}

exports.EmbeddedBackend = EmbeddedBackend;
