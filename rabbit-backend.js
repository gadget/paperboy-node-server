const RABBIT_SERVER_URL = process.env.PAPERBOY_RABBIT_SERVER_URL || 'amqp://localhost:5672';
var amqp = require('amqplib/callback_api');

class RabbitBackend {

  constructor() {
  }

  init(callback) {
    const that = this;
    amqp.connect(RABBIT_SERVER_URL, function(error0, connection) {
      if (error0) throw error0;
      connection.createChannel(function(error1, channel) {
        if (error1) throw error1;
        channel.assertQueue('paperboy-subscription-request');
        that.channel = channel;
        callback();
      });
    });
  }

  publishSubscriptionRequest(wsId, token) {
    var msg = {};
    msg.wsId = wsId;
    msg.token = token;
    this.channel.sendToQueue('paperboy-subscription-request', Buffer.from(JSON.stringify(msg)));
  }

  _subscribe(topic, callback) {
    const that = this;
    this.channel.assertExchange(topic, 'fanout', { durable: false });
    this.channel.assertQueue('', { exclusive: true }, function(error, q) {
      if (error) throw error;
      that.channel.bindQueue(q.queue, topic, '');
      that.channel.consume(q.queue, function(msg) {
        if (msg.content) {
          callback(msg.content.toString());
        }
      }, {
        noAck: true
      });
    });
  }

  subscribeAuthorized(callback) {
    this._subscribe('paperboy-subscription-authorized', callback);
  }

  subscribeClose(callback) {
    this._subscribe('paperboy-subscription-close', callback);
  }

  subscribeMessage(callback) {
    this._subscribe('paperboy-message', callback);
  }

}

exports.RabbitBackend = RabbitBackend;
