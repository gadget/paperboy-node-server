const RABBIT_SERVER_URL = process.env.PAPERBOY_RABBIT_SERVER_URL || 'amqp://localhost';
var amqp = require('amqplib/callback_api');

class RabbitBackend {

  constructor() {
  }

  init() {
    const that = this;
    amqp.connect(RABBIT_SERVER_URL, function(error0, connection) {
      if (error0) {
        throw error0;
      }
      connection.createChannel(function(error1, channel) {
        if (error1) {
          throw error1;
        }
        channel.assertExchange('paperboy-subscription-request', 'fanout', { durable: false });
        channel.assertQueue('', { exclusive: true }, function(error2, q) {
          if (error2) {
            throw error2;
          }
        });
        that.channel = channel;
      });
    });
  }

  publishSubscriptionRequest(wsId, token) {
    var msg = {};
    msg.wsId = wsId;
    msg.token = token;
    this.channel.publish('paperboy-subscription-request', '', Buffer.from(JSON.stringify(msg)));
  }

  _subscribe(topic, callback) {
    const that = this;
    this.channel.assertExchange(topic, 'fanout', { durable: false });
    this.channel.assertQueue('', { exclusive: true }, function(error2, q) {
      if (error2) {
        throw error2;
      }
      console.log("Waiting for messages in %s.", q.queue);
      that.channel.bindQueue(q.queue, topic, '');
      channel.consume(q.queue, function(msg) {
        if (msg.content) {
          console.log("msg: %s", msg.content.toString());
          callback(msg.content.toString());
        }
      }, {
        noAck: true
      });
    });
  }

  subscribeAuthorized(callback) {
    _subscribe('paperboy-subscription-authorized', callback);
  }

  subscribeClose(callback) {
    _subscribe('paperboy-subscription-close', callback);
  }

  subscribeMessage(callback) {
    _subscribe('paperboy-message', callback);
  }

}

exports.RabbitBackend = RabbitBackend;
