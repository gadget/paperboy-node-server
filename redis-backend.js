const REDIS_SERVER_URL = process.env.PAPERBOY_REDIS_SERVER_URL || 'redis://localhost:6379';
var redis = require('redis');

class RedisBackend {

  constructor() {
  }

  init() {
    this.authorizedSubscriber = redis.createClient(REDIS_SERVER_URL);
    this.closeSubscriber = redis.createClient(REDIS_SERVER_URL);
    this.messageSubscriber = redis.createClient(REDIS_SERVER_URL);
    this.publisher = redis.createClient(REDIS_SERVER_URL);
  }

  publishSubscriptionRequest(wsId, token) {
    var msg = {};
    msg.wsId = wsId;
    msg.token = token;
    this.publisher.rpush('paperboy-subscription-request', JSON.stringify(msg));
  }

  subscribeAuthorized(callback) {
    this.authorizedSubscriber.on('message', function(channel, messageString) {
      callback(messageString);
    });
    this.authorizedSubscriber.subscribe('paperboy-subscription-authorized');
  }

  subscribeClose(callback) {
    this.closeSubscriber.on('message', function(channel, messageString) {
      callback(messageString);
    });
    this.closeSubscriber.subscribe('paperboy-subscription-close');
  }

  subscribeMessage(callback) {
    this.messageSubscriber.on('message', function(channel, messageString) {
      callback(messageString);
    });
    this.messageSubscriber.subscribe('paperboy-message');
  }

}

exports.RedisBackend = RedisBackend;
