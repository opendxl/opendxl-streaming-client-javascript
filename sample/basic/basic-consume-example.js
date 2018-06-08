'use strict'

var common = require('../common')
var client = common.requireClient()
var Channel = client.Channel
var ChannelAuth = client.ChannelAuth

var CHANNEL_URL = 'http://127.0.0.1:50080'
var CHANNEL_USERNAME = 'me'
var CHANNEL_PASSWORD = 'secret'
var CHANNEL_CONSUMER_GROUP = 'sample_consumer_group'
var CHANNEL_TOPIC_SUBSCRIPTIONS = ['case-mgmt-events']
var VERIFY_CERTIFICATE_BUNDLE = ''

var WAIT_BETWEEN_QUERIES = 5

var channel = new Channel(CHANNEL_URL,
  new ChannelAuth(CHANNEL_URL, CHANNEL_USERNAME,
    CHANNEL_PASSWORD, VERIFY_CERTIFICATE_BUNDLE),
  CHANNEL_CONSUMER_GROUP, null, null, null, true,
  VERIFY_CERTIFICATE_BUNDLE)

var run = function () {
  channel.create(function (createError) {
    if (createError) {
      console.log('Create error: ' + createError.message)
    } else {
      channel.subscribe(CHANNEL_TOPIC_SUBSCRIPTIONS,
        function (subscriptionError) {
          if (subscriptionError) {
            console.log('Subscription error: ' + subscriptionError.message)
          } else {
            channel.run(
              function (payloads) {
                console.log('Consumed payloads: ' +
                  JSON.stringify(payloads, null, 4))
                return true
              },
              function (runError) {
                if (runError) {
                  console.log('Run error, exiting: ' + runError.message)
                } else {
                  run()
                }
              },
              WAIT_BETWEEN_QUERIES
            )
          }
        }
      )
    }
  })
}

run()
