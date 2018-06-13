'use strict'

var fs = require('fs')
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

var CA_BUNDLE_TEXT =
  VERIFY_CERTIFICATE_BUNDLE ? fs.readFileSync(
    VERIFY_CERTIFICATE_BUNDLE) : null

var addTlsOptions = function (options) {
  options = options || {}
  if (CA_BUNDLE_TEXT) {
    options.ca = CA_BUNDLE_TEXT
    options.rejectUnauthorized = true
  } else {
    options.rejectUnauthorized = false
  }
  return options
}

var channel = new Channel(CHANNEL_URL,
  CHANNEL_CONSUMER_GROUP,
  addTlsOptions({
    auth: new ChannelAuth(CHANNEL_URL, CHANNEL_USERNAME,
      CHANNEL_PASSWORD, addTlsOptions())
  })
)

channel.run(
  function (payloads) {
    console.log('Consumed payloads: ' +
      JSON.stringify(payloads, null, 4))
    return true
  },
  function (runError) {
    if (runError) {
      console.log('Run error, exiting: ' + runError.message)
    }
  },
  WAIT_BETWEEN_QUERIES,
  CHANNEL_TOPIC_SUBSCRIPTIONS
)
