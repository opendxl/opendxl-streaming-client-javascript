'use strict'

var fs = require('fs')
var Buffer = require('safe-buffer').Buffer
var common = require('../common')
var client = common.requireClient()
var Channel = client.Channel
var ChannelAuth = client.ChannelAuth

var CHANNEL_URL = 'http://127.0.0.1:50080'
var CHANNEL_USERNAME = 'me'
var CHANNEL_PASSWORD = 'secret'
var CHANNEL_TOPIC = 'my-topic'
var VERIFY_CERTIFICATE_BUNDLE = ''

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

var messagePayload = {
  message: 'Hello from OpenDXL'
}

var channelPayload = {
  records: [
    {
      routingData: {
        topic: CHANNEL_TOPIC,
        shardingKey: ''
      },
      message: {
        headers: {},
        payload: Buffer.from(JSON.stringify(messagePayload)).toString('base64')
      }
    }
  ]
}

var channel = new Channel(CHANNEL_URL,
  addTlsOptions({
    auth: new ChannelAuth(CHANNEL_URL, CHANNEL_USERNAME,
      CHANNEL_PASSWORD, addTlsOptions())
  })
)

channel.produce(
  channelPayload,
  function (error) {
    if (error) {
      console.log('Error : ' + error)
    } else {
      console.log('Succeeded.')
    }
  }
)
