'use strict'

var fs = require('fs')
var common = require('../common')
var Buffer = common.require('safe-buffer').Buffer
var client = common.require('@opendxl/dxl-streaming-client')
var Channel = client.Channel
var ChannelAuth = client.ChannelAuth

// Change these below to match the appropriate details for your
// channel connection.
var CHANNEL_URL = 'http://127.0.0.1:50080'
var CHANNEL_CLIENT_ID = "me"
var CHANNEL_CLIENT_SECRET = "secret"
var CHANNEL_SCOPE=""
var CHANNEL_GRANT_TYPE=""
var CHANNEL_AUDIENCE=""
var CHANNEL_TOPIC = 'my-topic'
// Path to a CA bundle file containing certificates of trusted CAs. The CA
// bundle is used to validate that the certificate of the server being connected
// to was signed by a valid authority. If set to an empty string, the server
// certificate is not validated.
var VERIFY_CERTIFICATE_BUNDLE = ''

// Read the contents of the CA bundle file into a string if one was specified
// above for the VERIFY_CERTIFICATE_BUNDLE constant.
var CA_BUNDLE_TEXT =
  VERIFY_CERTIFICATE_BUNDLE ? fs.readFileSync(
    VERIFY_CERTIFICATE_BUNDLE) : null

// Add TLS-related options to the supplied options object. This is used to
// supply the CA bundle file content for server verification.
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

// Create the message payload to be included in a record
var messagePayload = {
  message: 'Hello from OpenDXL'
}

// Create the full payload with records to produce to the channel
var channelPayload = {
  records: [
    {
      routingData: {
        topic: CHANNEL_TOPIC,
        shardingKey: ''
      },
      message: {
        headers: {},
        // Convert the message payload from an object to a base64-encoded
        // string.
        payload: Buffer.from(JSON.stringify(messagePayload)).toString('base64')
      }
    }
  ]
}

// Create a new channel object
var channel = new Channel(CHANNEL_URL,
  addTlsOptions({
    auth: new ChannelToken(CHANNEL_URL, CHANNEL_CLIENT_ID,
      CHANNEL_CLIENT_SECRET,
      CHANNEL_SCOPE,
      CHANNEL_GRANT_TYPE,
      CHANNEL_AUDIENCE, addTlsOptions())
  })
)

// Produce the payload records to the channel
channel.produce(
  channelPayload,
  function (error) {
    channel.destroy()
    if (error) {
      console.log('Error : ' + error)
    } else {
      console.log('Succeeded.')
    }
  }
)
