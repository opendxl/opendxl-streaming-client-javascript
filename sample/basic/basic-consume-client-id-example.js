'use strict'

var fs = require('fs')
var common = require('../common')
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
var CHANNEL_CONSUMER_GROUP = 'sample_consumer_group'
var CHANNEL_TOPIC_SUBSCRIPTIONS = [
  'case-mgmt-events',
  'my-topic',
  'topic-abc123']

// Path to a CA bundle file containing certificates of trusted CAs. The CA
// bundle is used to validate that the certificate of the server being connected
// to was signed by a valid authority. If set to an empty string, the server
// certificate is not validated.
var VERIFY_CERTIFICATE_BUNDLE = ''

// This constant controls the frequency (in seconds) at which the channel 'run'
// call below polls the streaming service for new records.
var WAIT_BETWEEN_QUERIES = 5

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

// Create a new channel object
var channel = new Channel(CHANNEL_URL,
  addTlsOptions({
    auth: new ChannelToken(CHANNEL_URL, CHANNEL_CLIENT_ID,
      CHANNEL_CLIENT_SECRET,
      CHANNEL_SCOPE,
      CHANNEL_GRANT_TYPE,
      CHANNEL_AUDIENCE,
      addTlsOptions()),
    consumerGroup: CHANNEL_CONSUMER_GROUP
  })
)

// Consume records indefinitely
channel.run(
  // The function below is called back upon by the 'run' method when
  // records are received from the channel.
  function (payloads) {
    // Print the payloads which were received. 'payloads' is an array of
    // objects extracted from the records received from the channel.
    console.log('Received payloads: ' + JSON.stringify(payloads, null, 4))
    // Return 'True' in order for the 'run' call to continue attempting to
    // consume records.
    return true
  },
  {
    doneCallback:
      // The function below is called if the 'run' is stopped. If the 'run' is
      // stopped due to an error, the 'runError' object contains a 'Error' type
      // object.
      function (runError) {
        if (runError) {
          console.log('Run error, exiting: ' + runError.message)
          channel.destroy()
        }
      },
    waitBetweenQueries: WAIT_BETWEEN_QUERIES,
    topics: CHANNEL_TOPIC_SUBSCRIPTIONS
  }
)
