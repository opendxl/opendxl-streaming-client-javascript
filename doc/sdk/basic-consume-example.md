This sample demonstrates how to establish a channel connection to the DXL
streaming service. Once the connection is established, the sample
repeatedly consumes and displays available records for the consumer group.

### Prerequisites

* A DXL streaming service is available for the sample to connect to.
* Credentials for a consumer are available for use with the sample.

### Setup

Modify the example to include the appropriate settings for the streaming
service channel:

```js
var CHANNEL_URL = 'http://127.0.0.1:50080'
var CHANNEL_USERNAME = 'me'
var CHANNEL_PASSWORD = 'secret'
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
```

For testing purposes, you can use the `fake_streaming_service` Python tool
embedded in the OpenDXL Streaming Client Python SDK to start up a local
streaming service which includes some fake data for a single preconfigured
consumer group. See the documentation for the
[Basic Consume Example](https://opendxl.github.io/opendxl-streaming-client-python/pydoc/basicconsumeexample.html#setup)
in the OpenDXL Streaming Client Python SDK for more information on the
`fake_streaming_service` tool. The initial settings in the example above include
the URL, credentials, and consumer group used by the `fake_streaming_service`.

To launch the `fake_streaming_service` tool, run the following command in
a command window:

```sh
$ python sample/fake_streaming_service.py
```

Messages like the following should appear in the command window:

```sh
INFO:__main__:Starting service
INFO:__main__:Started service on http://mycaseserver:50080
```

### Running

To run this sample execute the `sample/basic/basic-consume-example.js` script
as follows:

```sh
$ node sample/basic/basic-consume-example.js
```

As records are received by the sample, the contents of the message payloads
should be displayed to the output window. Using the `fake_streaming_service`,
for example, initial payloads similar to the following should appear:

```sh
Received payloads: [
    {
        "origin": "",
        "case": {
            "url": "https://mycaseserver.com/#/cases/9ab2cebb-6b5f-418b-a15f-df1a9ee213f2",
            "priority": "Low",
            "id": "9ab2cebb-6b5f-418b-a15f-df1a9ee213f2",
            "name": "A great case full of malware"
        },
        "user": "johndoe",
        "nature": "",
        "transaction-id": "",
        "timestamp": "",
        "tenant-id": "7af4746a-63be-45d8-9fb5-5f58bf909c25",
        "type": "creation",
        "id": "a45a03de-5c3d-452a-8a37-f68be954e784",
        "entity": "case"
    },
    {
        "origin": "",
        "case": {
            "url": "https://mycaseserver.com/#/cases/9ab2cebb-6b5f-418b-a15f-df1a9ee213f2",
            "priority": "Low",
            "id": "9ab2cebb-6b5f-418b-a15f-df1a9ee213f2",
            "name": "A great case full of malware"
        },
        "user": "other",
        "nature": "",
        "transaction-id": "",
        "timestamp": "",
        "tenant-id": "7af4746a-63be-45d8-9fb5-5f58bf909c25",
        "type": "priority-update",
        "id": "a45a03de-5c3d-452a-8a37-f68be954e784",
        "entity": "case"
    }
]
```

When no new records are available from the service, the sample should output
a line similar to the following:

```sh
Received payloads: []
```

### Details

The majority of the sample code is shown below:

```js
// Create a new channel object
var channel = new Channel(CHANNEL_URL,
  addTlsOptions({
    auth: new ChannelAuth(CHANNEL_URL, CHANNEL_USERNAME,
      CHANNEL_PASSWORD, addTlsOptions()),
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
  // The function below is called if the 'run' is stopped. If the 'run' is
  // stopped due to an error, the 'runError' object contains a 'Error' type
  // object.
  function (runError) {
    if (runError) {
      console.log('Run error, exiting: ' + runError.message)
      channel.destroy()
    }
  },
  WAIT_BETWEEN_QUERIES,
  CHANNEL_TOPIC_SUBSCRIPTIONS
)
```

The first step is to create a {@link Channel}
instance, which establishes a channel to the streaming service. The channel
includes the URL to the streaming service, `CHANNEL_URL`, and credentials
that the client uses to authenticate itself to the service, `CHANNEL_USERNAME`
and `CHANNEL_PASSWORD`.

The final step is to call the {@link Channel#run}
method. The `run` method establishes a consumer instance with the service,
subscribes the consumer instance for events delivered to the `topics`
included in the `CHANNEL_TOPIC_SUBSCRIPTIONS` variable, and continuously
polls the streaming service for available records.

The first parameter to the {@link Channel#run} method is a callback function
which is invoked with the payloads (an array of objects) extracted from records
consumed from the channel. The callback function outputs the contents of the
payloads parameter and returns `true` to indicate that the channel should
continue consuming records. Note that if the callback function were to instead
return `false`, the `run` method would stop polling the service for new records.
Note that if no records are received from a poll attempt, an empty array of
payloads is passed into the callback function.
