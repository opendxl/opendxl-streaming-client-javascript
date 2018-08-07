This sample demonstrates how to produce records to the DXL streaming service.

### Prerequisites

* A DXL streaming service is available for the sample to connect to.
* Credentials for the service available for use with the sample.

### Setup

Modify the example to include the appropriate settings for the streaming
service channel:

```js
// Change these below to match the appropriate details for your
// channel connection.
var CHANNEL_URL = 'http://127.0.0.1:50080'
var CHANNEL_USERNAME = 'me'
var CHANNEL_PASSWORD = 'secret'
var CHANNEL_TOPIC = 'my-topic'
// Path to a CA bundle file containing certificates of trusted CAs. The CA
// bundle is used to validate that the certificate of the server being connected
// to was signed by a valid authority. If set to an empty string, the server
// certificate is not validated.
var VERIFY_CERTIFICATE_BUNDLE = ''
```

For testing purposes, you can use the `fake_streaming_service` Python tool
embedded in the OpenDXL Streaming Client SDK to start up a local streaming
service. See the documentation for the
[Basic Produce Example](https://opendxl.github.io/opendxl-streaming-client-python/pydoc/basicproduceexample.html#setup)
in the OpenDXL Streaming Client Python SDK for more information on the
`fake_streaming_service` tool. The initial settings in the example above include
the URL and credentials used by the `fake_streaming_service`.

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

To run this sample execute the `sample/basic/basic-produce-example.js` script
as follows:

```sh
$ node sample/basic/basic-produce-example.js
```

If the records are successfully produced to the streaming service, the
following line should appear in the output window:

```sh
Succeeded.
```

To validate that the records were produced to the streaming service with
the expected content, you can execute the
``sample/basic/basic-consume-example.js`` script as follows:

```sh
$ node sample/basic/basic-consume-example.js
```

One of the records received by the sample should appear similar to the
following:

```sh
Received payloads: [
    {
        "message": "Hello from OpenDXL"
    }
]
```

### Details

The majority of the sample code is shown below:

```js
var CHANNEL_TOPIC = 'my-topic'

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
    auth: new ChannelAuth(CHANNEL_URL, CHANNEL_USERNAME,
      CHANNEL_PASSWORD, addTlsOptions())
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
```

The first step is to create a payload object which includes an array of
records to be sent to the channel. The `message.payload` item in each record
is flattened from a dictionary into a string and encoded using the `base64`
algorithm.

The next step is to create a {@link Channel} instance, which establishes a
channel to the streaming service. The channel parameters include the URL to the
streaming service, `CHANNEL_URL`, and credentials that the client uses to
authenticate itself to the service, `CHANNEL_USERNAME` and `CHANNEL_PASSWORD`.

The final step is to call the {@link Channel#produce} method with the payload of
records to be produced to the channel. Assuming the records can be produced
successfully, the text "Succeeded." should appear in the console output.
