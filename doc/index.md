## Overview

The OpenDXL Streaming JavaScript client library is used to consume records
from and produce records to a
[Data Exchange Layer](http://www.mcafee.com/us/solutions/data-exchange-layer.aspx)
(DXL) Streaming Service.

The DXL Streaming Service exposes a REST-based API that communicates with a
back-end streaming platform (Kafka, Kinesis, etc.). The streaming service
performs authentication and authorization and exposes methods to retrieve and
produce records.

One concrete example of a DXL Streaming Service is the
[McAfee Investigator](https://www.mcafee.com/enterprise/en-us/products/investigator.html)
"Events feed".

## Installation

* {@tutorial installation}

## Samples

* [Samples Overview]{@tutorial samples}
  * {@tutorial basic-consume-example}
  * {@tutorial basic-produce-example}

## JavaScript API

* {@link Channel}
* {@link ChannelAuth}
