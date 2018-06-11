'use strict'

var request = require('request')
var retry = require('retry')
var Buffer = require('safe-buffer').Buffer
var util = require('./util')
var ConsumerError = require('./consumer-error')
var PermanentError = require('./permanent-error')
var StopError = require('./stop-error')
var TemporaryError = require('./temporary-error')

var DEFAULT_CHANNEL_TIMEOUT = 300
var DEFAULT_WAIT_BETWEEN_QUERIES = 30
var RETRY_FACTOR = 2
var MIN_RETRY_TIMEOUT = 1000
var MAX_RETRY_TIMEOUT = 10000

function callbackAsync (callback) {
  if (callback) {
    setImmediate(function () {
      callback(null)
    })
  }
}

function Channel (base, auth, consumerGroup, pathPrefix, offset, timeout,
                  retryOnFail, verify) {
  if (!base) {
    throw new PermanentError('Value must be specified for base')
  }
  this._base = base

  this._auth = auth

  if (!consumerGroup) {
    throw new PermanentError('Value must be specified for consumerGroup')
  }
  this._consumerGroup = consumerGroup

  offset = offset || 'latest'
  var offsetValues = ['latest', 'earliest', 'none']
  if (!offset || offsetValues.indexOf(offset) < 0) {
    throw new PermanentError('Value for \'offset\' must be one of ' +
      offsetValues.join(', '))
  }

  this._pathPrefix = '/databus/consumer-service/v1'
  if (typeof pathPrefix !== 'undefined' &&
    pathPrefix !== null && pathPrefix.length) {
    this._pathPrefix = pathPrefix
  }

  this._offset = offset
  this._timeout = timeout || DEFAULT_CHANNEL_TIMEOUT

  this.retryOnFail = (typeof retryOnFail === 'undefined') ? true : retryOnFail

  this._request = request.defaults(
    util.appendCertVerifyToOptions({
      baseUrl: util.appendUrlSubpath(this._base, this._pathPrefix),
      jar: request.jar()
    }, verify, PermanentError)
  )

  this._consumerId = null
  this._activeSubscriptions = []
  this._requestedSubscriptions = []
  this._recordsCommitLog = []

  this._active = true
  this._running = false
  this._stopRequested = false
  this._stopCallbacks = []
  this._runLoopFunc = null
  this._runLoopTimeout = null

  this._stillActive = function (callback) {
    if (!this._active && callback) {
      callback(new PermanentError('Channel has been destroyed'))
    }
    return this._active
  }

  this._sendRequestWithAuthInfo = function (sendFunc, options,
                                            successCallback, errorCallback,
                                            notFoundCallback) {
    var channel = this
    sendFunc(options, function (error, response) {
      if (error) {
        if (errorCallback) {
          errorCallback(error)
        }
      } else if ([200, 201, 202, 204].indexOf(response.statusCode) >= 0) {
        if (successCallback) {
          successCallback(response)
        }
      } else if ([401, 403].indexOf(response.statusCode) >= 0) {
        if (errorCallback) {
          errorCallback(new TemporaryError(
            'Token potentially expired (' + response.statusCode + '): ' +
            JSON.stringify(response.body)
          ))
        }
        if (channel._auth && channel._auth.reset) {
          channel._auth.reset()
        }
      } else if ((response.statusCode === 404) && notFoundCallback) {
        notFoundCallback(response)
      } else {
        if (errorCallback) {
          errorCallback(new TemporaryError(
            'Unexpected temporary error ' + response.statusCode + ': ' +
            JSON.stringify(response.body)
          ))
        }
      }
    })
  }

  this._sendRequest = function (sendFunc, options,
                                successCallback, errorCallback,
                                notFoundCallback) {
    if (typeof options === 'string') {
      options = {uri: options}
    }
    if (this._auth) {
      var channel = this
      this._auth.authenticate(
        options,
        function (error, optionsWithAuth) {
          if (error) {
            if (errorCallback) {
              errorCallback(error)
            }
          } else {
            channel._sendRequestWithAuthInfo(sendFunc, optionsWithAuth,
              successCallback, errorCallback, notFoundCallback)
          }
        }
      )
    } else {
      this._sendRequestWithAuthInfo(sendFunc, options,
        successCallback, errorCallback, notFoundCallback)
    }
  }

  this._retryOnFailure = function (operationCallback, completeCallback) {
    if (!operationCallback) {
      if (completeCallback) {
        callbackAsync(completeCallback)
      }
    } else if (this._stillActive(completeCallback)) {
      var operation = retry.operation({
        factor: RETRY_FACTOR,
        forever: true,
        minTimeout: MIN_RETRY_TIMEOUT,
        maxTimeout: MAX_RETRY_TIMEOUT
      })

      var channel = this
      operation.attempt(function () {
        if (channel._stillActive(completeCallback)) {
          if (channel._running && channel._stopRequested) {
            if (completeCallback) {
              completeCallback(new StopError())
            }
          } else {
            operationCallback(function (error, result) {
              if (error && (!(error instanceof ConsumerError)) &&
                channel.retryOnFail && operation.retry(error)) {
                console.log('Retrying due to: ' + error.message)
              } else {
                if (error) {
                  console.log('Will not retry due to: ' + error.message +
                    (channel.retryOnFail ? '' : ' (retries disabled)')
                  )
                }
                if (completeCallback) {
                  completeCallback(error, result)
                }
              }
            })
          }
        }
      })
    }
  }

  this._alreadySubscribed = function (topics) {
    var returnValue = true
    if (topics.length === this._activeSubscriptions.length) {
      for (var i = 0; i < topics.length; i++) {
        if (topics[i] !== this._activeSubscriptions[i]) {
          returnValue = false
          break
        }
      }
    } else {
      returnValue = false
    }
    return returnValue
  }

  this._subscribe = function (topics, callback) {
    this._requestedSubscriptions = topics
    var channel = this
    this._retryOnFailure(
      function (retryCallback) {
        var consumerId = channel._consumerId
        channel._sendRequest(
          channel._request.post,
          {
            uri: 'consumers/' + consumerId + '/subscription',
            json: true,
            body: {topics: topics}
          },
          function () {
            channel._activeSubscriptions = topics
            retryCallback(null)
          },
          retryCallback,
          function () {
            retryCallback(new ConsumerError("Consumer '" + consumerId +
              "' does not exist"
            ))
          }
        )
      },
      callback
    )
  }

  this._handleProcessCallbackResponse = function (
    continueRunning, doneCallback, waitBetweenQueries) {
    if (this._stopRequested) {
      continueRunning = false
    }
    var channel = this
    this.commit(function (commitError) {
      if (commitError || !continueRunning) {
        doneCallback(commitError, continueRunning)
      } else {
        channel._runLoopFunc = function () {
          channel._runLoopFunc = null
          channel._runLoopTimeout = null
          doneCallback(null, !channel._stopRequested)
        }
        channel._runLoopTimeout = setTimeout(channel._runLoopFunc,
          waitBetweenQueries * 1000
        )
      }
    })
  }

  this._consumeLoop = function (processCallback, doneCallback,
                                waitBetweenQueries) {
    var channel = this
    this.consume(function (consumeError, payloads) {
      if (consumeError) {
        doneCallback(consumeError, false)
      } else {
        try {
          var continueRunning = processCallback(payloads,
            function (processError, continueRunning) {
              if (processError) {
                doneCallback(processError, false)
              } else {
                channel._handleProcessCallbackResponse(continueRunning,
                  doneCallback, waitBetweenQueries)
              }
            })
          if (typeof continueRunning !== 'undefined') {
            channel._handleProcessCallbackResponse(
              continueRunning, doneCallback, waitBetweenQueries
            )
          }
        } catch (processError) {
          doneCallback(processError, false)
        }
      }
    })
  }

  this._handleRunError = function (error, runLoop, doneCallback) {
    if (error instanceof ConsumerError) {
      this.reset()
      runLoop()
    } else {
      this._running = false
      this._stopCallbacks.forEach(function (stopCallback) {
        try {
          stopCallback()
        } catch (stopError) {
          console.log('Error thrown from stop callback: ' + stopError)
        }
      })
      this._stopCallbacks = []
      this._stopRequested = false
      if (doneCallback) {
        if (error instanceof StopError) {
          error = null
        }
        doneCallback(error)
      }
    }
  }
}

Channel.prototype.reset = function () {
  this._consumerId = null
  this._activeSubscriptions = []
  this._requestedSubscriptions = []
  this._recordsCommitLog = []
}

Channel.prototype.create = function (callback) {
  this.reset()
  var channel = this
  this._retryOnFailure(
    function (retryCallback) {
      channel._sendRequest(
        channel._request.post,
        {
          uri: 'consumers',
          json: true,
          body: {
            consumerGroup: channel._consumerGroup,
            configs: {
              'session.timeout.ms': (channel._timeout * 1000).toString(),
              'enable.auto.commit': 'false', // this has to be false for now
              'auto.offset.reset': channel._offset
            }
          }
        },
        function (response) {
          var consumerInstanceId = response.body.consumerInstanceId
          if (consumerInstanceId) {
            channel._consumerId = consumerInstanceId
            retryCallback(null)
          } else {
            retryCallback(new PermanentError(
              'Unable to locate consumerInstanceId in create consumer response'
            ))
          }
        },
        retryCallback
      )
    },
    callback
  )
}

Channel.prototype.subscribe = function (topics, callback) {
  if (!topics) {
    throw new PermanentError('Value must be specified for topics')
  }
  if (typeof topics === 'string') {
    topics = [topics]
  } else if (!topics.length) {
    throw new PermanentError('At least one topic must be specified')
  }

  if (this._alreadySubscribed(topics)) {
    callbackAsync(callback)
  } else {
    if (this._consumerId) {
      this._subscribe(topics, callback)
    } else {
      var channel = this
      this.create(function (error) {
        if (error) {
          callback(error)
        } else {
          channel._subscribe(topics, callback)
        }
      })
    }
  }
}

Channel.prototype.consume = function (callback) {
  if (!this._activeSubscriptions.length) {
    throw new PermanentError('Channel is not subscribed to any topic')
  }

  var channel = this
  this._retryOnFailure(
    function (retryCallback) {
      var consumerId = channel._consumerId
      channel._sendRequest(
        channel._request.get,
        {
          uri: 'consumers/' + consumerId + '/records',
          json: true
        },
        function (response) {
          var payloads = []
          var records = response.body.records
          records.forEach(function (record) {
            channel._recordsCommitLog.push({
              topic: record.routingData.topic,
              partition: record.partition,
              offset: record.offset
            })
            payloads.push(JSON.parse(Buffer.from(record.message.payload,
              'base64')))
          })
          retryCallback(null, payloads)
        },
        retryCallback,
        function () {
          retryCallback(new ConsumerError("Consumer '" + consumerId +
            "' does not exist"
          ))
        }
      )
    },
    callback
  )
}

Channel.prototype.commit = function (callback) {
  if (this._recordsCommitLog.length) {
    var channel = this
    this._retryOnFailure(
      function (retryCallback) {
        var consumerId = channel._consumerId
        channel._sendRequest(
          channel._request.post,
          {
            uri: 'consumers/' + consumerId + '/offsets',
            json: true,
            body: {offsets: channel._recordsCommitLog}
          },
          function () {
            channel._recordsCommitLog = []
            retryCallback(null)
          },
          retryCallback,
          function () {
            retryCallback(new ConsumerError("Consumer '" + consumerId +
              "' does not exist"
            ))
          }
        )
      },
      callback
    )
  } else {
    callbackAsync(callback)
  }
}

Channel.prototype.run = function (processCallback, doneCallback,
                                  waitBetweenQueries, topics) {
  if (!processCallback) {
    throw new PermanentError('Value must be specified for processCallback')
  }
  if (this._running) {
    throw new PermanentError('Previous run already in progress')
  }

  waitBetweenQueries = waitBetweenQueries || DEFAULT_WAIT_BETWEEN_QUERIES

  if (typeof topics === 'string') {
    topics = [topics]
  }
  if (topics) {
    if (topics.length) {
      this._requestedSubscriptions = topics
    } else {
      throw new PermanentError('At least one topic must be specified')
    }
  } else if (!this._activeSubscriptions.length) {
    throw new PermanentError('Channel is not subscribed to any topic')
  }

  this._running = true
  var channel = this

  var doConsumeLoop = function (processCallback, doneCallback) {
    channel._consumeLoop(processCallback,
      function (consumeLoopError, continueRunning) {
        if (!consumeLoopError && continueRunning) {
          doConsumeLoop(processCallback, doneCallback)
        } else {
          if (doneCallback) {
            doneCallback(consumeLoopError)
          }
        }
      },
      waitBetweenQueries
    )
  }

  var currentSubscriptions = channel._requestedSubscriptions.slice()
  var doRun = function () {
    channel.subscribe(currentSubscriptions, function (subscribeError) {
      if (subscribeError) {
        channel._handleRunError(subscribeError, doRun, doneCallback)
      } else {
        doConsumeLoop(processCallback,
          function (consumeLoopError) {
            currentSubscriptions = channel._requestedSubscriptions.slice()
            channel._handleRunError(consumeLoopError, doRun, doneCallback)
          })
      }
    })
  }

  doRun()
}

Channel.prototype.stop = function (callback) {
  if (this._running) {
    this._stopRequested = true
    if (this._runLoopTimeout) {
      clearTimeout(this._runLoopTimeout)
      this._runLoopTimeout = null
      if (this._runLoopFunc) {
        callbackAsync(this._runLoopFunc)
        this._runLoopFunc = null
      }
    }
    if (callback) {
      this._stopCallbacks.push(callback)
    }
  } else {
    callbackAsync(callback)
  }
}

Channel.prototype.delete = function (callback) {
  var consumerId = this._consumerId
  if (consumerId) {
    var channel = this
    channel._sendRequest(
      channel._request.delete,
      'consumers/' + consumerId,
      function () {
        channel.reset()
        if (callback) {
          callback(null)
        }
      },
      callback,
      function () {
        channel.reset()
        if (callback) {
          callback(new ConsumerError("Consumer with ID '" +
            consumerId +
            "' not found. Resetting consumer anyways."))
        }
      }
    )
  } else {
    callbackAsync(callback)
  }
}

Channel.prototype.destroy = function (callback) {
  if (this._active) {
    var channel = this
    this.stop(function (stopError) {
      if (stopError) {
        callback(stopError)
      } else {
        channel.delete(function (deleteError) {
          if (deleteError) {
            callback(deleteError)
          } else {
            channel._active = false
            if (callback) {
              callback(null)
            }
          }
        })
      }
    })
  } else {
    callbackAsync(callback)
  }
}

module.exports = Channel
