'use strict'

var request = require('request')
var retry = require('retry')
var Buffer = require('safe-buffer').Buffer
var util = require('./util')
var ConsumerError = require('./consumer-error')
var PermanentError = require('./permanent-error')
var StopError = require('./stop-error')
var TemporaryError = require('./temporary-error')

var DEFAULT_CONSUMER_PATH_PREFIX = '/databus/consumer-service/v1'
var DEFAULT_PRODUCER_PATH_PREFIX = '/databus/cloudproxy/v1'

// Constans for consumer config settings
var AUTO_OFFSET_RESET_CONFIG_SETTING = 'auto.offset.reset'
var ENABLE_AUTO_COMMIT_CONFIG_SETTING = 'enable.auto.commit'
var REQUEST_TIMEOUT_CONFIG_SETTING = 'request.timeout.ms'
var SESSION_TIMEOUT_CONFIG_SETTING = 'session.timeout.ms'

// Default number of seconds to wait between consume queries made to the
// streaming service
var DEFAULT_WAIT_BETWEEN_QUERIES = 30

var RETRY_FACTOR = 2
var MIN_RETRY_TIMEOUT = 1000
var MAX_RETRY_TIMEOUT = 10000

/**
 * Asynchronously invoke the supplied callback.
 * @param {Function} callback - Function to invoke.
 * @private
 */
function callbackAsync (callback) {
  if (callback) {
    setImmediate(function () {
      callback(null)
    })
  }
}

/**
 * @classdesc The `Channel` class is responsible for all communication with the
 * streaming service.
 *
 * The example below demonstrates the creation of a `Channel` instance and
 * creating a consumer for the consumer group.
 * @example
 * // Create the channel
 * var channel = new Channel('http://channel-server',
 *   {
 *     auth: new ChannelAuth('http://channel-server', 'user', 'password'),
 *     consumerGroup: 'thegroup'
 *   })
 *
 * // Create a new consumer on the consumer group
 * channel.create()
 * @param {String} base - Base URL at which the streaming service resides.
 * @param {Object} [options] - Options to use for the channel.
 * @param {BaseChannelAuth} [options.auth] - Authentication object to use for
 *   channel requests.
 * @param {String} [options.consumerGroup] - Consumer group to subscribe the
 *   channel consumer to.
 * @param {String} [options.pathPrefix] - Path to append to streaming service
 *   requests.
 * @param {String} [options.consumerPathPrefix=/databus/consumer-service/v1] -
 *   Path to append to consumer-related requests made to the streaming service.
 *   Note that if `options.pathPrefix` is set to a non-empty value, the
 *   `options.pathPrefix` value will be appended to consumer-related requests
 *   instead of the `options.consumerPathPrefix` value.
 * @param {String} [options.producerPathPrefix=/databus/cloudproxy/v1] - Path
 *   to append to producer-related requests made to the streaming service. Note
 *   that if the `options.pathPrefix` parameter is set to a non-empty value, the
 *   `options.pathPrefix` value will be appended to producer-related requests
 *   instead of the `options.producerPathPrefix` value.
 * @param {String} [options.offset=latest] - Offset for the next record to
 *   retrieve from the streaming service for the new {@link Channel#consume}
 *   call. Must be one of 'latest', 'earliest', or 'none'.
 * @param {Number} [options.requestTimeout] - The configuration controls the
 *   maximum amount of time the client (consumer) will wait for the broker
 *   response of a request. If the response is not received before the
 *   request timeout elapses the client may resend the request or fail
 *   the request if retries are exhausted. If set to `null` or `undefined` (the
 *   default), the request timeout is determined automatically by the streaming
 *   service. Note that if a value is set for the request
 *   timeout, the value should exceed the `options.sessionTimeout`. Otherwise,
 *   the streaming service may fail to create new consumers properly. To
 *   ensure that the request timeout is greater than the
 *   `options.sessionTimeout`, values for both (or neither) of the
 *   `options.requestTimeout` and `options.sessionTimeout` parameters should be
 *   specified.
 * @param {Number} [options.sessionTimeout] - The timeout (in seconds) used to
 *   detect channel consumer failures. The consumer sends periodic heartbeats
 *   to indicate its liveness to the broker. If no heartbeats are
 *   received by the broker before the expiration of this session
 *   timeout, then the broker may remove this consumer from the group.
 *   If set to `null` or `undefined` (the default), the session timeout is
 *   determined automatically by the streaming service. Note that if a value is
 *   set for the session timeout, the value should be less than the
 *   `options.requestTimeout`. Otherwise, the streaming service may fail to
 *   create new consumers properly. To ensure that the session timeout is
 *   less than the `options.requestTimeout`, values for both (or neither)
 *   of the `options.requestTimeout` and `options.sessionTimeout` parameters
 *   should be specified.
 * @param {Boolean} [options.retryOnFail=true] - Whether or not the channel
 *   will automatically retry a call which failed due to a temporary error.
 * @param {Object} [options.extraConfigs] - Object with properties containing
 *   any custom configuration settings which should be sent to the streaming
 *   service when a consumer is created. Note that any values specified for the
 *   `options.offset`, `options.requestTimeout`, and/or `options.sessionTimeout`
 *   parameters will override the corresponding values, if specified, in the
 *   `options.extraConfigs` parameter.
 * @param {String} [options.key] - Optional client private keys in PEM format.
 *   See
 *   {@link https://nodejs.org/api/tls.html#tls_tls_createsecurecontext_options}.
 * @param {String} [options.cert] - Optional client cert chains in PEM format.
 *   See
 *   {@link https://nodejs.org/api/tls.html#tls_tls_createsecurecontext_options}.
 * @param {String} [options.ca] - Optionally override the trusted CA
 *   certificates used to validate the streaming service. Any string can
 *   contain multiple PEM CAs concatenated together.
 *   See
 *   {@link https://nodejs.org/api/tls.html#tls_tls_createsecurecontext_options}.
 * @param {String} [options.passphrase] - Optional shared passphrase used for a
 *   single private key. See
 *   {@link https://nodejs.org/api/tls.html#tls_tls_createsecurecontext_options}.
 * @param {Boolean} [options.rejectUnauthorized=true] - If not false, the server
 *   certificate is verified against the list of supplied CAs. See
 *   {@link https://nodejs.org/api/tls.html#tls_tls_connect_options_callback}.
 * @param {Function} [options.checkServerIdentity] - A callback function to
 *   be used when checking the server's hostname against the certificate.
 *   See
 *   {@link https://nodejs.org/api/tls.html#tls_tls_connect_options_callback}.
 * @constructor
 */
function Channel (base, options) {
  var channel = this

  if (!base) {
    throw new PermanentError('Value must be specified for base')
  }
  this._base = base

  options = options || {}

  this._auth = options.auth
  this._consumerGroup = options.consumerGroup

  var offsetValues = ['latest', 'earliest', 'none']
  if (options.offset && offsetValues.indexOf(options.offset) < 0) {
    throw new PermanentError('Value for \'offset\' must be one of ' +
      offsetValues.join(', '))
  }

  var pathPrefix = options.pathPrefix
  if (pathPrefix) {
    this._consumerPathPrefix = pathPrefix
    this._producerPathPrefix = pathPrefix
  } else {
    this._consumerPathPrefix =
      options.consumerPathPrefix || DEFAULT_CONSUMER_PATH_PREFIX
    this._producerPathPrefix =
      options.producerPathPrefix || DEFAULT_PRODUCER_PATH_PREFIX
  }

  this._configs = {}
  var extraConfigs = options.extraConfigs || {}
  Object.keys(extraConfigs).forEach(function (setting) {
    channel._configs[setting] = extraConfigs[setting]
  })

  this._configs[AUTO_OFFSET_RESET_CONFIG_SETTING] = options.offset || 'latest'

  if (extraConfigs[ENABLE_AUTO_COMMIT_CONFIG_SETTING]) {
    this._configs[ENABLE_AUTO_COMMIT_CONFIG_SETTING] =
      extraConfigs[ENABLE_AUTO_COMMIT_CONFIG_SETTING]
  } else {
    // This has to be false for now
    this._configs[ENABLE_AUTO_COMMIT_CONFIG_SETTING] = 'false'
  }

  if ((typeof options.sessionTimeout !== 'undefined') &&
    (options.sessionTimeout !== null)) {
    var sessionTimeout = Number(options.sessionTimeout)
    if (isNaN(sessionTimeout)) {
      throw new TypeError('sessionTimeout must be a number')
    }
    // Convert from seconds to milliseconds
    this._configs[SESSION_TIMEOUT_CONFIG_SETTING] =
      (sessionTimeout * 1000).toString()
  }

  if ((typeof options.requestTimeout !== 'undefined') &&
    (options.requestTimeout !== null)) {
    var requestTimeout = Number(options.requestTimeout)
    if (isNaN(requestTimeout)) {
      throw new TypeError('requestTimeout must be a number')
    }
    // Convert from seconds to milliseconds
    this._configs[REQUEST_TIMEOUT_CONFIG_SETTING] =
      (requestTimeout * 1000).toString()
  }

  /**
   * Whether or not the channel will automatically retry a call which
   * failed due to a temporary error.
   */
  this.retryOnFail = options.hasOwnProperty(
    'retryOnFail') ? options.retryOnFail : true

  /**
   * Setup the defaults to be used in each HTTP request to be made to the
   * channel. A `jar` is defined in order to manage cookies which may be
   * provided from the server when establishing a channel consumer.
   * @private
   */
  this._request = request.defaults(
    util.addTlsOptions({
      baseUrl: this._base,
      jar: request.jar()
    }, options)
  )

  /**
   * Most recent consumer id returned from the streaming service for a
   * channel consumer.
   * @type {String}
   * @private
   */
  this._consumerId = null

  /**
   * Array of strings containing the topics for which a channel consumer is
   * actively subscribed.
   * @type {Array<String>}
   * @private
   */
  this._activeSubscriptions = []

  /**
   * Array of strings containing the topics that the channel consumer has
   * requested to be subscribed to. Once the server has provided a successful
   * response to a subscription request, this array is copied to the
   * {@link Channel#_activeSubscriptions}.
   * @type {Array<String>}
   * @private
   */
  this._requestedSubscriptions = []

  /**
   * Array of records which have been consumed from the channel but whose
   * offsets have not yet been committed back to the channel.
   * @type {Array<Object>}
   * @private
   */
  this._recordsCommitLog = []

  /**
   * Whether or not the channel is currently active. Once the channel has
   * been destroyed, this is set to `false`.
   * @type {boolean}
   * @private
   */
  this._active = true

  /**
   * Whether or not a {@link Channel#run} call is active.
   * @type {boolean}
   * @private
   */
  this._running = false

  /**
   * Whether or not a {@link Channel#stop} call has been made to terminate
   * an active {@link Channel#run} call.
   * @type {boolean}
   * @private
   */
  this._stopRequested = false

  /**
   * An array of callbacks which should be invoked when a channel is stopped.
   * @type {Array<Function>}
   * @private
   */
  this._stopCallbacks = []

  /**
   * Function invoked while a {@link Channel#run} is in progress to resume
   * consumption of records after waiting for the configured
   * `waitBetweenQueries`.
   * @type {Function}
   * @private
   */
  this._runLoopFunc = null

  /**
   * Timeout object related to the `_runLoopFunc`.
   * @private
   */
  this._runLoopTimeout = null

  /**
   * Returns whether or not the channel is still active.
   * @param {Function} callback - Callback to invoke with an error if the
   *   channel is not active.
   * @returns {Boolean} true if the channel is still active, false if not.
   * @private
   */
  this._stillActive = function (callback) {
    if (!this._active && callback) {
      callback(new PermanentError('Channel has been destroyed'))
    }
    return this._active
  }

  /**
   * Send an HTTP request to the channel. The request options should include
   * info for the user, if authenticated -- for example, a channel token.
   * @param {Function} sendFunc - Function to invoke in order to send the
   *   request.
   *
   *   The first parameter in the function call is the same as the `options`
   *   parameter. The second parameter is a callback that the function should
   *   invoke with the results from sending the request.
   *
   *   The first parameter in the call to the results callback is an `Error`
   *   object, if an error occurred during the send attempt, else `null`. The
   *   second parameter is an HTTP response object, if the an HTTP response
   *   could be returned for the request, else `null`.
   * @param {Object} [options] - Options to use for the request.
   * @param {Function} [successCallback] - Callback to invoke if the request
   *   is "successful", with an HTTP response code greater than or equal to
   *   200 and less than or equal to 204. The first parameter to the callback
   *   is the HTTP response object.
   * @param {Function} [errorCallback] - Callback to invoke if the request
   *   "fails". A failed connection to the server or HTTP status code in the
   *   response outside of the range from 200 to 204 is considered to be a
   *   failure. The first parameter to the callback is an `Error` object
   *   containing failure details.
   * @param {Function} [notFoundCallback] - Callback to invoke if the response
   *   to the request includes an HTTP 404 (Not Found) status code. If a `null`
   *   value is provided for this callback and the request results in an HTTP
   *   404 response, the `errorCallback` function, if specified, is invoked.
   *   The first parameter to the callback is the HTTP response object.
   * @private
   */
  this._sendRequestWithAuthInfo = function (sendFunc, options,
                                            successCallback, errorCallback,
                                            notFoundCallback) {
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
        // Call reset on the channel authenticator in order to allow it to
        // clear out any cached credentials (for example, a token) before
        // attempting to retry a request.
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

  /**
   * Send an HTTP request to the channel.
   * @param {Function} sendFunc - Function to invoke in order to send the
   *   request.
   *
   *   The first parameter in the function call is the same as the `options`
   *   parameter. The second parameter is a callback that the function should
   *   invoke with the results from sending the request.
   *
   *   The first parameter in the call to the results callback is an `Error`
   *   object, if an error occurred during the send attempt, else `null`. The
   *   second parameter is an HTTP response object, if the an HTTP response
   *   could be returned for the request, else `null`.
   * @param {Object} [options] - Options to use for the request.
   * @param {Function} [successCallback] - Callback to invoke if the request
   *   is "successful", with an HTTP response code greater than or equal to
   *   200 and less than or equal to 204. The first parameter to the callback
   *   is the HTTP response object.
   * @param {Function} [errorCallback] - Callback to invoke if the request
   *   "fails". A failed connection to the server or HTTP status code in the
   *   response outside of the range from 200 to 204 is considered to be a
   *   failure. The first parameter to the callback is an `Error` object
   *   containing failure details.
   * @param {Function} [notFoundCallback] - Callback to invoke if the response
   *   to the request includes an HTTP 404 (Not Found) status code. If a `null`
   *   value is provided for this callback and the request results in an HTTP
   *   404 response, the `errorCallback` function, if specified, is invoked.
   *   The first parameter to the callback is the HTTP response object.
   * @private
   */
  this._sendRequest = function (sendFunc, options,
                                successCallback, errorCallback,
                                notFoundCallback) {
    if (typeof options === 'string') {
      options = {uri: options}
    }
    if (this._auth) {
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

  /**
   * Perform an operation, retrying the operation in the event that it fails.
   *
   * The `operationCallback` will be called with a `completionCallback`
   * repeatedly until at least one of the following occurs:
   *
   * 1. No error is delivered to the first parameter in the `completionCallback`.
   * 2. A {@link ConsumerError} instance is delivered to the
   *   `completionCallback`.
   * 3. The {@link Channel#retryOnFail} property is set to `false`.
   * 4. An {@link Channel#run} call is active but a stop has been requested,
   *    for example, due to a call to {@link Channel#stop}.
   *
   * @param {Function} operationCallback - Function to invoke for each
   *   operation attempt. The first parameter delivered to the
   *   `operationCallback` is a callback which is invoked when the operation
   *   attempt is complete. The first parameter sent to the completion callback
   *   is an `Error` object, if an error occurred when performing the operation,
   *   else `null`. The second parameter sent to the completion callback is an
   *   object representing the `result` of the operation attempt.
   * @param {Function} completeCallback - Callback function to invoke after
   *   the final attempt to perform the `operationCallback` is made. The first
   *   parameter sent to the `completeCallback` is an `Error` object, if an
   *   `Error` occurred, else `null`. The second parameter sent to the
   *   `operationCallback` is an object containing the `result` from the last
   *   operation attempt (or `null` if the operation was stopped prior to a
   *   final attempt).
   * @private
   */
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

  /**
   * Determine if the channel is already subscribed to consume an array
   * of `topics`.
   * @param {Array<String>} topics - Array of topics.
   * @returns {boolean} true if the channel is subscribed to the supplied
   *   list of topics, false if there are any differences between the
   *   supplied topics and the array of active topics.
   * @private
   */
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

  /**
   * Internal helper for subscribing the consumer to an array of topics.
   * @param {Array<String>} topics - Topic array.
   * @param {Function} callback - Callback function invoked when the
   *   subscription attempt is complete. The first parameter in the call to the
   *   results callback is an `Error` object, if an error occurred during the
   *   send attempt, else `null`.
   * @private
   */
  this._subscribe = function (topics, callback) {
    this._requestedSubscriptions = topics
    this._retryOnFailure(
      function (retryCallback) {
        var consumerId = channel._consumerId
        channel._sendRequest(
          channel._request.post,
          {
            uri: util.appendUrlSubpath(channel._consumerPathPrefix,
              'consumers/' + consumerId + '/subscription'),
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

  /**
   * Function which processes the response from the `processCallback`
   * invoked during a run/consume operation. This function attempts to
   * perform a {@link Channel#commit} for outstanding records which have
   * been consumed but not previously committed and waits before calling
   * the `doneCallback` to continue until the supplied `waitBetweenQueries`
   * has elapsed.
   * @param {Boolean} continueRunning - Whether or not to try to consume
   *   more records.
   * @param {Function} doneCallback - Callback function to invoke when
   *   done processing the response. The first parameter delivered to the
   *   `doneCallback` is an `Error` object, if an error occurred during
   *   processing the response, else `null`. The second parameter
   *   delivered to the `doneCallback` is a `boolean` indicating whether
   *   or not to try to consume more records.
   * @param {Number} waitBetweenQueries - Number of seconds to wait between
   *   calls to consume records.
   * @private
   */
  this._handleProcessCallbackResponse = function (
    continueRunning, doneCallback, waitBetweenQueries) {
    if (this._stopRequested) {
      continueRunning = false
    }
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

  /**
   * Repeatedly consume records from subscribed topics during an
   * active {@link Channel#run}.
   * @param {Function} processCallback - Callback to invoke with an array
   *   of payloads from records which have been consumed.
   * @param {Function} doneCallback - Callback function to invoke when
   *   done performing the consume attempt. The first parameter delivered to the
   *   `doneCallback` is an `Error` object, if an error occurred during the
   *   consume attempt, else `null`. The second parameter delivered to the
   *   `doneCallback` is a `boolean` indicating whether or not to try to consume
   *   more records.
   * @param {Number} waitBetweenQueries - Number of seconds to wait between
   *   calls to consume records.
   * @private
   */
  this._consumeForRun = function (processCallback, doneCallback,
                                  waitBetweenQueries) {
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

  /**
   * Handle `Error` objects delivered during a {@link Channel#run}.
   * @param {Error} error - The error.
   * @param {Function} runLoop - The run loop function to invoke in the event
   *   of a recoverable error.
   * @param {Function} doneCallback - Function to invoke if the run has been
   *   stopped.
   * @private
   */
  this._handleRunError = function (error, runLoop, doneCallback) {
    // If a `ConsumerError` occurred, reset the stored consumer info and
    // continue running. As the run is continued, a new consumer should be
    // established.
    if (error instanceof ConsumerError) {
      this.reset()
      runLoop()
    } else {
      this._running = false
      var stopCallbacks = this._stopCallbacks
      this._stopCallbacks = []
      this._stopRequested = false
      // Invoke registered stop callbacks to let them know that the current
      // run has been stopped.
      stopCallbacks.forEach(function (stopCallback) {
        try {
          stopCallback()
        } catch (stopError) {
          console.log('Error thrown from stop callback: ' + stopError)
        }
      })
      if (doneCallback) {
        // If the error was due to the stop being requested, don't bother
        // passing that on to the `doneCallback` since this is was a
        // requested shutdown (as opposed to a more critical channel-related
        // error).
        if (error instanceof StopError) {
          error = null
        }
        doneCallback(error)
      }
    }
  }
}

/**
 * Resets local consumer data stored for the channel.
 */
Channel.prototype.reset = function () {
  this._consumerId = null
  this._activeSubscriptions = []
  this._requestedSubscriptions = []
  this._recordsCommitLog = []
}

/**
 * Creates a new consumer on the consumer group.
 * @param {Function} callback - Callback function to invoke when the creation
 *   attempt has completed. The first parameter in the call to the
 *   results callback is an `Error` object, if an error occurred during the
 *   send attempt, else `null`. Possible `Error` types include:
 *
 *   * {@link TemporaryError} - If the creation attempt fails and
 *     {@link Channel#retryOnFail} is set to False.
 *   * {@link PermanentError} - If the channel has been destroyed.
 */
Channel.prototype.create = function (callback) {
  if (!this._consumerGroup) {
    throw new PermanentError(
      "No value specified for 'consumerGroup' option during channel init")
  }

  this.reset()
  var channel = this
  this._retryOnFailure(
    function (retryCallback) {
      channel._sendRequest(
        channel._request.post,
        {
          uri: util.appendUrlSubpath(channel._consumerPathPrefix, 'consumers'),
          json: true,
          body: {
            consumerGroup: channel._consumerGroup,
            configs: channel._configs
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

/**
 * Subscribes the consumer to an array of topics.
 * @param {(String|Array<String>)} topics - Topic or array of topics.
 * @param {Function} callback - Callback function invoked when the
 *   subscription attempt is complete. The first parameter in the call to the
 *   results callback is an `Error` object, if an error occurred during the
 *   send attempt, else `null`. Possible `Error` types include:
 *
 *   * {@link ConsumerError} - If the consumer associated with the channel
 *     does not exist on the server and {@link Channel#retryOnFail} is set to
 *     `false`.
 *   * {@link TemporaryError} - If the subscription attempt fails and
 *     {@link Channel#retryOnFail} is set to False.
 *   * {@link PermanentError} - If the channel has been destroyed.
 */
Channel.prototype.subscribe = function (topics, callback) {
  if (!topics) {
    throw new PermanentError('Value must be specified for topics')
  }
  if (typeof topics === 'string') {
    topics = [topics]
  } else if (!topics.length) {
    throw new PermanentError('At least one topic must be specified')
  }

  // As an optimization, avoid re-subscribing if the array of topics
  // to subcribe for is identical to what the consumer is already subscribed to.
  if (this._alreadySubscribed(topics)) {
    callbackAsync(callback)
  } else {
    if (this._consumerId) {
      this._subscribe(topics, callback)
    } else {
      var channel = this
      this.create(function (error) {
        if (error) {
          if (callback) {
            callback(error)
          }
        } else {
          channel._subscribe(topics, callback)
        }
      })
    }
  }
}

/**
 * Consumes records from all the subscribed topics.
 * @param {Function} callback - Callback function invoked when the
 *   consume attempt is complete.
 *
 *   The first parameter supplied to the callback is an `Error` object, if an
 *   error occurred during the consume attempt, else `null`. Possible `Error`
 *   types include:
 *
 *   * {@link ConsumerError} - If the consumer associated with the channel
 *     does not exist on the server and {@link Channel#retryOnFail} is set to
 *     `false`.
 *   * {@link TemporaryError} - If the consume attempt fails
 *     and {@link Channel#retryOnFail} is set to False.
 *   * {@link PermanentError} - If the channel has been destroyed.
 *
 * The second parameter supplied to the callback is, for a successful consume,
 * an array of payloads (decoded as objects) from records returned from the
 * server. For a failed consume, the second parameter is `null`.
 * @throws {PermanentError} If the channel has not been subscribed to any
 *   topics.
 */
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
          uri: util.appendUrlSubpath(channel._consumerPathPrefix, 'consumers/' +
            consumerId + '/records'),
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

/**
 * Commits the record offsets to the channel.
 * @param {Function} callback - Callback function invoked when the
 *   commit attempt is complete.
 *
 *   The first parameter supplied to the callback is an `Error` object, if an
 *   error occurred during the commit attempt, else `null`. Possible `Error`
 *   types include:
 *
 *   * {@link ConsumerError} - If the consumer associated with the channel
 *     does not exist on the server and {@link Channel#retryOnFail} is set to
 *     `false`.
 *   * {@link TemporaryError} - If the commit attempt fails
 *     and {@link Channel#retryOnFail} is set to False.
 *   * {@link PermanentError} - If the channel has been destroyed.
 */
Channel.prototype.commit = function (callback) {
  if (this._recordsCommitLog.length) {
    var channel = this
    this._retryOnFailure(
      function (retryCallback) {
        var consumerId = channel._consumerId
        channel._sendRequest(
          channel._request.post,
          {
            uri: util.appendUrlSubpath(channel._consumerPathPrefix,
              'consumers/' + consumerId + '/offsets'),
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

/**
 * Repeatedly consume records from the subscribed topics. The supplied
 * `processCallback` is an array of payloads (decoded as objects) from records
 * returned from the server.
 *
 * The `processCallback` should return a value of `true` in order for this
 * function to continue consuming additional records. For a return value of
 * `false` or no return value, no additional records will be consumed and this
 * function will return.
 *
 * The {@link Channel#stop} method can also be called to halt an execution of
 * this method.
 * @param {Function} processCallback - Callback to invoke with an array
 *   of payloads from records which have been consumed.
 * @param {Function} doneCallback - Callback to invoke when the run is
 *   complete. The first parameter supplied to the callback is an `Error`
 *   object, if an error occurred during the run, else `null`.
 * @param {Number} waitBetweenQueries - Number of seconds to wait between
 *   calls to consume records.
 * @param {(String|Array<String>)} topics - Topic or array of topics. If set to
 *   a non-empty value, the channel will be subscribed to the specified topics.
 *   If set to an empty value, the channel will use topics previously subscribed
 *   via a call to the {@link Channel#subscribe} method.
 * @throws {PermanentError} If a previous run is already in progress.
 */
Channel.prototype.run = function (processCallback, doneCallback,
                                  waitBetweenQueries, topics) {
  if (!this._consumerGroup) {
    throw new PermanentError(
      "No value specified for 'consumerGroup' option during channel init")
  }

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
    channel._consumeForRun(processCallback,
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

/**
 * Stop an active execution of a {@link Channel#run}. If no run is active, the
 * supplied callback is invoked immediately. If a run is active, the supplied
 * callback is invoked after the run has been completed.
 *
 * @param {Function} callback - Function to invoke when the run has been
 *   stopped.
 */
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

/**
 * Deletes the consumer from the consumer group.
 * @param {Function} callback - Callback to invoke after the consumer has
 *   been deleted. The first parameter supplied to the callback is an `Error`
 *   object, if an error occurred during the delete, else `null`.
 */
Channel.prototype.delete = function (callback) {
  var consumerId = this._consumerId
  if (consumerId) {
    var channel = this
    channel._sendRequest(
      channel._request.delete,
      util.appendUrlSubpath(channel._consumerPathPrefix, 'consumers/' +
        consumerId),
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

/**
 * Produces records to the channel.
 * @param {Object} payload - Payload containing the records to be posted to the
 *   channel.
 * @param {Function} callback - Function to invoke when the produce has been
 *   stopped. The first parameter supplied to the callback is an `Error`
 *   object, if an error occurred during the produce, else `null`. The parameter
 *   will be of type {@link PermanentError} if an unsuccessful response is
 *   received from the streaming service for the produce attempt.
 */
Channel.prototype.produce = function (payload, callback) {
  this._sendRequest(
    this._request.post,
    {
      uri: util.appendUrlSubpath(this._producerPathPrefix, 'produce'),
      json: true,
      body: payload,
      headers: {
        'Content-Type': 'application/vnd.dxl.intel.records.v1+json'
      }
    },
    function () {
      if (callback) {
        callback(null)
      }
    },
    callback
  )
}

/**
 * Destroys the channel (releases all associated resources).
 *
 * **NOTE:** Once the method has been invoked, no other calls should be
 * made to the channel.
 *
 * @param {Function} callback - Function to invoke when the channel has been
 *   destroyed. The first parameter supplied to the callback is an `Error`
 *   object, if an error occurred during the destroy, else `null`. The parameter
 *   will be of type {@link TemporaryError} if a consumer has previously been
 *   created for the channel but an attempt to delete the consumer from the
 *   channel fails.
 */
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
