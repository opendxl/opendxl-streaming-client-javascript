'use strict'

var inherits = require('inherits')
var util = require('./util')
var TemporaryError = require('./temporary-error')

/**
 * @classdesc Error raised when an unexpected/unknown (but possibly recoverable)
 * error occurs during an authentication attempt.
 * @param {String} message - The error message.
 * @constructor
 */
function TemporaryAuthenticationError (message) {
  util.initializeError(this, message)
}

inherits(TemporaryAuthenticationError, TemporaryError)

module.exports = TemporaryAuthenticationError
