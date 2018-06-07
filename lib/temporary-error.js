'use strict'

var inherits = require('inherits')
var util = require('./util')

/**
 * @classdesc Error raised when an unexpected/unknown (but possibly recoverable)
 * error occurs.
 * @param {String} message - The error message.
 * @constructor
 */
function TemporaryError (message) {
  util.initializeError(this, message)
}

inherits(TemporaryError, Error)

module.exports = TemporaryError
