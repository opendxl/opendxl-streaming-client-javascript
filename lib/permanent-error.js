'use strict'

var inherits = require('inherits')
var util = require('./util')

/**
 * @classdesc Error raised for an operation which would not be expected to
 * succeed even if the operation were retried.
 * @param {String} message - The error message.
 * @constructor
 */
function PermanentError (message) {
  util.initializeError(this, message)
}

inherits(PermanentError, Error)

module.exports = PermanentError
