'use strict'

var inherits = require('inherits')
var util = require('./util')

/**
 * @classdesc Error raised for an operation which is interrupted due to the
 * channel being stopped.
 * @param {String} message - The error message.
 * @constructor
 */
function StopError (message) {
  util.initializeError(this, message)
}

inherits(StopError, Error)

module.exports = StopError
