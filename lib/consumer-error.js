'use strict'

var inherits = require('inherits')
var util = require('./util')
var TemporaryError = require('./temporary-error')

/**
 * @classdesc Error when a channel operation fails due to the associated
 * consumer not being recognized by the consumer service.
 * @param {String} message - The error message.
 * @constructor
 */
function ConsumerError (message) {
  util.initializeError(this, message)
}

inherits(ConsumerError, TemporaryError)

module.exports = ConsumerError
