'use strict'

var inherits = require('inherits')
var util = require('./util')
var PermanentError = require('./permanent-error')

/**
 * @classdesc Exception raised when an error occurs during an authentication
 * attempt due to the user being unauthorized.
 * @param {String} message - The error message.
 * @constructor
 */
function PermanentAuthenticationError (message) {
  util.initializeError(this, message)
}

inherits(PermanentAuthenticationError, PermanentError)

module.exports = PermanentAuthenticationError
