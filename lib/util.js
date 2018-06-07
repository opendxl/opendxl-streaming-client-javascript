/**
 * @module Util
 * @private
 */

'use strict'

var fs = require('fs')

module.exports = {
  /**
   * Initialize the supplied object with the standard information which appears
   * on an {@link Error} function. This function is used to allow a function
   * to derive from the {@link Error} function.
   * @param {Object} obj - The object to initialize error data onto.
   * @param {String} [message=null] - An error message.
   */
  initializeError: function (obj, message) {
    Error.call(obj, message)
    if (Error.hasOwnProperty('captureStackTrace')) {
      Error.captureStackTrace(obj, obj.constructor)
    }
    obj.name = obj.constructor.name
    obj.message = message
  },
  appendUrlSubpath: function (url, subpath) {
    return url.replace(/\/$/, '') + '/' + subpath.replace(/^\//, '')
  },
  appendCertVerifyToOptions: function (options, verify, ErrorType) {
    ErrorType = ErrorType || Error
    if (options) {
      if (verify) {
        try {
          options.ca = fs.readFileSync(verify)
        } catch (error) {
          throw new ErrorType('Unable to read file for verify option: ' +
            error.message
          )
        }
      } else {
        options.rejectUnauthorized = false
        options.requestCert = false
      }
      options.checkServerIdentity = function () {
        return undefined
      }
    }
    return options
  }
}
