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
  addTlsOptions: function (options, tlsOptions) {
    if (options && tlsOptions) {
      if (tlsOptions.key) {
        options.key = tlsOptions.key
      }
      if (tlsOptions.cert) {
        options.cert = tlsOptions.cert
      }
      if (tlsOptions.ca) {
        options.ca = tlsOptions.ca
      }
      if (tlsOptions.passphrase) {
        options.passphrase = tlsOptions.passphrase
      }
      if (tlsOptions.hasOwnProperty('rejectUnauthorized')) {
        options.rejectUnauthorized = tlsOptions.rejectUnauthorized
      }
      if (tlsOptions.checkServerIdentity) {
        options.checkServerIdentity = tlsOptions.checkServerIdentity
      }
    }
    return options
  }
}
