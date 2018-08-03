/**
 * @module Util
 * @private
 */

'use strict'

module.exports = {
  /**
   * Initialize the supplied object with the standard information which appears
   * on an {@link Error} function. This function is used to allow a function
   * to derive from the {@link Error} function.
   * @param {Object} obj - The object to initialize error data onto.
   * @param {String} [message] - An error message.
   */
  initializeError: function (obj, message) {
    Error.call(obj, message)
    if (Error.hasOwnProperty('captureStackTrace')) {
      Error.captureStackTrace(obj, obj.constructor)
    }
    obj.name = obj.constructor.name
    obj.message = message
  },
  /**
   * Append a subpath onto a base URL, removing any redundant forward slash
   * segment separators. For example, the following combinations of parameters
   * result in the return of "http://localhost/base/sub".
   *
   * * `url` ("http://localhost/base"), `subpath` ("sub").
   * * `url` ("http://localhost/base/"), `subpath` ("/sub").
   * * `url` ("http://localhost/base"), `subpath` ("/sub").
   * * `url` ("http://localhost/base/"), `subpath` ("sub").
   *
   * @param {String} url - Base URL.
   * @param {String} subpath - Subpath to append to the base URL.
   * @returns {String} Base URL, with the subpath appended.
   */
  appendUrlSubpath: function (url, subpath) {
    return url.replace(/\/$/, '') + '/' + subpath.replace(/^\//, '')
  },
  /**
   * Add properties from the `tlsOptions` object into the `options` object.
   * @param options
   * @param tlsOptions
   * @param {Object} tlsOptions - Additional options to supply for request
   *   authentication.
   * @param {String} [tlsOptions.key] - Optional client private keys in PEM
   *   format. See
   *   {@link https://nodejs.org/api/tls.html#tls_tls_createsecurecontext_options}.
   * @param {String} [tlsOptions.cert] - Optional client cert chains in PEM
   *   format. See
   *   {@link https://nodejs.org/api/tls.html#tls_tls_createsecurecontext_options}.
   * @param {String} [tlsOptions.ca] - Optionally override the trusted CA
   *   certificates used to validate the authentication server. Any string can
   *   contain multiple PEM CAs concatenated together.
   *   See
   *   {@link https://nodejs.org/api/tls.html#tls_tls_createsecurecontext_options}.
   * @param {String} [tlsOptions.passphrase] - Optional shared passphrase used
   *   for a single private key. See
   *   {@link https://nodejs.org/api/tls.html#tls_tls_createsecurecontext_options}.
   * @param {Boolean} [tlsOptions.rejectUnauthorized=true] - If not false, the
   *   server certificate is verified against the list of supplied CAs. See
   *   {@link https://nodejs.org/api/tls.html#tls_tls_connect_options_callback}.
   * @param {Function} [tlsOptions.checkServerIdentity] - A callback function to
   *   be used when checking the server's hostname against the certificate.
   *   See
   *   {@link https://nodejs.org/api/tls.html#tls_tls_connect_options_callback}.
   * @returns {Object} The `options` object, with recognized options from the
   *   `tlsOptions` parameter appended.
   */
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
