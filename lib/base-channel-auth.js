'use strict'

/**
 * Interface for authentication classes used with channel requests.
 * @interface
 */
function BaseChannelAuth () {}

/**
 * Authenticate the user for an HTTP channel request. The supplied callback
 * should be invoked with the results of the authentication attempt. See
 * {@link BaseChannelAuth~authCallback} for more information on the
 * content provided to the callback.
 * @param {Object} requestOptions - Options included in the HTTP channel
 *   request.
 * @param {BaseChannelAuth~authCallback} callback - Callback function
 *   invoked with the results of the authentication attempt.
 */
BaseChannelAuth.prototype.authenticate = function (requestOptions, callback) {
  throw new Error('Not implemented')
}

/**
 * Callback invoked with the result of a call to
 * {@link BaseChannelAuth#authenticate}.
 * @callback BaseChannelAuth~authCallback
 * @param {Error} [authError] - If no errors occurred during authentication,
 *   this parameter is `null`. For an error due to the user
 *   not being authorized, this parameter contains an instance of a
 *   {@link PermanentAuthenticationError} object. For an unexpected/unknown
 *   error, this parameter contains an instance of a
 *   {@link TemporaryAuthenticationError} object.
 * @param {Object} [requestOptions] - If any errors occurred during
 *   authentication, this parameter is `null`. Otherwise, this parameter
 *   includes the `requestOptions` object supplied in the {@link
 *   BaseChannelAuth#authenticate} call.  The `requestOptions` object may
 *   include additional properties for the authenticated user. For example, if
 *   the user were mapped to a bearer auth token, the `auth` property for the
 *   `requestOptions` object supplied to the callback could be set to:
 *
 *   ```js
 *   {bearer: '<the user token>'}
 *   ```
 */

/**
 * Purge any credentials cached from a previous authentication.
 */
BaseChannelAuth.prototype.reset = function () {
  throw new Error('Not implemented')
}

module.exports = BaseChannelAuth
