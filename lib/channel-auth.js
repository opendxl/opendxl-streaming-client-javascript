'use strict'

var request = require('request')
var PermanentError = require('./permanent-error')
var PermanentAuthenticationError = require('./permanent-authentication-error')
var TemporaryAuthenticationError = require('./temporary-authentication-error')
var util = require('./util')

var LOGIN_PATH_FRAGMENT = '/identity/v1/login'

function ChannelAuth (base, username, password, verify) {
  this._loginRequest = request.defaults(
    util.appendCertVerifyToOptions({
      baseUrl: base,
      uri: LOGIN_PATH_FRAGMENT
    }, verify, PermanentError)
  )

  this._username = username
  this._password = password
  this._token = null

  this._addBearerAuthToken = function (requestOptions, callback) {
    requestOptions.auth = {bearer: this._token}
    callback(null, requestOptions)
  }
}

ChannelAuth.prototype.authenticate = function (requestOptions, callback) {
  var that = this
  if (this._token) {
    this._addBearerAuthToken(requestOptions, callback)
  } else {
    this._loginRequest.get(
      {
        auth: {
          user: this._username,
          password: this._password
        },
        json: true
      },
      function (error, response, body) {
        if (error) {
          callback(new TemporaryAuthenticationError(
            'Unexpected error: ' + error.message
          ))
        } else if (response.statusCode === 200) {
          if (body.AuthorizationToken) {
            that._token = body.AuthorizationToken
            that._addBearerAuthToken(requestOptions, callback)
          } else {
            callback(new PermanentAuthenticationError(
              'Unable to locate AuthorizationToken in login response'
            ))
          }
        } else if ([401, 403].indexOf(response.statusCode) >= 0) {
          callback(new PermanentAuthenticationError(
            'Unauthorized ' + response.statusCode + ': ' + body
          ))
        } else {
          callback(new TemporaryAuthenticationError(
            'Unexpected status code ' + response.statusCode + ': ' +
            JSON.stringify(body)
          ))
        }
      }
    )
  }
}

ChannelAuth.prototype.reset = function () {
  this._token = null
}

module.exports = ChannelAuth
