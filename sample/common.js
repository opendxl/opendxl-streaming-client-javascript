'use strict'

var fs = require('fs')
var path = require('path')

module.exports = {
  requireClient: function () {
    var packageName = '@opendxl/dxl-streaming-consumer-client'
    var packageFile = path.join(__dirname, '..', 'package.json')
    var module
    // Use local library sources if the example is being run from within a local
    // repository clone.
    if (fs.existsSync(packageFile)) {
      var packageInfo = JSON.parse(fs.readFileSync(packageFile))
      if (packageInfo.name === packageName) {
        module = require(path.dirname(packageFile))
      }
    }
    if (!module) {
      // The example does not appear to be running from within a local
      // repository clone, so attempt to require by the package name.
      module = require(packageName)
    }
    return module
  }
}
