"use strict";

var uri = require('uri-js')
  , md5 = require('MD5')


function hashurl (url) {
  return md5(uri.normalize(url))
}

module.exports = hashurl 