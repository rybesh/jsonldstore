"use strict";

var Readable = require('stream').Readable

function ArrayReadable(options, array) {
  if (!options) options = {}
  options.objectMode = true
  if (!(this instanceof ArrayReadable))
    return new ArrayReadable(options, array)

  Readable.call(this, options)
  this._array = array
}

ArrayReadable.prototype = Object.create(
  Readable.prototype, { constructor: { value: ArrayReadable }})

ArrayReadable.prototype._read = function() {
  var ok = true
  do {
    if (this._array && this._array.length) {
      ok = this.push(this._array.pop())
    }
    else {
      this.push(null)
      break
    }
  } while (ok)
}

exports.ArrayReadable = ArrayReadable

