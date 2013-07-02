"use strict";

var Writable = require('stream').Writable

function CouchbaseWritable(options, bucket) {
  if (!options) options = {}
  options.objectMode = true
  if (!(this instanceof CouchbaseWritable))
    return new CouchbaseWritable(options)

  Writable.call(this, options)
  this._bucket = bucket
}

CouchbaseWritable.prototype = Object.create(
  Writable.prototype, { constructor: { value: CouchbaseWritable }})

CouchbaseWritable.prototype._write = function(chunk, encoding, done) {
  var self = this

  // Only accept objects with '@id' properties.
  if (chunk === null || 
      chunk === undefined || 
      'object' !== typeof chunk ||
      (!('@id' in chunk))) {
    var err = new TypeError('Invalid chunk: ' + chunk)
    self.emit('error', err)
    process.nextTick(function(){ done(err) })
    return
  }

  // Store the object under its '@id'.
  self._bucket.set(chunk['@id'], chunk, function (err, meta) {
    if (err) {
      self.emit('error', err)
      process.nextTick(function(){ done(err) })
      return
    }
    self.emit('meta', meta)
    process.nextTick(done)
  })
}

exports.CouchbaseWritable = CouchbaseWritable

