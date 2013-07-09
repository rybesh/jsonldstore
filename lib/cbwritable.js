"use strict";

var Writable = require('stream').Writable
  , couchbase = require('couchbase')
  , hashurl = require('./hashurl')

function CouchbaseWritable(options, bucket, graph_key) {
  if (!options) options = {}
  options.objectMode = true
  if (!(this instanceof CouchbaseWritable))
    return new CouchbaseWritable(options)

  Writable.call(this, options)
  this._bucket = bucket
  this._graph_key = graph_key
  this._operation = options.operation || 'set'
}

CouchbaseWritable.prototype = Object.create(
  Writable.prototype, { constructor: { value: CouchbaseWritable }})

CouchbaseWritable.prototype._keyfor = function(o) {
  var self = this

  if ('@graph' in o)
    return self._graph_key
  else
    return self._graph_key + '/' + hashurl(o['@id'])
}

CouchbaseWritable.prototype._do = function(key, o, callback) {
  var self = this

  if (self._operation === 'remove') {
    self._bucket.remove(key, function(err, meta){
      if (!err) self.emit('remove', o)
      // Ignore key not found errors when removing.
      if (err && err.code === couchbase.errors.keyNotFound)
        callback(null, meta)
      else
        callback(err, meta)
    })

  } else { // add, replace, set
    self._bucket[self._operation](key, o, function(err, meta){
      if (!err) self.emit(self._operation, o)
      callback(err, meta)
    })
  }
}

CouchbaseWritable.prototype.replace = function(key, o, done) {
  var self = this

  if (! self._validChunk(o)) {
    var err = new TypeError('Invalid object: ' + o)
    self.emit('error', err)
    process.nextTick(function(){ done(err) })
    return
  }

  self._bucket.replace(key, o, function (err, meta) {
    if (err) {
      self.emit('error', err)
      process.nextTick(function(){ done(err) })
      return
    }
    self.emit('meta', meta)
    process.nextTick(done)
  })
}

CouchbaseWritable.prototype._validChunk = function(chunk) {
  // Only accept objects with '@id' properties.
  return !(chunk === null || 
           chunk === undefined || 
           'object' !== typeof chunk ||
           (!('@id' in chunk)))
}

CouchbaseWritable.prototype._write = function(chunk, encoding, done) {
  var self = this

  if (! self._validChunk(chunk)) {
    var err = new TypeError('Invalid chunk: ' + chunk)
    self.emit('error', err)
    process.nextTick(function(){ done(err) })
    return
  }

  // Do something with the object.
  self._do(self._keyfor(chunk), chunk, function (err, meta) {
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

