"use strict";

var Writable = require('stream').Writable
  , couchbase = require('couchbase')
  , hashurl = require('./hashurl')

function CouchbaseWritable(options, db, graph_key) {
  if (!options) options = {}
  options.objectMode = true
  if (!(this instanceof CouchbaseWritable))
    return new CouchbaseWritable(options, db, graph_key)

  Writable.call(this, options)
  this._db = db
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
    self._db.remove(key, function(err, result){
      if (!err) self.emit('remove', key, o)
      // Ignore key not found errors when removing.
      if (err && err.code === couchbase.errors.keyNotFound)
        callback(null, result)
      else
        callback(err, result)
    })

  } else { // add, replace, set
    self._db[self._operation](key, o, function(err, result){
      if (!err) self.emit(self._operation, key, o)
      callback(err, result)
    })
  }
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
  self._do(self._keyfor(chunk), chunk, function (err, result) {
    if (err) {
      self.emit('error', err)
      process.nextTick(function(){ done(err) })
      return
    }
    self.emit('result', result)
    process.nextTick(done)
  })
}

exports.CouchbaseWritable = CouchbaseWritable
