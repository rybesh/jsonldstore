var couchbase = require('couchbase')
  , Writable = require('stream').Writable

function CouchbaseWritable(options, bucket) {
  if (!options) options = {}
  options.objectMode = true
  if (!(this instanceof CouchbaseWritable))
    return new CouchbaseWritable(options)

  Writable.call(this, options)
  this._bucket = null
}

CouchbaseWritable.prototype = Object.create(
  Writable.prototype, { constructor: { value: CouchbaseWritable }})

CouchbaseWritable.prototype.connect = function(config, done) {
  var self = this
  couchbase.connect(config, function(err, bucket) {
    if (err) {
      process.nextTick(function(){ done(err) })
      return
    }
    self._bucket = bucket
    process.nextTick(done)
  })
}

CouchbaseWritable.prototype._write = function(chunk, encoding, done) {
  var self = this

  // Check to see if we have a live CouchBase connection.
  if (self._bucket === null) {
    var err = new Error('Must connect to couchbase before writing chunks')
    self.emit('error', err)
    process.nextTick(function(){ done(err) })
    return    
  }

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

