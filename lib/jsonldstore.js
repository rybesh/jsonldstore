"use strict";

var restify = require('restify')
  , couchbase = require('couchbase')
  , JSONLDTransform = require('jsonldtransform').JSONLDTransform
  , CouchbaseWritable = require('./cbwritable').CouchbaseWritable
  , _bucket = null

function dbconnect(bucketname) {
  return function(req, res, next) {
    if (_bucket === null) {
      couchbase.connect({ bucket: bucketname }, 
        function(err, bucket) {
          if (err) return next(err)
          _bucket = bucket
          req.couchbase = new CouchbaseWritable(null, _bucket)
          next()
        })
    } else {
      req.couchbase = new CouchbaseWritable(null, _bucket)
      process.nextTick(next)
    }
  }
}

function load(req, res, next) {
  var parsejsonld = new JSONLDTransform()
    , results = []

  if (!req.couchbase) {
    return next(new Error('Must run dbconnect handler before load route'))
  }

  req.couchbase.on('error', function(err) {
    next(err)
  })
  req.couchbase.on('meta', function(meta) {
    results.push(meta)
  })
  req.couchbase.on('finish', function() {
    res.json(201, results)
    next()
  })

  parsejsonld.on('error', function(err) {
    next(err)
  })
  parsejsonld.on('context', function(o, scope){
    req.log.debug('context', o, scope)
  })
  parsejsonld.on('graph', function(o){
    req.couchbase.write(o)
    res.header('location', o['@id'])
  })

  req.pipe(parsejsonld).pipe(req.couchbase)
}

exports.createServer = function(options) {
  if (!options) options = {}
  var server = restify.createServer(options);
  server.use(restify.requestLogger())
  server.use(dbconnect(options.bucket))
  server.post('/', load)
  server.on('uncaughtException', function (req, res, route, e) {
    server.log.fatal(e.stack)
  })
  return server
}
