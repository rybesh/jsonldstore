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
          req.bucket = bucket
          next()
        })
    } else {
      req.bucket = _bucket
      process.nextTick(next)
    }
  }
}

function load(req, res, next) {
  var couchbase
    , parsejsonld
    , results = []

  if (!req.bucket) {
    return next(new Error('Must run dbconnect handler before load route'))
  }

  couchbase = new CouchbaseWritable(null, req.bucket)
  couchbase.on('error', function(err) {
    next(err)
  })
  couchbase.on('meta', function(meta) {
    results.push(meta)
  })
  couchbase.on('finish', function() {
    res.json(201, results)
    next()
  })

  parsejsonld = new JSONLDTransform()
  parsejsonld.on('error', function(err) {
    next(err)
  })
  parsejsonld.on('context', function(o, scope){
    req.log.debug('context', o, scope)
  })
  parsejsonld.on('graph', function(o){
    couchbase.write(o)
    res.header('location', o['@id'])
  })

  req.pipe(parsejsonld).pipe(couchbase)
}

function get(req, res, next) {
  if (!req.bucket) {
    return next(new Error('Must run dbconnect handler before get route'))
  }
  req.bucket.get(req.params[0], function(err, doc, meta) {
    if (err) return next(err)
    res.json(doc)
    next()
  })
}

exports.createServer = function(options) {
  if (!options) options = {}
  var server = restify.createServer(options);
  server.use(restify.requestLogger())
  server.use(dbconnect(options.bucket))
  server.post('/', load)
  server.get(/^(.*)$/, get)
  server.on('uncaughtException', function (req, res, route, e) {
    server.log.fatal(e.stack)
  })
  return server
}
