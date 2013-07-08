"use strict";

var restify = require('restify')
  , async = require('async')
  , bunyan = require('bunyan')
  , couchbase = require('couchbase')
  , uuid = require('node-uuid')
  , JSONLDTransform = require('jsonldtransform').JSONLDTransform
  , ArrayReadable = require('./arrayreadable').ArrayReadable
  , CouchbaseWritable = require('./cbwritable').CouchbaseWritable
  , _bucket = null

function dbconnect(bucketname) {
  return function(req, res, next) {
    req.log.debug(req.method,' to ',req.url)
    if (_bucket === null) {
      couchbase.connect({ bucket: bucketname }, 
        function(err, bucket) {
          if (err) {
            req.log.error(err)
            return next(err)
          }
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

function load_graph(req, res, next) {
  var adder
    , parsejsonld
    , graph_key = uuid.v4()
    , results = []

  if (!req.bucket) {
    return next(new Error('Must run dbconnect handler before load_graph route'))
  }

  adder = new CouchbaseWritable({operation: 'add'}, req.bucket, graph_key)
  adder.on('error', function(err) {
    req.log.error(err)
    next(err)
  })
  adder.on('meta', function(meta) {
    results.push(meta)
  })
  adder.on('finish', function() {
    res.json(201, results)
    next()
  })

  parsejsonld = new JSONLDTransform()
  parsejsonld.on('error', function(err) {
    req.log.error(err)
    next(err)
  })
  parsejsonld.on('context', function(c, scope){
    req.log.debug('context', c, scope)
  })
  parsejsonld.on('graph', function(g){
    adder.write(g)
    res.header('location', '/graphs/'+graph_key)
  })

  req.pipe(parsejsonld).pipe(adder)
}

function delete_graph(req, res, next) {
  var remover
    , graph_key = req.params[0]
    , results = []

  if (!req.bucket) {
    return next(new Error('Must run dbconnect handler before load_graph route'))
  }

  remover = new CouchbaseWritable({operation:'remove'}, req.bucket, graph_key)
  remover.on('error', function(err) {
    req.log.error(err)
    next(err)
  })
  remover.on('meta', function(meta) {
    results.push(meta)
  })
  remover.on('finish', function() {
    res.json(200, results)
    next()
  })

  req.bucket.get(graph_key, function(err, doc, meta) {
    if (err) {
      req.log.error(err)
      return next(err)
    }
    new ArrayReadable({}, doc['@graph'].concat([doc])).pipe(remover)
  })
}

function add_object(req, res, next) {
  var parsejsonld
    , adder
    , graph_key = req.params[0]
    , results = []

  if (!req.bucket) {
    return next(new Error('Must run dbconnect handler before add_object route'))
  }

  parsejsonld = new JSONLDTransform()
  parsejsonld.on('error', function(err) {
    req.log.error(err)
    next(err)
  })
  parsejsonld.on('context', function(c, scope){
    req.log.debug('context', c, scope)
  })
  parsejsonld.on('graph', function(g){
    next(new restify.BadRequestError('cannot post graphs to objects URI'))
  })

  adder = new CouchbaseWritable({operation: 'add'}, req.bucket, graph_key)
  adder.on('error', function(err) {
    req.log.error(err)
    next(err)
  })

  req.bucket.get(graph_key, function(err, doc, meta) {
    if (err) {
      req.log.error(err)
      return next(err)
    }

    adder.on('add', function(o) {
      doc['@graph'].push({ '@id': o['@id'] })
    })
    adder.on('meta', function(meta) {
      if ('id' in meta && meta.id !== graph_key) { // new object
        res.header(
          'location', 
          '/graphs/'+graph_key+'/objects/'+meta.id.split('/')[1])
        results.push(meta)
      }
    })

    parsejsonld.on('end', function() {
      adder.replace(graph_key, doc, function(err){
        if (err) {
          req.log.error(err)
          return next(err)
        }
        res.json(201, results)
        next()
      })
      adder.end()
    })

    req.pipe(parsejsonld).pipe(adder, {end:false})
  })

}

function get(req, res, next) {
  var adder
    , graph_key = req.params[0]
    , key = graph_key

  if (!req.bucket) {
    return next(new Error('Must run dbconnect handler before get route'))
  }

  if (req.params[1])
    key = graph_key + '/' + req.params[1]

  req.bucket.get(key, function(err, doc, meta) {
    if (err) {
      req.log.error(err)
      return next(err)
    }
    res.json(doc)
    next()
  })
}

exports.createServer = function(options) {
  if (!options) options = {}
  var server = restify.createServer(options);
  server.use(restify.requestLogger())
  server.use(dbconnect(options.bucket))
  server.post('/graphs', load_graph)
  server.get(/^\/graphs\/([^\/]+)$/, get)
  server.del(/^\/graphs\/([^\/]+)$/, delete_graph)

  server.post(/^\/graphs\/([^\/]+)\/objects$/, add_object)
  server.on('uncaughtException', function (req, res, route, e) {
    server.log.fatal(e.stack)
  })
  return server
}
