"use strict";

var restify = require('restify')
  , async = require('async')
  , bunyan = require('bunyan')
  , couchbase = require('couchbase')
  , uuid = require('node-uuid')
  , Transform = require('stream').Transform
  , JSONLDTransform = require('jsonldtransform').JSONLDTransform
  , ArrayReadable = require('./arrayreadable').ArrayReadable
  , CouchbaseWritable = require('./cbwritable').CouchbaseWritable
  , hashurl = require('./hashurl')
  , _bucket = null

function dbconnect(bucketname) {
  return function(req, res, next) {
    req.log.debug(req.method+'\n'+req.url)
    if (_bucket === null) {
      req.log.debug('connecting to bucket: ' + bucketname)
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

function URLAttacher(options, graph_url) {
  options = options || {}
  options.objectMode = true
  if(!(this instanceof URLAttacher))
    return new URLAttacher(options, graph_url)
  Transform.call(this, options)
  this._graph_url = graph_url
}
URLAttacher.prototype = Object.create(
  Transform.prototype, {constructor:{value:URLAttacher}})
URLAttacher.prototype._attachURL = function(o) {
  var url = this._graph_url
  if ('@graph' in o) {
    o['@graph'] = o['@graph'].map(this._attachURL, this)
  } else {
    url += '/objects/'+hashurl(o['@id'])
  }
  if (('url' in o) && o.url != url)
    o.original_url = o.url
  o.url = url
  return o
}
URLAttacher.prototype._transform = function(o, ignored, done) {
  this.push(this._attachURL(o))
  done()
}

function load_graph(req, res, next) {
  var adder
    , addurls
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

  addurls = new URLAttacher(null, req.url+'/'+graph_key)

  parsejsonld = new JSONLDTransform()
  parsejsonld.on('error', function(err) {
    req.log.error(err)
    next(err)
  })
  parsejsonld.on('context', function(c, scope){
    req.log.debug('context', c, scope)
  })
  parsejsonld.on('graph', function(g){
    addurls.write(g)
    res.header('location', req.url+'/'+graph_key)
  })

  req.pipe(parsejsonld).pipe(addurls).pipe(adder)
}

function list_graphs(req, res, next) {
  if (!req.bucket) {
    return next(
      new Error('Must run dbconnect handler before list_graphs route'))
  }

  req.bucket.view('jsonldstore', 'graphs', {stale:false}, 
    function (err, doc) {
      if (err) {
        req.log.error(err)
        return next(err)
      }
      res.json(doc.map(function(o){
        return { '@id': o.key, 'url': req.url+'/'+ o.id }
      }))
      next()
    }
  )
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
    res.json(results)
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
    , addurls
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
    next(new restify.BadRequestError('cannot POST graphs to objects URI'))
  })

  addurls = new URLAttacher(null, req.url+'/'+graph_key)

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

    req.pipe(parsejsonld).pipe(addurls).pipe(adder, {end:false})
  })

}

function get(req, res, next) {
  var adder
    , graph_key = req.params[0]
    , object_key = req.params[1]
    , key = graph_key

  if (!req.bucket) {
    return next(new Error('Must run dbconnect handler before get route'))
  }

  if (object_key)
    key = graph_key + '/' + object_key

  req.bucket.get(key, function(err, doc, meta) {
    if (err) {
      if (err.code === couchbase.errors.keyNotFound) {
        err = new restify.NotFoundError()
      } else {
        req.log.error(err)
      }
      return next(err)
    }
    res.json(doc)
    next()
  })
}

function get_objects(req, res, next) {
  var adder
    , graph_key = req.params[0]
    , object_keys

  if (!req.bucket) {
    return next(
      new Error('Must run dbconnect handler before get_objects route'))
  }

  req.bucket.get(graph_key, function(err, doc, meta) {
    if (err) {
      req.log.error(err)
      return next(err)
    }
    object_keys = doc['@graph'].map(function(o){
      return graph_key + '/' + hashurl(o['@id'])
    })
    req.bucket.get(object_keys, null, function(err, doc, meta) {
      if (err) {
        req.log.error(err)
        return next(err)
      }
      res.json(doc)
      next()
    })
  })
}

function put_object(req, res, next) {
  var graph_key = req.params[0]
    , object_key = req.params[1]
    , key = graph_key + '/' + object_key
    , parsejsonld
    , replacer
    , results = []

  if (!req.bucket) {
    return next(
      new Error('Must run dbconnect handler before put_object route'))
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
    next(new restify.BadRequestError('cannot PUT graphs to objects URI'))
  })

  replacer = new CouchbaseWritable({operation:'replace'}, req.bucket, graph_key)
  replacer.on('error', function(err) {
    req.log.error(err)
    next(err)
  })
  replacer.on('meta', function(meta) {
    results.push(meta)
  })
  replacer.on('finish', function() {
    res.json(results)
    next()
  })

  req.pipe(parsejsonld).pipe(replacer)
}

function delete_object(req, res, next) {
  var key = req.params[0] + '/' + req.params[1]

  if (!req.bucket) {
    return next(
      new Error('Must run dbconnect handler before delete_object route'))
  }

  req.bucket.remove(key, function(err, meta) {
    if (err) {
      req.log.error(err)
      return next(err)
    }
    res.json(meta)
    next()
  })
}

exports.createServer = function(options) {
  if (!options) options = {}

  var server = restify.createServer(options);
  server.use(restify.requestLogger())
  server.use(restify.CORS())
  server.use(restify.fullResponse())
  server.use(dbconnect(options.bucket))

  server.get('/graphs', list_graphs)
  server.post('/graphs', load_graph)

  server.get(/^\/graphs\/([^\/]+)$/, get)
  server.del(/^\/graphs\/([^\/]+)$/, delete_graph)

  server.get(/^\/graphs\/([^\/]+)\/objects$/, get_objects)
  server.post(/^\/graphs\/([^\/]+)\/objects$/, add_object)

  server.get(/^\/graphs\/([^\/]+)\/objects\/([^\/]+)$/, get)
  server.put(/^\/graphs\/([^\/]+)\/objects\/([^\/]+)$/, put_object)
  server.del(/^\/graphs\/([^\/]+)\/objects\/([^\/]+)$/, delete_object)

  server.on('uncaughtException', function (req, res, route, e) {
    server.log.fatal(e.stack)
  })
  return server
}
