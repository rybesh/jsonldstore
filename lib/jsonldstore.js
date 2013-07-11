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
            req.log.error(err, err.code, 23)
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
URLAttacher.prototype.attachURL = function(o) {
  var url = this._graph_url
  if ('@graph' in o) {
    o['@graph'] = o['@graph'].map(this.attachURL, this)
  } else {
    url += '/objects/'+hashurl(o['@id'])
  }
  if (('url' in o) && o.url != url)
    o.original_url = o.url
  o.url = url
  return o
}
URLAttacher.prototype._transform = function(o, ignored, done) {
  this.push(this.attachURL(o))
  done()
}

function load_graph(req, res, next) {
  var adder
    , added_keys = []
    , addurls
    , parsejsonld
    , graph_key = uuid.v4()
    , results = []
    , failure = null

  if (!req.bucket) {
    return next(new Error('Must run dbconnect handler before load_graph route'))
  }

  adder = new CouchbaseWritable({operation: 'add'}, req.bucket, graph_key)
  adder.on('error', function(err) {
    req.log.error(err, err.code, 79)
    next(err)
  })
  adder.on('add',  function(key, o) { added_keys.push(key) })
  adder.on('meta', function(meta)   { results.push(meta) })
  adder.on('finish', function() {
    if (failure) {
      added_keys.forEach(function(key){ req.bucket.remove(key, function(){}) })
      next(failure)
    }
  })

  addurls = new URLAttacher(null, req.url+'/'+graph_key)

  parsejsonld = new JSONLDTransform()
  parsejsonld.on('error', function(err) {
    req.log.error(err, err.code, 95)
    next(err)
  })
  parsejsonld.on('context', function(c, scope){
    req.log.debug('context', c, scope)
  })
  parsejsonld.on('graph', function(g){
    // Index the graph key by the graph @id. Do not allow duplicate @ids.
    req.bucket.add('graph_name/'+hashurl(g['@id']), graph_key, 
      function(err, meta) {
        if (err) {
          if (err.code === couchbase.errors.keyAlreadyExists) {
            failure = new restify.ConflictError(
              '/graphs/named/'+g['@id']+' already exists')
          } else {
            req.log.error(err, err.code, 110)
            next(err)
          }
          return
        }
        req.bucket.add(graph_key, addurls.attachURL(g), function(err, meta){
          if (err) {
            req.log.error(err, err.code, 117)
            return next(err)
          }
          results.push(meta)
          res.json(201, results, { location: req.url+'/'+graph_key })
          next()
        })
      })
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
        req.log.error(err, err.code, 140)
        return next(err)
      }
      res.json(200, doc.map(function(o){
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
    req.log.error(err, err.code, 162)
    next(err)
  })
  remover.on('meta', function(meta) {
    results.push(meta)
  })
  req.bucket.get(graph_key, function(err, doc, meta) {
    if (err) {
      if (err.code === couchbase.errors.keyNotFound) {
        res.send(200)
        return next()
      } else {
        req.log.error(err, err.code, 170)
        return next(err)
      }
    }
    remover.on('finish', function() {
      req.bucket.remove('graph_name/'+hashurl(doc['@id']), 
        function(err){
          if (err && err.code !== couchbase.errors.keyNotFound) {
            req.log.error(err, err.code, 177)
            return next(err)
          }
          res.json(200, results)
          next()
        })
    })
    new ArrayReadable({}, doc['@graph'].concat([doc])).pipe(remover)
  })
}

function add_object(req, res, next) {
  var parsejsonld
    , adder
    , addurls
    , graph_key = req.params[0]
    , object_key
    , results = []

  if (!req.bucket) {
    return next(new Error('Must run dbconnect handler before add_object route'))
  }

  parsejsonld = new JSONLDTransform()
  parsejsonld.on('error', function(err) {
    req.log.error(err, err.code, 201)
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
    req.log.error(err, err.code, 215)
    next(err)
  })

  req.bucket.get(graph_key, function(err, doc, meta) {
    if (err) {
      req.log.error(err, err.code, 221)
      return next(err)
    }

    adder.on('add', function(key, o) {
      object_key = key
      doc['@graph'].push({ '@id': o['@id'], url: req.url+'/'+key })
    })
    adder.on('meta', function(meta) {
      if ('id' in meta && meta.id !== graph_key) // new object
        results.push(meta)
    })
    adder.on('finish', function() {
      req.bucket.replace(graph_key, doc, function(err){
        if (err) {
          req.log.error(err, err.code, 236)
          return next(err)
        }
        res.json(201, results, { location: req.url+'/'+object_key })
        next()
      })
    })

    req.pipe(parsejsonld).pipe(addurls).pipe(adder)
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
        req.log.error(err, err.code, 267)
      }
      return next(err)
    }
    res.json(200, doc)
    next()
  })
}

function get_named_graph(req, res, next) {
  var graph_name = req.params[0]
  
  if (!req.bucket) {
    return next(new Error('Must run dbconnect handler before get route'))
  }
  
  req.bucket.get('graph_name/'+hashurl(graph_name), 
    function(err, graph_key, meta) {
      if (err) {
        if (err.code === couchbase.errors.keyNotFound) {
          err = new restify.NotFoundError()
        } else {
          req.log.error(err, err.code, 289)
        }
        return next(err)
      }
      res.json(301 
        , { code: 'MovedPermanently'
          , message: ("The graph named '"+graph_name+
                      "' can be found at /graphs/"+graph_key)
          }
        , { location: '/graphs/'+graph_key })
      next()
    }
  )
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
      req.log.error(err, err.code, 316)
      return next(err)
    }
    object_keys = doc['@graph'].map(function(o){
      return graph_key + '/' + hashurl(o['@id'])
    })
    req.bucket.get(object_keys, null, function(err, doc, meta) {
      if (err) {
        req.log.error(err, err.code, 324)
        return next(err)
      }
      res.json(200, doc)
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
    req.log.error(err, err.code, 348)
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
    req.log.error(err, err.code, 360)
    next(err)
  })
  replacer.on('meta', function(meta) {
    results.push(meta)
  })
  replacer.on('finish', function() {
    res.json(200, results)
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
      req.log.error(err, err.code, 384)
      return next(err)
    }
    res.json(200, meta)
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

  server.get(/^\/graphs\/named\/(.+)$/, get_named_graph)

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
