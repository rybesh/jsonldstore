"use strict";

var restify = require('restify')
  , couchbase = require('couchbase')
  , uuid = require('node-uuid')
  , Transform = require('stream').Transform
  , JSONLDTransform = require('jsonldtransform').JSONLDTransform
  , ArrayReadable = require('./arrayreadable').ArrayReadable
  , CouchbaseWritable = require('./cbwritable').CouchbaseWritable
  , hashurl = require('./hashurl')
  , _ = require('underscore')
  , _db = null

function dbconnect(bucketname) {
  return function(req, res, next) {
    req.log.debug(req.method+'\n'+req.url)
    if (_db === null) {
      req.log.debug('connecting to bucket: ' + bucketname)

      _db = new couchbase.Connection(
        { bucket: bucketname },
        function(err) {
          if (err) {
            req.log.error(err, err.code)
            return next(err)
          }
          req.db = _db
          next()
        })

    } else {
      req.db = _db
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
    , graph = null
    , context = null
    , parsejsonld
    , graph_key = uuid.v4()
    , results = []
    , failure = null

  if (!req.db) {
    return next(new Error('Must run dbconnect handler before load_graph route'))
  }

  adder = new CouchbaseWritable({operation: 'add'}, req.db, graph_key)
  adder.on('error', function(err) {
    req.log.error(err, err.code)
    next(err)
  })
  adder.on('add',  function(key, o) { added_keys.push(key) })
  adder.on('result', function(result)   { results.push(result) })
  adder.on('finish', function() {
    if (failure) {
      added_keys.forEach(function(key){ req.db.remove(key, function(){}) })
      next(failure)
    }
  })

  addurls = new URLAttacher(null, req.url+'/'+graph_key)

  parsejsonld = new JSONLDTransform()
  parsejsonld.on('error', function(err) {
    req.log.error(err, err.code)
    next(err)
  })
  parsejsonld.on('context', function(c, scope){
    context = c
    // TODO: respect scope
    req.db.add(graph_key + '/context', {'@context':c}, function(err, result) {
      if (err) {
        req.log.error(err, err.code)
        return next(err)
      }
    })
  })
  parsejsonld.on('graph', function(g){
    graph = g
  })
  parsejsonld.on('finish', function(){
    // Index the graph key by the graph @id. Do not allow duplicate @ids.
    req.db.add('graph_name/'+hashurl(graph['@id']), graph_key,
      function(err, result) {
        if (err) {
          if (err.code === couchbase.errors.keyAlreadyExists) {
            failure = new restify.ConflictError(
              '/graphs/named/'+graph['@id']+' already exists')
          } else {
            req.log.error(err, err.code)
            next(err)
          }
          return
        }
        if (context) graph['@context'] = req.url+'/'+graph_key+'/context'
        graph['objects'] = req.url+'/'+graph_key+'/objects'
        req.db.add(graph_key, addurls.attachURL(graph), function(err, result){
          if (err) {
            req.log.error(err, err.code)
            return next(err)
          }
          results.push(result)
          res.json(201, results, { location: req.url+'/'+graph_key })
          next()
        })
      })
  })

  req.pipe(parsejsonld).pipe(addurls).pipe(adder)
}

function list_graphs(req, res, next) {
  if (!req.db) {
    return next(
      new Error('Must run dbconnect handler before list_graphs route'))
  }
  req.db.view('jsonldstore', 'graphs', {stale:false}).query(
    {},
    function (err, doc) {
      if (err) {
        req.log.error(err, err.code)
        return next(err)
      }
      res.json(200, doc.map(
        function(o) {
          return { '@id': o.key
                 , 'url': req.url+'/'+o.id
                 , '@context': o.value
                 , 'objects': req.url+'/'+o.id+'/objects'
                 }
        }))
      next()
    }
  )
}

function delete_graph(req, res, next) {
  var remover
    , graph_key = req.params[0]
    , results = []

  if (!req.db) {
    return next(new Error('Must run dbconnect handler before load_graph route'))
  }

  remover = new CouchbaseWritable({operation:'remove'}, req.db, graph_key)
  remover.on('error', function(err) {
    req.log.error(err, err.code)
    next(err)
  })
  remover.on('result', function(result) {
    results.push(result)
  })
  req.db.get(graph_key, function(err, results) {
    if (err) {
      if (err.code === couchbase.errors.keyNotFound) {
        res.send(200)
        return next()
      } else {
        req.log.error(err, err.code)
        return next(err)
      }
    }
    remover.on('finish', function() {
      req.db.remove('graph_name/'+hashurl(results.value['@id']),
        function(err){
          if (err && err.code !== couchbase.errors.keyNotFound) {
            req.log.error(err, err.code)
            return next(err)
          }
          req.db.remove(graph_key+'/context',
            function(err){
              if (err && err.code !== couchbase.errors.keyNotFound) {
                req.log.error(err, err.code)
                return next(err)
              }
              res.json(200, results)
              next()
            })
        })
    })
    new ArrayReadable(
      {},
      results.value['@graph'].concat([results.value])).pipe(remover)
  })
}

function add_object(req, res, next) {
  var parsejsonld
    , adder
    , addurls
    , graph_key = req.params[0]
    , object_key
    , results = []

  if (!req.db) {
    return next(new Error('Must run dbconnect handler before add_object route'))
  }

  parsejsonld = new JSONLDTransform()
  parsejsonld.on('error', function(err) {
    req.log.error(err, err.code)
    next(err)
  })
  parsejsonld.on('context', function(c, scope){
    req.log.debug('context', c, scope)
  })
  parsejsonld.on('graph', function(g){
    next(new restify.BadRequestError('cannot POST graphs to objects URI'))
  })

  addurls = new URLAttacher(null, req.url+'/'+graph_key)

  adder = new CouchbaseWritable({operation: 'add'}, req.db, graph_key)
  adder.on('error', function(err) {
    req.log.error(err, err.code)
    next(err)
  })

  req.db.get(graph_key, function(err, result) {
    if (err) {
      req.log.error(err, err.code)
      return next(err)
    }

    adder.on('add', function(key, o) {
      object_key = key
      result.value['@graph'].push({ '@id': o['@id'], url: req.url+'/'+key })
    })
    adder.on('result', function(result) {
      if ('id' in result && result.id !== graph_key) // new object
        results.push(result)
    })
    adder.on('finish', function() {
      req.db.replace(graph_key, result.value, function(err){
        if (err) {
          req.log.error(err, err.code)
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

  if (!req.db) {
    return next(new Error('Must run dbconnect handler before get route'))
  }

  if (object_key)
    key = graph_key + '/' + object_key

  req.db.get(key, function(err, result) {
    if (err) {
      if (err.code === couchbase.errors.keyNotFound) {
        err = new restify.NotFoundError()
      } else {
        req.log.error(err, err.code)
      }
      return next(err)
    }
    res.json(200, result.value)
    next()
  })
}

function get_named_graph(req, res, next) {
  var graph_name = req.params[0]

  if (!req.db) {
    return next(new Error('Must run dbconnect handler before get route'))
  }

  req.db.get('graph_name/'+hashurl(graph_name),
    function(err, result) {
      if (err) {
        if (err.code === couchbase.errors.keyNotFound) {
          err = new restify.NotFoundError()
        } else {
          req.log.error(err, err.code)
        }
        return next(err)
      }
      res.json(301
        , { code: 'MovedPermanently'
          , message: ("The graph named '"+graph_name+
                      "' can be found at /graphs/"+result.value)
          }
        , { location: '/graphs/'+result.value })
      next()
    }
  )
}

function get_objects(req, res, next) {
  var adder
    , graph_key = req.params[0]
    , object_keys

  if (!req.db) {
    return next(
      new Error('Must run dbconnect handler before get_objects route'))
  }

  req.db.get(graph_key, function(err, result) {
    if (err) {
      req.log.error(err, err.code)
      return next(err)
    }
    object_keys = result.value['@graph'].map(
      function(o){
        return graph_key + '/' + hashurl(o['@id'])
      })
    req.db.getMulti(object_keys, null, function(err, results) {
      if (err) {
        req.log.error(err, err.code)
        return next(err)
      }
      res.json(200, _.map(results, function (v, k) { return v.value }))
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

  if (!req.db) {
    return next(
      new Error('Must run dbconnect handler before put_object route'))
  }

  parsejsonld = new JSONLDTransform()
  parsejsonld.on('error', function(err) {
    req.log.error(err, err.code)
    next(err)
  })
  parsejsonld.on('context', function(c, scope){
    req.log.debug('context', c, scope)
  })
  parsejsonld.on('graph', function(g){
    next(new restify.BadRequestError('cannot PUT graphs to objects URI'))
  })

  replacer = new CouchbaseWritable({operation:'replace'}, req.db, graph_key)
  replacer.on('error', function(err) {
    req.log.error(err, err.code)
    next(err)
  })
  replacer.on('result', function(result) {
    results.push(result)
  })
  replacer.on('finish', function() {
    res.json(200, results)
    next()
  })

  req.pipe(parsejsonld).pipe(replacer)
}

function delete_object(req, res, next) {
  var key = req.params[0] + '/' + req.params[1]

  if (!req.db) {
    return next(
      new Error('Must run dbconnect handler before delete_object route'))
  }

  req.db.remove(key, function(err, result) {
    if (err) {
      req.log.error(err, err.code)
      return next(err)
    }
    res.json(200, result)
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

  server.get(/^\/graphs\/([^\/]+)\/(context)$/, get)

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
