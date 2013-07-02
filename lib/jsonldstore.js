var restify = require('restify')
  , JSONLDTransform = require('jsonldtransform').JSONLDTransform
  , CouchbaseWritable = require('./cbwritable').CouchbaseWritable
  , _couchbase = null

function dbconnect(bucket) {
  return function(req, res, next) {
    if (_couchbase === null) {
      _couchbase = new CouchbaseWritable()
      _couchbase.connect({ debug: true, bucket: bucket }, function(err){
        if (err) return next(err)
        req.couchbase = _couchbase
        next()
      })
    } else {
        req.couchbase = _couchbase
        req.couchbase.removeAllListeners()
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

  req.couchbase.on('meta', function(meta) {
    req.log.debug(meta)
    results.push(meta)
  })
  req.couchbase.on('finish', function() {
    console.log('couchbase writestream finished')
    res.json(201, results)
    next()
  })

  parsejsonld.on('context', function(o, scope){
    req.log.debug('context', o, scope)
  })
  parsejsonld.on('graph', function(o){
    req.couchbase.write(o)
  })
  
  req.on('end', function(){
    console.log('req readstream ended')
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
