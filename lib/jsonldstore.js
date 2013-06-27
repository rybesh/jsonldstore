var restify = require('restify')
  , couchbase = require('couchbase')
  , stream = require('stream')
  , clarinet = require('clarinet')

// function respond(req, res, next) {
//   var data = new stream.Readable()
//   data._read = function(size) {
//     data.push(JSON.stringify(g1))
//     data.push(null)
//   }
//   data.emit('readable')
//   data.pipe(res)
// }

function load(req, res, next) {
  var parsejson = clarinet.createStream()
    , context = null
    , stack = []
  parsejson.on('error', function(err){
    res.send(err)
  })
  parsejson.on('value', function(v){
    if (context.object) context.object[context.key] = v
    if (context.array) context.array.push(v)
  })
  parsejson.on('openobject', function(k){
    stack.push(context)
    context = { key:k, object:{} }
  })
  parsejson.on('key', function(k){
    context.key = k
  })
  parsejson.on('closeobject', function(){
    if (context.object && '@id' in context.object) {
      console.log(context.object)
      var id = context.object['@id']
      context = stack.pop() || null
      if (context) { 
        if (context.array) context.array.push(id)
        if (context.object) context.object[context.key] = id
      }
    }
  })
  parsejson.on('openarray', function(){
    stack.push(context)
    context = { array:[] }
  })
  parsejson.on('closearray', function(){
    if (context.array) {
      var array = context.array
      context = stack.pop() || null
      if (context) { 
        if (context.array) context.array.push(array)
        if (context.object) context.object[context.key] = array
      }
    }
  })
  parsejson.on('end', function(){
    res.send('OK')
  })
  req.pipe(parsejson)
}

exports.createServer = function() {
  var server = restify.createServer();
  server.on('uncaughtException', function (req, res, route, e) {
    console.log(e.stack)
  })
  server.put('/', load)
  return server
}
