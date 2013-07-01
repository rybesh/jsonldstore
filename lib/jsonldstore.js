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
