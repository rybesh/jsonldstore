var fs = require('fs')
  , http = require('http')
  , Writable = require('stream').Writable

module.exports =
  { setUp: function(callback) {
      this.load = function(path, callback) {
        var options = 
          { hostname: '0.0.0.0'
          , port: 8080
          , method: 'POST'
          , path: '/'
          }
        fs.createReadStream(path).pipe(http.request(options, callback))  
      }
      this.nowhere = new Writable()
      this.nowhere._write = function(chunk, encoding, done) { done() }
      callback()
    }
//------------------------------------------------------------------------------
  , 'loading via POST to /': function(test) {
      var self = this
      test.expect(2)
      self.load('test/data/named_graph.json', function(res) {
        test.equal(res.statusCode, 201)
        test.equal(res.headers.location, '/_graphs/test-graph-1')
        res.pipe(self.nowhere)
        test.done()
      })
    }
  }
