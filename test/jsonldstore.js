var fs = require('fs')
  , http = require('http')
  , Writable = require('stream').Writable
  , JSONLDTransform = require('jsonldtransform').JSONLDTransform

module.exports =
  { setUp: function(done) {
      var self = this
      this.request = function(method, path, callback){
        var options = 
          { hostname: '0.0.0.0'
          , port: 8080
          , method: method
          , path: path
          }
        return http.request(options, callback)
      }
      this.load = function(path, callback) {
        fs.createReadStream(path).pipe(self.request('POST', '/', callback))  
      }
      this.nowhere = new Writable()
      this.nowhere._write = function(chunk, encoding, done) { done() }
      done()
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
//------------------------------------------------------------------------------
  , 'GETting a graph': function(test) {
      var self = this
        , parsejsonld = new JSONLDTransform()
      parsejsonld.on('error', function(e) {
        test.ifError(e)
      })
      parsejsonld.on('graph', function(o) {
        test.deepEqual(o, 
          { '@id': '/_graphs/test-graph-1' 
          , '@graph':
              [ { "@id": "/topicnode/666" }
              , { "@id": "http://www.wikidata.org/wiki/Q4115712" }
              ]
          })
        test.done()
      })
      test.expect(1)
      self.load('test/data/named_graph.json', function(res1) {
        res1.pipe(self.nowhere)
        self.request('GET', res1.headers.location, function(res2) {
          if (res2.statusCode !== 200) {
            test.fail(res2.statusCode, 200, null, '!==')
            test.done()
          } else {
            res2.pipe(parsejsonld).pipe(self.nowhere)
          }
        }).end()
      })
    }
  }
