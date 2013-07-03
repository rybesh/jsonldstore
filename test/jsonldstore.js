var fs = require('fs')
  , http = require('http')
  , Writable = require('stream').Writable
  , JSONLDTransform = require('jsonldtransform').JSONLDTransform

module.exports =
  { setUp: function(done) {
      var self = this
      self.request = function(method, path, callback){
        var options = 
          { hostname: '0.0.0.0'
          , port: 8080
          , method: method
          , path: path
          }
          , req = http.request(options, callback)
        if (method === 'GET' || method === 'DELETE')
          req.end()
        return req
      }
      self.graphs = []
      self.load = function(path, callback) {
        fs.createReadStream(path).pipe(self.request('POST', '/graphs', 
          function(res) {
            self.graphs.push(res.headers.location)
            callback(res)
          }))
      }
      self.nowhere = new Writable()
      self.nowhere._write = function(chunk, encoding, done) { done() }
      done()
    }
  , tearDown: function(done) {
      var self = this
      self.graphs.forEach(function (uri) {
        self.request('DELETE', uri)
      })
      done()
    }
//------------------------------------------------------------------------------
  , 'Loading via POST to /graphs/': function(test) {
      var self = this
      test.expect(2)
      self.load('test/data/named_graph.json', function(res) {
        test.equal(res.statusCode, 201)
        test.ok(
          res.headers.location.slice(0,8) === '/graphs/', 
          'location is: ' + res.headers.location)
        res.pipe(self.nowhere)
        test.done()
      })
    }
//------------------------------------------------------------------------------
//   , 'POST to graph URI': function(test) {
//       var self = this
//         , req
//       test.expect(1)
//       self.load('test/data/named_graph.json', function(res1) {
//         res1.pipe(self.nowhere)
//         req = self.request('POST', res1.headers.location, function(res2) {
//           if (res2.statusCode !== 201) {
//             test.fail(res2.statusCode, 201, null, '!==')
//             test.done()
//           } else {
//             test.equal(res2.headers.location, '/topicnode/123')
//             res2.pipe(self.nowhere)
//             test.done()
//           }
//         })
//         req.write(JSON.stringify(
//           { "@id": "/topicnode/123"
//           , "@type": "http://www.wikidata.org/wiki/Q215627"
//           , "name": "Ryan Shaw"
//           }))
//         req.end()
//       })
//   }
//------------------------------------------------------------------------------
  // , 'GETting a graph': function(test) {
  //     var self = this
  //       , parsejsonld = new JSONLDTransform()
  //     parsejsonld.on('error', function(e) {
  //       test.ifError(e)
  //     })
  //     parsejsonld.on('graph', function(o) {
  //       test.deepEqual(o, 
  //         { '@id': '/_graphs/test-graph-1' 
  //         , '@graph':
  //             [ { "@id": "/topicnode/666" }
  //             , { "@id": "http://www.wikidata.org/wiki/Q4115712" }
  //             ]
  //         })
  //       test.done()
  //     })
  //     test.expect(1)
  //     self.load('test/data/named_graph.json', function(res1) {
  //       res1.pipe(self.nowhere)
  //       self.request('GET', res1.headers.location, function(res2) {
  //         if (res2.statusCode !== 200) {
  //           test.fail(res2.statusCode, 200, null, '!==')
  //           test.done()
  //         } else {
  //           res2.pipe(parsejsonld).pipe(self.nowhere)
  //         }
  //       })
  //     })
  //   }
  }
