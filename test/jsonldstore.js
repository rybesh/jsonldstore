"use strict";

var fs = require('fs')
  , http = require('http')
  , Writable = require('stream').Writable
  , JSONLDTransform = require('jsonldtransform').JSONLDTransform
  , hashurl = require('../lib/hashurl')

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
      self.load = function(path, callback) {
        fs.createReadStream(path).pipe(
          self.request('POST', '/graphs', callback))
      }
      self.consume = function(res, done) {
        res.on('readable', res.read)
        if (done) res.on('end', done)
      }
      self.deleteGraphs = function(uris, done) {
        if (uris.length) {
          self.request('DELETE', uris.pop(), function(res){
            self.consume(res)
            self.deleteGraphs(uris, done)
          })
        } else {
          process.nextTick(done)
        }
      }
      done()
    }
  , tearDown: function(done) {
      var self = this
        , deleter = new Writable()
      deleter._buffer = ''
      deleter._write = function(chunk, encoding, callback) {
        deleter._buffer += chunk
        callback()
      }
      deleter.on('finish', function(){
        self.deleteGraphs(
          JSON.parse(deleter._buffer).map(function(o){ return o.key }), done)
      })
      self.request('GET', '/graphs', function(res){
        res.pipe(deleter)
      })
    }
//------------------------------------------------------------------------------
  , 'Loading via POST to /graphs': function(test) {
      var self = this
      test.expect(3)
      self.load('test/data/named_graph.json', function(res) {
        test.equal(res.statusCode, 201)
        test.ok(res.headers.location, 'location header set')
        test.ok(
          res.headers.location.slice(0,8) === '/graphs/', 
          'location is: ' + res.headers.location)
        self.consume(res, test.done)
      })
    }
//------------------------------------------------------------------------------
  , 'POST to graph objects URI': function(test) {
      var self = this
        , objects_uri
        , req
      test.expect(3)
      self.load('test/data/named_graph.json', function(res1) {
        objects_uri = res1.headers.location + '/objects'
        self.consume(res1)
        req = self.request('POST', objects_uri, function(res2) {
          if (res2.statusCode !== 201) {
            test.fail(res2.statusCode, 201, null, '!==')
          } else {
            test.ok(res2.headers.location, 'location header set')
            test.ok(
              res2.headers.location.indexOf(objects_uri) === 0, 
              (res2.headers.location+' does not start with '+objects_uri))
            test.ok(
              res2.headers.location.length > objects_uri.length,
              res2.headers.location)
          }
          self.consume(res2, test.done)
        })
        req.write(JSON.stringify(
          { "@id": "/topicnode/123"
          , "@type": "http://www.wikidata.org/wiki/Q215627"
          , "name": "Ryan Shaw"
          }))
        req.end()
      })
  }
//------------------------------------------------------------------------------
  , 'GETting a graph': function(test) {
      var self = this
        , parsejsonld = new JSONLDTransform()
      parsejsonld.on('error', function(e) {
        test.ifError(e)
      })
      test.expect(1)
      self.load('test/data/named_graph.json', function(res1) {
        self.consume(res1)
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
        self.request('GET', res1.headers.location, function(res2) {
          if (res2.statusCode !== 200) {
            test.fail(res2.statusCode, 200, null, '!==')
            test.done()
          } else {
            res2.pipe(parsejsonld)
          }
        })
      })
    }
//------------------------------------------------------------------------------
  , 'GETting a graph\'s objects': function(test) {
      var self = this
        , expect
        , parsejsonld = new JSONLDTransform()
        , check = new Writable({objectMode:true})
      expect = 
        [ { "@id": "/topicnode/666"
          , "@type": "http://www.wikidata.org/wiki/Q215627"
          , "name": "Emma Goldman"
          , "place of birth": "http://www.wikidata.org/wiki/Q4115712"
          }
        , { "@id": "http://www.wikidata.org/wiki/Q4115712"
          , "@type": "http://www.wikidata.org/wiki/Q2221906"
          , "name": "Kaunas"
          }
        ]
      test.expect(expect.length)
      parsejsonld.on('error', function(e) {
        test.ifError(e)
      })
      check._write = function(chunk, encoding, callback) {
        test.deepEqual(chunk, expect.shift())
        callback()
      }
      check.on('finish', function(){
        test.done()
      })
      self.load('test/data/named_graph.json', function(res1) {
        self.consume(res1)
        self.request('GET', res1.headers.location+'/objects', function(res2) {
          if (res2.statusCode !== 200) {
            test.fail(res2.statusCode, 200, null, '!==')
            test.done()
          } else {
            res2.pipe(parsejsonld).pipe(check)
          }
        })
      })
    }
//------------------------------------------------------------------------------
  , 'GETting a single object': function(test) {
      var self = this
        , expect
        , parsejsonld = new JSONLDTransform()
        , check = new Writable({objectMode:true})
      expect = 
        [ { "@id": "/topicnode/666"
          , "@type": "http://www.wikidata.org/wiki/Q215627"
          , "name": "Emma Goldman"
          , "place of birth": "http://www.wikidata.org/wiki/Q4115712"
          }
        ]
      test.expect(expect.length)
      parsejsonld.on('error', function(e) {
        test.ifError(e)
      })
      check._write = function(chunk, encoding, callback) {
        test.deepEqual(chunk, expect.shift())
        callback()
      }
      check.on('finish', function(){
        test.done()
      })
      self.load('test/data/named_graph.json', function(res1) {
        self.consume(res1)
        self.request('GET', 
          res1.headers.location+'/objects/'+hashurl('/topicnode/666'), 
          function(res2) {
            if (res2.statusCode !== 200) {
              test.fail(res2.statusCode, 200, null, '!==')
              test.done()
            } else {
              res2.pipe(parsejsonld).pipe(check)
            }
          }
        )
      })
    }
//------------------------------------------------------------------------------
  , 'DELETE a single object': function(test) {
      var self = this
        , object_url
      test.expect(1)
      self.load('test/data/named_graph.json', function(res1) {
        self.consume(res1)
        object_url = res1.headers.location+'/objects/'+hashurl('/topicnode/666')
        self.request('DELETE', object_url, function(res2) {
          self.consume(res2)
          if (res2.statusCode !== 200) {
            test.fail(res2.statusCode, 200, null, '!==')
            test.done()
          } else {
            self.request('GET', object_url, function(res3) {
              self.consume(res3)
              if (res3.statusCode !== 404) {
                test.fail(res3.statusCode, 404, null, '!==')
              } else {
                test.ok(res3.statusCode === 404)
              }
              test.done()
            })
          }
        })
      })
    }
//------------------------------------------------------------------------------
  , 'PUT to update a single object': function(test) {
      var self = this
        , object_url
        , req
        , expect
        , parsejsonld = new JSONLDTransform()
        , check = new Writable({objectMode:true})
      expect = 
        [ { "@id": "/topicnode/666"
          , "@type": "http://www.wikidata.org/wiki/Q215627"
          , "name": "Emma Goldman"
          , "place of birth": "http://www.wikidata.org/wiki/Q4115712"
          , "sex": "http://www.wikidata.org/wiki/Q6581072"
          }
        ]
      test.expect(expect.length)
      parsejsonld.on('error', function(e) {
        test.ifError(e)
      })
      check._write = function(chunk, encoding, callback) {
        test.deepEqual(chunk, expect.shift())
        callback()
      }
      check.on('finish', function(){
        test.done()
      })
      test.expect(1)
      self.load('test/data/named_graph.json', function(res1) {
        self.consume(res1)
        object_url = res1.headers.location+'/objects/'+hashurl('/topicnode/666')
        req = self.request('PUT', object_url, function(res2) {
          self.consume(res2)
          if (res2.statusCode !== 200) {
            test.fail(res2.statusCode, 200, null, '!==')
            test.done()
          } else {
            self.request('GET', object_url, function(res3) {
              if (res3.statusCode !== 200) {
                test.fail(res3.statusCode, 200, null, '!==')
                test.done()
              } else {
                res3.pipe(parsejsonld).pipe(check)
              }
            })
          }
        })
        req.write(JSON.stringify(expect[0]))
        req.end()
      })
    }
//------------------------------------------------------------------------------
  , 'GET /graphs': function(test) {
      var self = this
        , buffer = ''
        , result = null
      test.expect(4)
      self.load('test/data/named_graph.json', function(res1) {
        self.consume(res1)
        self.request('GET', '/graphs', function(res2) {
          if (res2.statusCode !== 200) {
            test.fail(res2.statusCode, 200, null, '!==')
            test.done()
          } else {
            res2.on('readable', function() {
              buffer += res2.read()
            })
            res2.on('end', function() {
              result = JSON.parse(buffer)
              test.ok(result instanceof Array, buffer)
              test.equal(result.length, 1)
              test.equal(result[0].key, res1.headers.location)
              test.equal(result[0].value['@id'], '/_graphs/test-graph-1')
              test.done()
            })
          }
        })
      })
    }
  }
