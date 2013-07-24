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
          self.request('POST', '/graphs', function(res) {
            if (res.statusCode !== 201) {
              console.log(
                'Loading '+path+' failed with status: '+res.statusCode)
            }
            if (res.statusCode === 409) {
              console.log('Clearing existing data')
              self.consume(res)
              self.clearGraphs(function() { self.load(path, callback) })
            } else {
              callback(res)
            }
          }))
      }
      self.consume = function(res, done) {
        res.on('readable', res.read)
        if (done) res.on('end', done)
      }
      self.deleteGraphs = function(urls, done) {
        if (urls.length) {
          var url = urls.pop()
          self.request('DELETE', url, function(res){
            if (res.statusCode  !== 200) {
              console.log(
                'deleting '+url+' failed with status: '+res.statusCode)
            }
            self.consume(res)
            self.deleteGraphs(urls, done)
          })
        } else {
          process.nextTick(done)
        }
      }
      self.clearGraphs = function(done) {
        var deleter = new Writable()
        deleter._buffer = ''
        deleter._write = function(chunk, encoding, callback) {
          deleter._buffer += chunk
          callback()
        }
        deleter.on('finish', function(){
          self.deleteGraphs(
            JSON.parse(deleter._buffer).map(function(o){ return o.url }), done)
        })
        self.request('GET', '/graphs', function(res){
          res.pipe(deleter)
        })
      }
      done()
    }
  , tearDown: function(done) { this.clearGraphs(done) }
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
      test.expect(5)
      self.load('test/data/named_graph.json', function(res1) {
        objects_uri = res1.headers.location + '/objects'
        self.consume(res1)
        req = self.request('POST', objects_uri, function(res2) {
          self.consume(res2)
          if (res2.statusCode !== 201) {
            test.fail(res2.statusCode, 201, null, '!==')
            test.done()
          } else {
            test.ok(res2.headers.location, 'location header set')
            test.ok(
              res2.headers.location.indexOf(objects_uri) === 0, 
              (res2.headers.location+' does not start with '+objects_uri))
            test.ok(
              res2.headers.location.length > objects_uri.length,
              res2.headers.location)
            self.request('GET', objects_uri, function(res3) {
              var result, buffer = ''
              res3.on('readable', function() { buffer += res3.read() })
              res3.on('end', function() {
                result = JSON.parse(buffer)
                test.ok(result instanceof Array, buffer)
                test.equal(result.length, 3)
                test.done()
              })
            })
          }
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
        test.done()
      })
      test.expect(1)
      self.load('test/data/named_graph.json', function(res1) {
        self.consume(res1)
        parsejsonld.on('graph', function(o) {
          test.deepEqual(o, 
            { '@id': '/_graphs/test-graph-1'
            , 'url': res1.headers.location
            , 'objects': res1.headers.location + '/objects'
            , '@graph':
              [ { '@id': '/topicnode/666' }
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
  , 'GETting a graph with context': function(test) {
      var self = this
        , parsejsonld = new JSONLDTransform()
      parsejsonld.on('error', function(e) {
        test.ifError(e)
        test.done()
      })
      test.expect(3)
      self.load('test/data/named_graph_with_context.json', function(res1) {
        self.consume(res1)
        parsejsonld.on('context', function(url, scope) {
          test.equal(url, res1.headers.location + '/context')
          test.equal(scope, '')
        })
        parsejsonld.on('graph', function(o) {
          test.deepEqual(o, 
            { '@id': '/_graphs/test-graph-2'
            , 'url': res1.headers.location
            , '@context': res1.headers.location + '/context'
            , 'objects': res1.headers.location + '/objects'
            , '@graph':
              [ { '@id': '/topicnode/666' }
              , { "@id": "http://www.wikidata.org/wiki/Q4115712" }
              , { "@id": "http://www.wikidata.org/wiki/Q37" }
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
  , 'GETting a graph\'s context': function(test) {
      var self = this
        , parsejsonld = new JSONLDTransform()
      parsejsonld.on('error', function(e) {
        test.ifError(e)
        test.done()
      })
      parsejsonld.on('context', function(o, scope) {
        test.deepEqual(o, 
          { 'person': 'http://www.wikidata.org/wiki/Q215627'
          , 'place': "http://www.wikidata.org/wiki/Q618123"
          , 'label': 'http://www.w3.org/2000/01/rdf-schema#'
          , 'place of birth':
            { '@id': 'http://www.wikidata.org/wiki/Property:P19'
            , '@type': '@id'
            }
          , 'country':
            { '@id': 'http://www.wikidata.org/wiki/Property:P17'
            , '@type': '@id'
            }
          })
        test.equal(scope, '')
        test.done()
      })
      test.expect(2)
      self.load('test/data/named_graph_with_context.json', function(res1) {
        self.consume(res1)
        self.request('GET', res1.headers.location + '/context', function(res2) {
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
  , 'GETting a graph by name': function(test) {
      var self = this
      test.expect(1)
      self.load('test/data/named_graph.json', function(res1) {
        self.consume(res1)
        self.request('GET', '/graphs/named//_graphs/test-graph-1', 
          function(res2) {
            self.consume(res2)
            if (res2.statusCode !== 301) {
              test.fail(res2.statusCode, 301, null, '!==')
              test.done()
            } else {
              test.equal(res2.headers.location, res1.headers.location)
              test.done()
            }
          }
        )
      })
    }
//------------------------------------------------------------------------------
  , 'GETting a graph\'s objects': function(test) {
      var self = this
        , expect
        , parsejsonld = new JSONLDTransform()
        , check = new Writable({objectMode:true})
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
        expect = 
          [ { "@id": "/topicnode/666"
            , 'url': (res1.headers.location+'/objects/'
                      + hashurl('/topicnode/666'))
            , "@type": "http://www.wikidata.org/wiki/Q215627"
            , "name": "Emma Goldman"
            , "place of birth": "http://www.wikidata.org/wiki/Q4115712"
            }
          , { "@id": "http://www.wikidata.org/wiki/Q4115712"
            , 'url': (res1.headers.location+'/objects/'
                      + hashurl('http://www.wikidata.org/wiki/Q4115712'))
            , "@type": "http://www.wikidata.org/wiki/Q2221906"
            , "name": "Kaunas"
            }
          ]
        test.expect(expect.length)
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
      parsejsonld.on('error', function(e) {
        test.ifError(e)
        test.done()
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
        expect = 
          [ { "@id": "/topicnode/666"
            , 'url': (res1.headers.location+'/objects/'
                      + hashurl('/topicnode/666'))
            , "@type": "http://www.wikidata.org/wiki/Q215627"
            , "name": "Emma Goldman"
            , "place of birth": "http://www.wikidata.org/wiki/Q4115712"
            }
          ]
        test.expect(expect.length)
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
        expect = 
          [ { "@id": "/topicnode/666"
            , 'url': (res1.headers.location+'/objects/'
                      + hashurl('/topicnode/666'))
            , "@type": "http://www.wikidata.org/wiki/Q215627"
            , "name": "Emma Goldman"
            , "place of birth": "http://www.wikidata.org/wiki/Q4115712"
            , "sex": "http://www.wikidata.org/wiki/Q6581072"
            }
          ]
        test.expect(expect.length)
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
      test.expect(6)
      self.load('test/data/named_graph_with_context.json', function(res1) {
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
              test.equal(
                result[0]['@id'], 
                '/_graphs/test-graph-2')
              test.equal(
                result[0].url, 
                res1.headers.location)
              test.equal(
                result[0]['@context'], 
                res1.headers.location + '/context')
              test.equal(
                result[0]['objects'], 
                res1.headers.location + '/objects')
              test.done()
            })
          }
        })
      })
    }
  }
