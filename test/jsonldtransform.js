var JSONLDTransform = require('../lib/jsonldtransform').JSONLDTransform
  , Writable = require('stream').Writable
  , util = require('util')
  , fs = require('fs')

module.exports = 
  { setUp: function(callback) {
      var self = this
      self.transform = new JSONLDTransform()
      self.check = new Writable({objectMode:true})
      self.expect = function(test, expecting) {
        self.test = test
        self.expecting = expecting
        self.test.expect(self.expecting.length)
      }
      function checknext(o) {
        self.test.deepEqual(o, self.expecting.shift())
        if (self.expecting.length == 0) self.test.done()
      }
      self.check._write = function(o, ignore, callback) {
        checknext(o)
        callback()
      }
      self.transform.on('context', function(o){
        checknext('context')
        checknext(o)
      })
      self.transform.on('graph', function(o){
        checknext('graph')
        checknext(o)
      })
      self.transform.on('readable', function(){
        checknext('readable')
      })
      self.transform.on('pipe', function(){
        checknext('pipe')
      })
      self.transform.on('finish', function(){
        checknext('finish')
      })
      self.transform.on('end', function(){
        checknext('end')
      })
      self.transform.on('error', function(e){
        self.test.ifError(e)
      })
      callback()
    }
//------------------------------------------------------------------------------
  , 'plain JSON': function(test) {
      this.expect(test,
        [ 'pipe'
        , 'readable'
        , { "name": "Manu Sporny"
          , "homepage": "http://manu.sporny.org/"
          , "image": "http://manu.sporny.org/images/manu.png"
          }
        , 'finish' // done writing
        , 'end'    // done reading
        ])
      fs.createReadStream('test/data/plain_json.json')
        .pipe(this.transform).pipe(this.check)
    }
//------------------------------------------------------------------------------
  , 'simple JSON-LD': function(test) {
      this.expect(test,
        [ 'pipe'
        , 'readable'
        , { "http://schema.org/name": "Manu Sporny"
          , "http://schema.org/url": { "@id": "http://manu.sporny.org/" }
          , "http://schema.org/image": { "@id": "http://manu.sporny.org/images/manu.png" }
          }
        , 'finish' // done writing
        , 'end'    // done reading
        ])
      fs.createReadStream('test/data/simple_jsonld.json')
        .pipe(this.transform).pipe(this.check)
    }
//------------------------------------------------------------------------------
  , 'just context': function(test) {
      this.expect(test,
        [ 'pipe'
        , 'context'
        ,  { "name": "http://schema.org/name"
           , "image": 
             { "@id": "http://schema.org/image"
             , "@type": "@id"
             }
           , "homepage": 
             { "@id": "http://schema.org/url"
             , "@type": "@id"
             }
           }
        , 'finish' // done writing
        , 'end'    // done reading
        ])
      fs.createReadStream('test/data/just_context.json')
        .pipe(this.transform).pipe(this.check)
    }
//------------------------------------------------------------------------------
  , 'referenced context': function(test) {
      this.expect(test,
        [ 'pipe'
        , 'context'
        , 'http://json-ld.org/contexts/person.jsonld'
        , 'readable'
        , { "name": "Manu Sporny"
          , "homepage": "http://manu.sporny.org/" 
          , "image": "http://manu.sporny.org/images/manu.png"
          }
        , 'finish' // done writing
        , 'end'    // done reading
        ])
      fs.createReadStream('test/data/referenced_context.json')
        .pipe(this.transform).pipe(this.check)
    }
//------------------------------------------------------------------------------
  , 'inline context': function(test) {
      this.expect(test,
        [ 'pipe'
        , 'context'
        ,  { "name": "http://schema.org/name"
           , "image": 
             { "@id": "http://schema.org/image"
             , "@type": "@id"
             }
           , "homepage": 
             { "@id": "http://schema.org/url"
             , "@type": "@id"
             }
           }
        , 'readable'
        , { "name": "Manu Sporny"
          , "homepage": "http://manu.sporny.org/"
          , "image": "http://manu.sporny.org/images/manu.png"
          }
        , 'finish' // done writing
        , 'end'    // done reading
        ])
      fs.createReadStream('test/data/inline_context.json')
        .pipe(this.transform).pipe(this.check)
    }
//------------------------------------------------------------------------------
  , 'multiple contexts': function(test) {
      this.expect(test,
        [ 'pipe'
        , 'context'
        , 'http://example.org/contexts/person.jsonld'
        , 'readable'
        , { "name": "Manu Sporny"
          , "homepage": "http://manu.sporny.org/" 
          , "depiction": "http://twitter.com/account/profile_image/manusporny"
          }
        , 'context'
        , 'http://example.org/contexts/place.jsonld'
        , 'readable'
        , { "name": "The Empire State Building"
          , "description": "The Empire State Building is a 102-story landmark in New York City." 
          , "geo": { "latitude": "40.75", "longitude": "73.98" }
          }
        , 'finish' // done writing
        , 'end'    // done reading
        ])
      fs.createReadStream('test/data/multiple_contexts.json')
        .pipe(this.transform).pipe(this.check)
    }
//------------------------------------------------------------------------------
  , 'scoped contexts': function(test) {
      this.expect(test,
        [ 'pipe'
        , 'context'
        , { "name": "http://example.com/person#name"
          , "details": "http://example.com/person#details"
          }
        , 'readable'
        , { "name": "Markus Lanthaler"
          , "details": 
            { "@context": { "name": "http://example.com/organization#name" }
            , "name": "Graz University of Technology" 
            }
          }
        , 'finish' // done writing
        , 'end'    // done reading
        ])
      fs.createReadStream('test/data/scoped_contexts.json')
        .pipe(this.transform).pipe(this.check)
    }
//------------------------------------------------------------------------------
  , 'combined external and local contexts': function(test) {
      this.expect(test,
        [ 'pipe'
        , 'context'
        , [ "http://json-ld.org/contexts/person.jsonld"
          , { "pic": "http://xmlns.com/foaf/0.1/depiction" }
          ]
        , 'readable'
        , { "name": "Manu Sporny"
          , "homepage": "http://manu.sporny.org/"
          , "pic": "http://twitter.com/account/profile_image/manusporny"
          }
        , 'finish' // done writing
        , 'end'    // done reading
        ])
      fs.createReadStream('test/data/combined_external_and_local_contexts.json')
        .pipe(this.transform).pipe(this.check)
    }
//------------------------------------------------------------------------------
  , 'language map': function(test) {
      this.expect(test,
        [ 'pipe'
        , 'context'
        , { "occupation": 
            { "@id": "ex:occupation", "@container": "@language" } 
          }
        , 'readable'
        , { "name": "Yagyū Muneyoshi"
          , "occupation":
            { "ja": "忍者"
            , "en": "Ninja"
            , "cs": "Nindža"
            }
          }
        , 'finish' // done writing
        , 'end'    // done reading
        ])
      fs.createReadStream('test/data/language_map.json')
        .pipe(this.transform).pipe(this.check)
    }
//------------------------------------------------------------------------------
  , 'set multiple values': function(test) {
      this.expect(test,
        [ 'pipe'
        , 'readable'
        , { "@id": "http://example.org/articles/8"
          , "dc:title": 
            [ { "@value": "Das Kapital"
              , "@language": "de"
              }
            , { "@value": "Capital"
              , "@language": "en"
              }
            ]
          }
        , 'finish' // done writing
        , 'end'    // done reading
        ])
      fs.createReadStream('test/data/set_multiple_values.json')
        .pipe(this.transform).pipe(this.check)
    }
//------------------------------------------------------------------------------
  , 'named graph': function(test) {
      this.expect(test,
        [ 'pipe'
        , 'readable'
        , { '@id': '/topicnode/666'
          , '@type': 'http://www.wikidata.org/wiki/Q215627'
          , 'name': 'Emma Goldman'
          , 'place of birth': 'http://www.wikidata.org/wiki/Q4115712' 
          }
        , 'readable'
          , { '@id': 'http://www.wikidata.org/wiki/Q4115712'
          , '@type': 'http://www.wikidata.org/wiki/Q2221906'
          , 'name': 'Kaunas' 
          }
        , 'graph'
        , { '@id': '/_graphs/test-graph-1'
          , '@graph': ['/topicnode/666','http://www.wikidata.org/wiki/Q4115712']
          }
        , 'finish' // done writing
        , 'end'    // done reading
        ])
      fs.createReadStream('test/data/named_graph.json')
        .pipe(this.transform).pipe(this.check)
    }
//------------------------------------------------------------------------------
  }
