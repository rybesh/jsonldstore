var restify = require('restify')
  , Readable = require('stream').Readable

  , g1 =
    { '@context': {}
    , '@id': '/_graphs/my-test-graph'
    , '@graph':
      [ { '@id': '/topicnode/666'
        , '@type': 'http://www.wikidata.org/wiki/Q215627'
        , 'name': 'Emma Goldman'
        , 'place of birth': 'http://www.wikidata.org/wiki/Q4115712'
        }
      , { '@id': 'http://www.wikidata.org/wiki/Q4115712'
        , '@type': 'http://www.wikidata.org/wiki/Q2221906'
        , 'name': 'Kaunas'
        }
      ]
    }

function respond(req, res, next) {
  console.log('responding')
  var data = new Readable()
  data._read = function(size) {
    data.push(JSON.stringify(g1))
    data.push(null)
  }
  data.emit('readable')
  data.pipe(res)
}

exports.createServer = function() {
  var server = restify.createServer();
  server.get('/_graphs/my-test-graph', respond)
  return server
}
