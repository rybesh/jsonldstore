var APIeasy = require('api-easy')
  , assert = require('assert')
  , suite = APIeasy.describe('jsonldstore API')

suite.discuss('When reading from API')
  .discuss('and GET requesting a graph URI')
  .use('localhost', 8080)
  .get('/_graphs/test-graph-1')
  .expect(200,
    { '@context': {}
    , '@id': '/_graphs/test-graph-1'
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
    })
  .export(module)