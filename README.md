## jsonldstore

A RESTful service for storing and managing JSON-LD.

## example


## why?


## install

```Shell
npm install jsonldstore
```

## test

```Shell
npm test jsonldstore
```

## run

Make sure [couchbase](http://www.couchbase.com/download) is running
somewhere and has a bucket for you to use. Then, run `jsonldstore`:

```Shell
usage: jsonldstore [options]

options:
  -p                 Port to use [8080]
  -a                 Address to use [0.0.0.0]
  -d --data          JSON file with data to load
  -b --bucket        Bucket to load data into [default]
  -s --silent        Suppress log messages from output
  -h --help          Print this list and exit
```

Note that log output is in JSON. To view the logs more comfortably,
pipe them to the [bunyan CLI
tool](https://github.com/trentm/node-bunyan#cli-usage):

```Shell
jsonldstore | bunyan -o short
```

To run a test server (for running the unit tests), use the
[`run_test_server.sh`](https://github.com/rybesh/jsonldstore/blob/master/run_test_server.sh)
script. To load it with some example data, use the `-d` flag:

```Shell
./run_test_server.sh -d test/data/named_graph.json
```

## license

[MIT](http://opensource.org/licenses/MIT)