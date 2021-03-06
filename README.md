## jsonldstore

A RESTful service for storing and managing JSON-LD.

## example


## why?


## install

```Shell
npm install jsonldstore
```

## test

Running the tests is easier if you clone this repo. To run a test
server (for running the unit tests), use the
[`run_test_server.sh`](https://github.com/rybesh/jsonldstore/blob/master/run_test_server.sh)
script in the root of the repo. Then run the unit tests:

```Shell
npm test
```

To load the test server with some example data (not needed for the
unit tests), use the `--data` flag:

```Shell
./run_test_server.sh --bucket mybucket --data test/data/named_graph.json
```

Note that you must use the long form `--data` rather than `-d` so that
[`nodemon`](https://github.com/remy/nodemon) (if it is installed)
doesn't intercept the option.

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

## license

[MIT](http://opensource.org/licenses/MIT)