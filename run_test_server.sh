#! /bin/bash

# Use a bucket named 'test'...
SERVER="./bin/jsonldstore -b test $@"

# ...and pipe ouput to the bunyan logviewer. 
LOGVIEW="./node_modules/bunyan/bin/bunyan -o short"

# Run under nodemon if we have it.
if command -v nodemon >/dev/null 2>&1 ; then
    eval "nodemon $SERVER | $LOGVIEW"
else
    eval "$SERVER | $LOGVIEW"
fi




