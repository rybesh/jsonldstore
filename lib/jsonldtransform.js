var Transform = require('stream').Transform
  , clarinet = require('clarinet')

function JSONLDTransform(options) {
  if (!options) options = {}
  options.objectMode = true
  if (!(this instanceof JSONLDTransform))
    return new JSONLDTransform(options)

  Transform.call(this, options)
  this._parser = clarinet.parser()
  this._state = null
  this._stack = []
  this._parsingContext = false
  var self = this

  function isURI(o) {
    var keys = Object.keys(o)
    return (keys.length === 1 && keys[0] === '@id')
  }

  function isGraph(o) {
    return ('@graph' in o)
  }

  function isContext() {
    return (self._stack.length && 
            self._stack.slice(-1)[0].key == '@context')
  }

  this._parser.onerror = function(e){
    self.emit('error', e)
  }

  this._parser.onvalue = function(v){
    if (self._state.object) {
      if (self._state.key == '@context') {
        self.emit('context', v)
        self._parsingContext = false
      } else {
        self._state.object[self._state.key] = v      
      }
    }
    if (self._state.array) 
      self._state.array.push(v)
  }

  this._parser.onopenobject = function(k){
    if (self._state)
      self._stack.push(self._state)
    self._state = { key:k, object:{} }
    if (self._state.key == '@context')
      self._parsingContext = true
  }

  this._parser.onkey = function(k){
    self._state.key = k
    if (self._state.key == '@context')
      self._parsingContext = true
  }

  this._parser.oncloseobject = function(){
    var value = null
    if (self._state.object) {

      if (isURI(self._state.object)) {
        value = self._state.object

      } else if (isGraph(self._state.object)) {
        self.emit('graph', self._state.object)

      } else if (isContext()) {
        self.emit('context', self._state.object)
        self._parsingContext = false

      } else if (self._parsingContext) {
        value = self._state.object

      } else {
        if (Object.keys(self._state.object).length > 0)
          self.push(self._state.object)
        value = self._state.object['@id']
      }

      self._state = self._stack.pop() || null
      if (self._state && value) { 
        if (self._state.array) self._state.array.push(value)
        if (self._state.object) self._state.object[self._state.key] = value
      }
    }
  }

  this._parser.onopenarray = function(){
    self._stack.push(self._state)
    self._state = { array:[] }
  }

  this._parser.onclosearray = function(){
    if (self._state.array) {
      var array = self._state.array
      self._state = self._stack.pop() || null
      if (self._state) { 
        if (self._state.array) self._state.array.push(array)
        if (self._state.object) self._state.object[self._state.key] = array
      }
    }
  }
}

JSONLDTransform.prototype = Object.create(
  Transform.prototype, { constructor: { value: JSONLDTransform }})

JSONLDTransform.prototype._transform = function(chunk, encoding, done) {
  if (this._parser.closed)
    this.push(null)
  else
    this._parser.write(chunk.toString('utf8'))
  done()
}

exports.JSONLDTransform = JSONLDTransform
