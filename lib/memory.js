/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/**
 * KVStore implementation using in-memory data.
 *
 */

const Hoek = require('hoek');
const errors = require('../errors');

// Hoek's clone function only works on objects.
// Wrap it so that other datatypes are returned unchanged.
function clone(value) {
  if (typeof value !== 'object') return value;
  return Hoek.clone(value);
}

function MemoryStore() {
  this.data = {};
}

MemoryStore.connect = function () {
  return new MemoryStore();
};

MemoryStore.prototype.get = function get(key, cb) {
  var self = this;
  process.nextTick(function() {
    if (self.data[key] === undefined) {
      cb(null, null);
    } else {
      // take a copy so caller cannot modify our internal data structures.
      cb(null, {
        value: clone(self.data[key].value),
        casid: self.data[key].casid
      });
    }
  });
};

MemoryStore.prototype.set = function (key, value, cb) {
  value = clone(value);
  var self = this;
  process.nextTick(function() {
    if (self.data[key] === undefined) {
        self.data[key] = {
          value: value,
          casid: 1
        };
    } else {
        self.data[key].value = value;
        self.data[key].casid++;
    }
    cb(null);
  });
};

MemoryStore.prototype.cas = function (key, value, casid, cb) {
  value = clone(value);
  var self = this;
  process.nextTick(function() {
    if (self.data[key] === undefined) {
      if (casid !== null) return cb(errors.ERROR_CAS_MISMATCH);
      self.data[key] = {
        value: value,
        casid: 1
      };
    } else {
      if (self.data[key].casid !== casid)  return cb(errors.ERROR_CAS_MISMATCH);
      self.data[key].value = value;
      self.data[key].casid++;
    }
    cb(null);
  });
};

MemoryStore.prototype.delete = function (key, cb) {
  var self = this;
  process.nextTick(function() {
    delete self.data[key];
    cb(null);
  });
};

MemoryStore.prototype.close = function (cb) {
  this.data = {};
  process.nextTick(cb);
};

MemoryStore.prototype.ping = function (cb) {
  process.nextTick(cb);
};

module.exports = MemoryStore;
