/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/**
 * Very lightly abstracted key-value storage for PiCL projects.
 *
 * This module provides a simple key-value storage API that abstracts away
 * the details of the underlying storage server.  It explicitly mirrors the
 * model used by the memcache protocol.  In production it's currently intended
 * to be mysql; for local development you can use an in-memory store.
 *
 * To obtain a database connection, call the connect() function:
 *
 *    var kvstore = require('lib/kvstore');
 *    var db = kvstore.connect({<options>});
 *
 * This function takes an options hash to specify details of the underlying
 * storage backend, and will fill in default options from runtime configuration
 * data.  It returns a connection object with the following methods:
 *
 *    get(key, cb(<err>, <res>)):
 *
 *      Get the data stored for the given key.  The result will be an object
 *      with field 'value' giving the stored value, and field 'casid' giving
 *      the current CAS id.  If the key does not exist then the result will be
 *      null.
 *
 *
 *    set(key, value, cb(<err>)):
 *
 *      Unconditionally set the data stored for the given key.
 *
 *
 *    cas(key, value, casid, cb(<err>)):
 *
 *      Check-and-set the data stored for the given key.  The 'casid' should be
 *      a value taken from a previous call to get() for that key, or null to
 *      check that the key does not exist.
 *
 *
 *    delete(key, cb(<err>)):
 *
 *      Unconditionally delete the data stored for the given key.  There is no
 *      conditional delete since AFAIK it's not offered by
 *      couchbase.
 *
 * Here's an example of how these methods might be used:
 *
 *  db.get("mydata", function(err, res) {
 *      if(err) throw err;
 *      console.log("My data is currently: " + res.value);
 *      db.cas("mydata", res.value + "newdata", res.casid, function(err) {
 *          if(err) throw "oh noes there was a write conflict";
 *      });
 *  });
 *
 * Each of the connection methods will transparently block until the underlying
 * storage backend connection is established, which allows calls to connect()
 * to be made synchronously.  If you need to be notified when the underlying
 * connection has been established, pass a callback to connect() like so:
 *
 *    kvstore.connect({<options>}, function(err, db) {
 *        ...do stuff with the db...
 *    }
 *
 */

var Hoek = require('hoek');

module.exports = function (config) {
  // The set of default options to use for new db connections in this process.
  var DEFAULT_OPTIONS = config;

  // The set of available backend names.
  // This will be populated with the loaded sub-modules on demand.
  var AVAILABLE_BACKENDS = DEFAULT_OPTIONS.kvstore.available_backends.reduce(
    function(map, backend) {
      map[backend] = null;
      return map;
    }, {});

  return {
    connect: function(options) {
      options = Hoek.applyToDefaults(DEFAULT_OPTIONS.kvstore, options || {});

      // Load the specified backend implementation
      // if it's not already available.
      var backend = AVAILABLE_BACKENDS[options.backend];
      if (backend === undefined) {
          return;
      }
      if (backend === null) {
          backend = require("./lib/" + options.backend + ".js");
          AVAILABLE_BACKENDS[options.backend] = backend;
      }
      if (options.backend in DEFAULT_OPTIONS) {
        options = Hoek.merge(options, DEFAULT_OPTIONS[options.backend]);
      }

      return backend.connect(options);
    }
  };
};


