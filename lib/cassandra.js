/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/*  KVStore implementation using Cassandra.
 *
 *  This stores each item as a separate row in Cassandra, having kv_value
 *  and kv_casid columns.  It relies on the new (in 2.0, currently in beta
 *  release) conditional-write functionality of Cassandra to make the CAS
 *  operation work correctly.  Since incrementing the casid won't work
 *  properly in an eventually-consistent setting, we instead set the casid
 *  to a random integer on every write.
 *
 */

const crypto = require('crypto');
const helenus = require('helenus');

const errors = require('../errors');
const makeBlockingProxy = require('./proxy');


const CREATE_TABLE =
  "CREATE TABLE IF NOT EXISTS kvstore (" +
    "kv_key varchar, " +
    "kv_value varchar, " +
    "kv_casid int, " +
    "PRIMARY KEY (kv_key)" +
  ")"
;

const DROP_TABLE =
  "DROP TABLE IF EXISTS kvstore"
;

const GET_ITEM =
  "SELECT kv_value, kv_casid FROM kvstore WHERE kv_key = ?"
;

const SET_ITEM =
  "UPDATE kvstore " +
  "SET kv_value = ?, kv_casid = ? " +
  "WHERE kv_key = ?"  // NOTE: the row will be created if it does not exist
;

const CAS_ITEM =
  "UPDATE kvstore " +
  "SET kv_value = ?, kv_casid = ? " +
  "WHERE kv_key = ? IF kv_casid = ?"
;

const ADD_ITEM =
  "UPDATE kvstore " +
  "SET kv_value = ?, kv_casid = ? " +
  "WHERE kv_key = ? IF NOT EXISTS"
;

const DEL_ITEM =
  "DELETE FROM kvstore WHERE kv_key = ?"
;

const PING = 
  "SELECT kv_key FROM kvstore WHERE kv_key = 'ping'"
;


function CassandraStore(options) {
  this.pool = new helenus.ConnectionPool(options);
}

CassandraStore.prototype.get = function get(key, cb) {
  this.pool.cql(GET_ITEM, [key], function(err, rows) {
    if (err) return cb(err);
    if (!rows.length) return cb(null, null);
    var value = rows[0].get("kv_value").value;
    var casid = rows[0].get("kv_casid").value;
    try {
      value = JSON.parse(value);
    } catch (err) {
      return cb(err);
    }
    return cb(null, {
      value: value,
      casid: casid
    });
  });
};

// Helper used to generate a random casid value on each write.
//
CassandraStore.prototype._casid = function _casid() {
  return crypto.pseudoRandomBytes(2).readUInt16BE(0);
};

CassandraStore.prototype.set = function set(key, value, cb) {
  value = JSON.stringify(value);
  this.pool.cql(SET_ITEM, [value, this._casid(), key], function(err) {
    return cb(err);
  });
};

CassandraStore.prototype.cas = function cas(key, value, casid, cb) {
  value = JSON.stringify(value);
  var query, args;
  if (!casid) {
    query = ADD_ITEM;
    args = [value, this._casid(), key];
  } else {
    query = CAS_ITEM;
    args = [value, this._casid(), key, casid];
  }
  this.pool.cql(query, args, function(err, rows) {
    if (rows.length !== 1) return cb('unexpected result');
    var applied = rows[0][0].value;
    if (!applied) return cb(errors.ERROR_CAS_MISMATCH);
    return cb(null);
  });
};

CassandraStore.prototype.delete = function del(key, cb) {
  this.pool.cql(DEL_ITEM, [key], function(err) {
    return cb(err);
  });
};

CassandraStore.prototype.close = function (cb) {
  this.pool.once('close', cb);
  this.pool.close();
};

CassandraStore.prototype.closeAndRemove = function (cb) {
  var self = this;
  this.pool.cql(DROP_TABLE, [], function(err) {
    if (err) return cb(err);
    self.close(cb);
  });
};

CassandraStore.prototype.ping = function (cb) {
  this.pool.cql(PING, [], function(err) {
    return cb(err);
  });
};

CassandraStore.connect = function (options) {
  if (typeof options.hosts === 'string') {
    options.hosts = options.hosts.split(',');
  }
  options.cqlVersion = options.cqlVersion || "3.0.0";
  var store = new CassandraStore(options);

  var proxy = makeBlockingProxy();
  var done = function(err) {
    proxy._unblock(err, store);
  };
  // If we don't set an error handler here, pool-level errors will
  // show up as exceptions.  If we do set one, even an empty one,
  // then pool-level errors will propagate into callbacks.
  store.pool.on("error", function(){});
  store.pool.connect(function(err) {
    // If we're not auto-creating the schema,
    // then we can't do anything about errors.
    if (!options.create_schema) {
      return done(err);
    }
    // If there was no error, proceed to creating the table.
    if (!err) {
      return store.pool.cql(CREATE_TABLE, [], done);
    }
    // See if the error was due to a missing keysapce.
    // Via regex matching.  The horror...the horror...
    var missingKeyspaceRE = /Could Not Connect To Any Nodes/;
    if (!missingKeyspaceRE.exec(err.toString())) return done(err);
    // Create the keyspace, using a fresh pool.
    // There may be a way to reuse store.pool, but it escapes me...
    var createOptions = {hosts: options.hosts};
    var createPool = new helenus.ConnectionPool(createOptions);
    createPool.once('error', done);
    createPool.connect(function(err) {
      if (err) return done(err);
      createPool.createKeyspace(options.keyspace, function(err) {
        if (err) return done(err);
        createPool.once('close', function(err) {
          if (err) return done(err);
          // Create the table if it's missing.
          store.pool.cql(CREATE_TABLE, [], done);
        });
        createPool.close();
      });
    });
  });
  return proxy;
};


module.exports = CassandraStore;
