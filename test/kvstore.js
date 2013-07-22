var assert = require('assert');
var config = require('./config');
var kvstore = require('../index')(config.root());

describe('kvstore', function () {

  var db;

  beforeEach(function () {
    db = kvstore.connect();
  });

  afterEach(function (done) {
    if (db.connection && db.connection.closeAndRemove) {
      db.connection.closeAndRemove(done);
    } else {
      db.close(done);
    }
  });

  it('can set and retrieve keys', function (done) {
    db.set("test-key", "VALUE", function(err) {
      assert.equal(err, null);
      db.get("test-key", function(err, info) {
        assert.equal(err, null);
        assert.equal(info.value, "VALUE");
        done();
      });
    });
  });

  it('can ping', function (done) {
    db.get("test-key", function () {
      db.ping(done);
    });
  });

  it('supports atomic check-and-set', function (done) {
    db.set("test-key", "VALUE", function(err) {
      assert.equal(err, null);
      db.get("test-key", function(err, info) {
        assert.equal(info.value, "VALUE");
        db.cas("test-key", "OTHER-VALUE-ONE", info.casid, function(err) {
          assert.equal(err, null);
          db.cas("test-key", "OTHER-VALUE-TWO", info.casid, function(err) {
            assert.equal(err, kvstore.errors.ERROR_CAS_MISMATCH);
            db.get("test-key", function(err, info) {
              assert.equal(err, null);
              assert.equal(info.value, "OTHER-VALUE-ONE");
              done();
            });
          });
        });
      });
    });
  });

});
