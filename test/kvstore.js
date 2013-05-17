var assert = require('assert');
var config = require('./config');
var errors = require('../errors');
var kvstore = require('../index')(config.get('kvstore'));

describe('kvstore', function () {

  var db;

  beforeEach(function () {
    db = kvstore.connect();
  });

  afterEach(function (done) {
    if (config.get('kvstore.backend') === 'mysql') {
      db.connection.closeAndRemove(done);
    }
    else {
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

  it('supports atomic check-and-set', function (done) {
    db.set("test-key", "VALUE", function(err) {
      assert.equal(err, null);
      db.get("test-key", function(err, info) {
        assert.equal(info.value, "VALUE");
        db.cas("test-key", "OTHER-VALUE-ONE", info.casid, function(err) {
          assert.equal(err, null);
          db.cas("test-key", "OTHER-VALUE-TWO", info.casid, function(err) {
            assert.equal(err, errors.ERROR_CAS_MISMATCH);
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
