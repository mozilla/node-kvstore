// Function to create a blocking proxy for a yet-to-be-established connection.
// This returns an object that looks and acts just like a kvstore connection,
// but whose method calls all transparently block until a real connection (or
// connection error) is provided asynchronously.
//
function makeBlockingProxy() {
  // The proxy object to return.
  var proxy = {};

  // Variables to hold the connection, or connection error, once established.
  var dbConnection = null;
  var dbError = null;

  // List of calls that are blocked waiting for the connection to be provided.
  var waitList = [];

  // Create a transparently-blocking method that proxies to the named method
  // on the underlying connection.
  //
  function makeBlockingMethod(methodName) {
    return function() {
      if (dbConnection !== null) {
        // The connection is ready, immediately call the underlying method.
        dbConnection[methodName].apply(dbConnection, arguments);
      } else if (dbError !== null) {
        // The connection errored out, call the callback with an error.
        // All kvstore methods take a callback as their final argument.
        arguments[arguments.length - 1].call(undefined, dbError);
      } else {
        // The connection is pending, add this call to the waitlist.
        waitList.push({ methodName: methodName, args: arguments });
      }
    };
  }
  proxy.get = makeBlockingMethod("get");
  proxy.set = makeBlockingMethod("set");
  proxy.cas = makeBlockingMethod("cas");
  proxy.delete = makeBlockingMethod("delete");
  proxy.close = makeBlockingMethod("close");

  // Private method which is called to provide the connection once established.
  // This will continue execution of any waiting calls.
  //
  proxy._unblock = function _unblock(err, db) {
    // Record the connection or error into the closed-over variables.
    // If the connection was successful, optimize future use of the proxy
    // proxy by copying over methods from the underlying connection.
    if (err) {
      dbError = err;
    } else {
      dbConnection = db;
      proxy.get = db.get.bind(db);
      proxy.set = db.set.bind(db);
      proxy.cas = db.cas.bind(db);
      proxy.delete = db.delete.bind(db);
      proxy.close = db.close.bind(db);
      proxy.ping = db.ping.bind(db);
      proxy.connection = db;
    }
    // Resume any calls that are waiting for the connection.
    // By re-calling the named method on the proxy object, we avoid duplicating
    // the connection-or-error fulfillment logic from makeBlockingMethod().
    waitList.forEach(function(blockedCall) {
      process.nextTick(function() {
        proxy[blockedCall.methodName].apply(proxy, blockedCall.args);
      });
    });
    // Clean up so that things can be GC'd.
    waitList = null;
    delete proxy._unblock;
  };

  return proxy;
}

module.exports = makeBlockingProxy;
