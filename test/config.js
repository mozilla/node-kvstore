/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

const fs = require('fs');
const convict = require('convict');

const AVAILABLE_BACKENDS = ["memory", "mysql", "memcached", "dynamodb"];


var conf = module.exports = convict({
  kvstore: {
    backend: {
      format: AVAILABLE_BACKENDS,
      default: "memory",
      env: 'KVSTORE_BACKEND'
    },
    available_backends: {
      doc: "List of available key-value stores",
      default: AVAILABLE_BACKENDS
    }
  },
  mysql: {
    user: {
      default: 'root',
      env: 'MYSQL_USERNAME'
    },
    password: {
      default: '',
      env: 'MYSQL_PASSWORD'
    },
    database: {
      default: 'test',
      env: 'MYSQL_DATABASE'
    },
    host: {
      default: '127.0.0.1',
      env: 'MYSQL_HOST'
    },
    port: {
      default: '3306',
      env: 'MYSQL_PORT'
    },
    create_schema: {
      default: true,
      env: 'CREATE_MYSQL_SCHEMA'
    },
    max_query_time_ms: {
      doc: "The maximum amount of time we'll allow a query to run before considering the database to be sick",
      default: 5000,
      format: 'duration',
      env: 'MAX_QUERY_TIME_MS'
    },
    max_reconnect_attempts: {
      doc: "The maximum number of times we'll attempt to reconnect to the database before failing all outstanding queries",
      default: 3,
      format: 'nat'
    }
  },
  memcached: {
    hosts: {
      default: '127.0.0.1:11211',
      format: '*',
      env: 'MEMCACHED_HOSTS'
    },
    lifetime: {
      default: 10000,
      env: 'MEMCACHED_LIFETIME'
    }
  },
  dynamodb: {
    region: {
      default: 'us-west-2',
      env: 'DYNAMODB_REGION'
    },
    accessKeyId: {
      default: '',
      env: 'AWS_KEY'
    },
    secretAccessKey: {
      default: '',
      env: 'AWS_SECRET'
    }
  }
});

// handle configuration files.  you can specify a CSV list of configuration
// files to process, which will be overlayed in order, in the CONFIG_FILES
// environment variable
if (process.env.CONFIG_FILES) {
  var files = process.env.CONFIG_FILES.split(',');
  files.forEach(function(file) {
    if(fs.existsSync(file)) {
      conf.loadFile(file);
    }
  });
}

conf.validate();
