const AWS = require('aws-sdk');
const errors = require('../errors');

function DynamoStore(options) {
	this.client = new AWS.DynamoDB(options);
}

DynamoStore.connect = function (options) {
	return new DynamoStore(options);
};

function parse(data) {
	var result = {};
	try {
		result.value = JSON.parse(data.Item.kv_value.S);
	}
	catch (e) {}
	result.casid = +(data.Item.kv_casid.N);
	return result;
}

DynamoStore.prototype.get = function get(key, cb) {
	this.client.getItem(
		{
			TableName: 'kvstore',
			Key: {
				kv_key: { S: key }
			}//, ConsistentRead: true (?)
		},
		function (err, data) {
			if (err) { return cb(err); }
			if (!data || !data.Item) { return cb(null, null); }

			var result = parse(data);
			if (!result.value) {
				err = new Error("error parsing value");
			}
			cb(err, result);
		}
	);
};

DynamoStore.prototype.set = function set(key, value, cb) {
	this.cas(key, value, false, cb);
};

DynamoStore.prototype.cas = function cas(key, value, casid, cb) {
	var query = {
		TableName: 'kvstore',
		Key: {
			kv_key: { S: key }
		},
		AttributeUpdates: {
			kv_value: {
				Value: { S: JSON.stringify(value) },
				Action: 'PUT'
			},
			kv_casid: {
				Value: { N: '1' },
				Action: 'ADD'
			}
		}
	};

	if (casid || casid === 0) {
		query.Expected = {
			kv_casid: {
				Value: { N: casid.toString() }
			}
		};
	}

	this.client.updateItem(
		query,
		function (err) {
			if (err) {
				if (err.code === 'ConditionalCheckFailedException') {
					return cb(errors.ERROR_CAS_MISMATCH);
				} else {
					return cb(err.code);
				}
			}
			cb(err);
		}
	);
};

DynamoStore.prototype.delete = function del(key, cb) {
	this.client.deleteItem(
		{
			TableName: 'kvstore',
			Key: {
				kv_key: { S: key }
			}
		},
		function (err) {
			cb(err);
		}
	);
};

DynamoStore.prototype.close = function (cb) {
	process.nextTick(cb);
};

DynamoStore.prototype.ping = function (cb) {
	process.nextTick(cb);
};

module.exports = DynamoStore;
