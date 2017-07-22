const async = require('async');
const request = require('request');
const _ = require('lodash');

const DEFAULT_CONNECTION_STRING = 'http://127.0.0.1:2379';

const CONNECTION_DETAILS = {
    connectionString : null,
    username : null,
    password : null,
    token : null
};

const ETCD_API = {
    put : '/v3alpha/kv/put',
    range : '/v3alpha/kv/range',
    delete : '/v3alpha/kv/deleterange',
    authenticate : '/v3alpha/auth/authenticate'
};

const SUPPORTED_OPERATIONS = {
    CREATE : 'create',
    GET : 'get',
    UPDATE : 'update',
    DELETE : 'delete'
};

function authenticate (callback) {
    var authRequest = {
        name : CONNECTION_DETAILS.username,
        password : CONNECTION_DETAILS.password
    };

    var options = {
        method: 'POST',
        url: CONNECTION_DETAILS.connectionString + ETCD_API.authenticate,
        body: JSON.stringify(authRequest)
    };

    request(options, function (error, response, body) {
        if (error) {
            return callback(error);
        }

        let parsedBody = JSON.parse(response.body);
        CONNECTION_DETAILS.token = parsedBody.token;
        callback();
    });
}


function init (connectionDetails, callback) {
    CONNECTION_DETAILS.connectionString = connectionDetails.connectionString || DEFAULT_CONNECTION_STRING;
    CONNECTION_DETAILS.username = connectionDetails.username;
    CONNECTION_DETAILS.password = connectionDetails.password;

    if (!_.startsWith(CONNECTION_DETAILS.connectionString, 'http')) {
        CONNECTION_DETAILS.connectionString = 'http://' + CONNECTION_DETAILS.connectionString;
    }

    if (CONNECTION_DETAILS.username) {
        authenticate(callback);
    } else {
        callback();
    }
}

function convertGetKeysResponse (kvs) {
    for (let i = 0; i < kvs.length; i++) {
        kvs[i].key = Buffer.from(kvs[i].key, 'base64').toString();
        kvs[i].value = kvs[i].value ? Buffer.from(kvs[i].value, 'base64').toString() : '';
    }
}

function getChildren (parentKey, callback) {
    var getRequest = {
        key : new Buffer(parentKey).toString('base64'),
        range_end : new Buffer(parentKey + 1).toString('base64')
    };

    var options = {
        method: 'POST',
        url: CONNECTION_DETAILS.connectionString + ETCD_API.range,
        body: JSON.stringify(getRequest)
    };

    request(options, function (error, response, body) {
        if (error) {
            return callback(error);
        } else if (response.statusCode != 200) {
            return callback({statusCode : response.statusCode, body : response.body})
        }


        let parsedBody = JSON.parse(response.body);
        delete parsedBody.header;

        if (parsedBody.kvs) {
            convertGetKeysResponse(parsedBody.kvs);
            callback(null, parsedBody.kvs);
        } else {
            callback({err : 'no keys found'});
        }
    });
}

function createKey (key, value, callback) {
    var putRequest = {
        key : new Buffer(key).toString('base64'),
        value : new Buffer(value).toString('base64')
    };

    var options = {
        method: 'POST',
        url: CONNECTION_DETAILS.connectionString + ETCD_API.put,
        body: JSON.stringify(putRequest)
    };

    request(options, function (error, response, body) {
        if (error) {
            return callback(error);
        } else if (response.statusCode != 200) {
            return callback({statusCode : response.statusCode, body : response.body})
        }
        callback();
    });
}

function deleteKey (key, callback) {
    var deleteRequest = {
        key : new Buffer(key).toString('base64')
    };

    var options = {
        method: 'POST',
        url: CONNECTION_DETAILS.connectionString + ETCD_API.delete,
        body: JSON.stringify(deleteRequest)
    };

    request(options, function (error, response, body) {
        if (error) {
            return callback(error);
        } else if (response.statusCode != 200) {
            return callback({statusCode : response.statusCode, body : response.body})
        }
        callback();
    });
}


function performEtcdOperation (operation, params, callback) {
    switch (operation) {
        case SUPPORTED_OPERATIONS.GET :
            getChildren(params.key, callback);
            break;
        case SUPPORTED_OPERATIONS.CREATE :
            createKey(params.key, params.value, callback);
            break;
        case SUPPORTED_OPERATIONS.DELETE :
            deleteKey(params.key, callback);
            break;
        case SUPPORTED_OPERATIONS.UPDATE :
            createKey(params.key, params.value, callback);
    }
}

module.exports = {
    init : init,
    performEtcdOperation : performEtcdOperation,
    SUPPORTED_OPERATIONS : SUPPORTED_OPERATIONS
};
