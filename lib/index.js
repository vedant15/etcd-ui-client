'use strict';

const async = require('async');
const _ = require('lodash');
const grpc = require('grpc');
const fs = require('fs');

const PROTO_PATH = __dirname + '/proto/rpc.proto';
const etcdService = grpc.load(PROTO_PATH).etcdserverpb;

let KVClient = null;
let AuthClient = null;

const callMetadata = new grpc.Metadata();

const DEFAULT_CONNECTION_STRING = '127.0.0.1:2379';


const CONNECTION_DETAILS = {
  connectionString: null,
  username: null,
  password: null,
  token: null,
  sslCertificate: null
};

const SUPPORTED_OPERATIONS = {
  CREATE: 'create',
  GET: 'get',
  UPDATE: 'update',
  DELETE: 'delete'
};

function initializeClients (callback) {
  let channelCreds = null;
  if (CONNECTION_DETAILS.sslCertificate) {
    channelCreds = grpc.credentials.createSsl(fs.readFileSync(CONNECTION_DETAILS.sslCertificate));
  } else {
    channelCreds = grpc.credentials.createInsecure();
  }

  async.series([
    function initializeAuthClient (next) {
      AuthClient = new etcdService.Auth(CONNECTION_DETAILS.connectionString, channelCreds);
      grpc.waitForClientReady(AuthClient, new Date(new Date().getTime() + (1000 * 20)), next);
    },
    function initializeKvClient (next) {
      KVClient = new etcdService.KV(CONNECTION_DETAILS.connectionString, channelCreds);
      grpc.waitForClientReady(KVClient, new Date(new Date().getTime() + (1000 * 20)), next);
    }
  ], callback);
}

function authenticate (callback) {
  if (!CONNECTION_DETAILS.username || !CONNECTION_DETAILS.password) {
    return callback();
  }

  const authRequest = {
    name: CONNECTION_DETAILS.username,
    password: CONNECTION_DETAILS.password
  };

  AuthClient['authenticate'](authRequest, function (error, result) {
    if (error) {
      return callback(error);
    }

    CONNECTION_DETAILS.token =  result  ? result.token : null;
    if (!callMetadata.get('token') || callMetadata.get('token').length < 1) {
      callMetadata.add('token', result.token);
    } else {
      callMetadata.set('token', result.token);
    }

    callback(error);
  });
}


function init(connectionDetails, callback) {
  CONNECTION_DETAILS.connectionString = connectionDetails.connectionString ? connectionDetails.connectionString.trim() : DEFAULT_CONNECTION_STRING;
  CONNECTION_DETAILS.username = connectionDetails.username ? connectionDetails.username.trim() : null;
  CONNECTION_DETAILS.password = connectionDetails.password ? connectionDetails.password.trim() : null;
  CONNECTION_DETAILS.sslCertificate = connectionDetails.sslCertificate ? connectionDetails.sslCertificate.trim() : null;

  async.series([
    initializeClients,
    authenticate
  ], callback);
}

function executeRequest (targetClient, operation, requestBody, callback) {

  async.waterfall([

    (next) => {
      targetClient[operation](requestBody, callMetadata, (err, response) => {
        if (err && err.code === 16) {
          return next(null, null);
        } else if (err) {
          return next(err);
        } else {
          return next(null, response);
        }
      });
    },

    (response, next) => {
      if (response) {
        return next(null, response);
      }

      authenticate((err) => {
        if (err) {
          return next(err);
        }

        next(null, null);
      });
    },

    (response, next) => {

      if (response) {
        return next(null, response);
      }

      targetClient[operation](requestBody, callMetadata, next);
    }
  ], callback);
}

function getChildren(parentKey, callback) {

  let end = Buffer.from(Buffer.from(parentKey));

  //credit: taken from https://github.com/mixer/etcd3/blob/master/src/util.ts
  for (let i = end.length - 1; i >= 0; i--) {
    if (end[i] < 0xff) {
      end[i]++;
      end = end.slice(0, i + 1);
      break;
    }
  }

  const getRequest = {
    key: Buffer.from(parentKey),
    range_end: end
  };

  executeRequest(KVClient, 'range', getRequest,  (err, response) => {
    if (err) {
      return callback(err);
    }

    const children = [];
    let i = 0;
    while (i < response.count) {
      children.push(
        {
          key : response.kvs[i].key.toString(),
          value : response.kvs[i].value ? response.kvs[i].value.toString() : ''
        }
      );
      i++;
    }
    return callback(null, children);
  });
}

function createKey (key, value, callback) {

  const putRequest = {
    key :  Buffer.from(key),
    value : Buffer.from(value)
  };

  executeRequest(KVClient, 'put', putRequest, callback);
}

function deleteKey (key, callback) {
  const deleteRequest = {
    key: new Buffer(key)
  };

  executeRequest(KVClient, 'deleteRange', deleteRequest, callback);
}


function performEtcdOperation(operation, params, callback) {
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
  init: init,
  performEtcdOperation: performEtcdOperation,
  SUPPORTED_OPERATIONS: SUPPORTED_OPERATIONS
};
