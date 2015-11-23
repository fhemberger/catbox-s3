'use strict';

// Load modules
const request = require('request');
const Hoek = require('hoek');
const AWS  = require('aws-sdk');


// Declare internals
const internals = {};

const getStoragePathForKey = function (key) {

    // Remove leading/trailing slashes
    key.id      = key.id.replace(/(^\/|\/$)/g, '');
    key.segment = key.segment.replace(/(^\/|\/$)/g, '');

    return key.id === '' ?
        key.segment :
        key.segment + '/' + key.id;
};


const parseBody = function (res, body) {

    if (res.headers['content-type'] === 'text/plain' && Buffer.isBuffer(body)) {
        body = body.toString();
    }

    if (res.headers['content-type'] === 'application/json') {
        /* eslint-disable brace-style */
        try {
            body = JSON.parse(body);
        } catch (e) {}
        /* eslint-enable brace-style */
    }

    return body;
};


exports = module.exports = internals.Connection = function S3Cache (options) {

    Hoek.assert(this.constructor === internals.Connection, 'S3 cache client must be instantiated using new');
    Hoek.assert(options && options.bucket, 'Invalid Amazon S3 bucket value');
    Hoek.assert(options && options.accessKeyId, 'Invalid Amazon S3 accessKeyId value');
    Hoek.assert(options && options.secretAccessKey, 'Invalid Amazon S3 secretAccessKey value');

    this.settings = Hoek.clone(options || {});
    this.client = null;
};


internals.Connection.prototype.start = function (callback) {

    const self = this;
    const params = {
        Bucket: this.settings.bucket
    };

    const clientOptions = {
        accessKeyId     : this.settings.accessKeyId,
        secretAccessKey : this.settings.secretAccessKey
    };

    if (this.settings.region) {
        clientOptions.region = this.settings.region;
    }

    this.client = new AWS.S3(clientOptions);

    // Check if we can access the bucket
    // FIXME: This only checks if the given bucket exists at all,
    // not if the current user has access rights to it.
    this.client.headBucket(params, (err) => {

        if (err) {
            return callback(err);
        }
        self.isConnected = true;
        callback();
    });
};


internals.Connection.prototype.stop = function () {

    if (this.client) {
        this.client = null;
        this.isConnected = false;
    }
};


internals.Connection.prototype.isReady = function () {

    return this.isConnected;
};


internals.Connection.prototype.validateSegmentName = function (name) {

    if (!name) {
        return new Error('Empty string');
    }

    if (name.indexOf('\0') !== -1) {
        return new Error('Includes null character');
    }

    if (name.length < 3 || name.length > 63) {
        return new Error('Must be between 3 and 63 characters');
    }

    return null;
};


internals.Connection.prototype.get = function (key, callback) {

    if (!this.isConnected) {
        return callback(new Error('Connection not started'));
    }

    const baseUrl = this.settings.region ?
        'http://s3-' + this.settings.region + '.amazonaws.com/' :
        'http://s3.amazonaws.com/';

    const requestOptions = {
        url      : baseUrl + this.settings.bucket + '/' + getStoragePathForKey(key),
        encoding : null
    };

    request.get(requestOptions, (err, res, body) => {

        if (err) {
            return callback(err);
        }

        if (res.statusCode !== 200) {
            return callback(null, null);
        }

        const now    = new Date().getTime();
        const stored = new Date(res.headers['x-amz-meta-catbox-stored']);
        let ttl      = Number(res.headers['x-amz-meta-catbox-ttl']) || 0;

        // Calculate remaining ttl
        ttl = (stored.getTime() + ttl) - now;

        // Cache item has expired
        if (ttl <= 0) {
            return callback(null, null);
        }

        const result = {
            item   : parseBody(res, body),
            stored : stored,
            ttl    : ttl
        };

        callback(null, result);
    });
};


internals.Connection.prototype.set = function (key, value, ttl, callback) {

    let type = 'application/octet-stream';

    if (!this.isConnected) {
        return callback(new Error('Connection not started'));
    }

    if (['Object', 'Array'].indexOf(value.constructor.name) === 0) {
        /* eslint-disable brace-style */
        try {
            value = JSON.stringify(value);
            type = 'application/json';
        } catch (e) {
            return callback(new Error('Could not convert object to JSON'));
        }
        /* eslint-enable brace-style */
    }

    if (['String', 'Number', 'Boolean'].indexOf(value.constructor.name) === 0) {
        type = 'text/plain';
    }

    const now = new Date();
    const params = {
        Bucket      : this.settings.bucket,
        Key         : getStoragePathForKey(key),
        ACL         : 'public-read',
        Expires     : new Date(now.getTime() + ttl),
        ContentType : type,
        Body        : value
    };

    const req = this.client.putObject(params);
    req.on('build', () => {

        req.httpRequest.headers['x-amz-meta-catbox-stored'] = now;
        req.httpRequest.headers['x-amz-meta-catbox-ttl']    = ttl;
    });
    req.send((err) => {

        if (err) {
            return callback(err);
        }
        callback();
    });
};


internals.Connection.prototype.drop = function (key, callback) {

    if (!this.isConnected) {
        return callback(new Error('Connection not started'));
    }

    const params = {
        Bucket : this.settings.bucket,
        Key    : getStoragePathForKey(key)
    };

    this.client.deleteObject(params, (err) => {

        if (err) {
            return callback(err);
        }
        callback();
    });
};
