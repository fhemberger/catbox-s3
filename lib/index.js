'use strict';

// Load modules
const Hoek = require('hoek');
const AWS  = require('aws-sdk');


// Declare internals
const internals = {};

internals.getStoragePathForKey = function (key) {

    /* eslint-disable hapi/hapi-scope-start */
    const convert = (str) => str
            // Remove leading/trailing slashes
            .replace(/(^\/|\/$)/g, '')
            // Replace special URL characters
            .replace(/[?&#%]/g, '~');
    /* eslint-enable hapi/hapi-scope-start */

    if (key.id) {
        key.id = convert(key.id);
    }

    if (key.segment) {
        key.segment = convert(key.segment);
    }

    return key.id === '' ?
        key.segment :
        key.segment + '/' + key.id;
};


internals.parseBody = function (contentType, body) {

    if (contentType === 'text/plain' && Buffer.isBuffer(body)) {
        body = body.toString();
    }

    if (contentType === 'application/json') {
        /* eslint-disable brace-style */
        try {
            body = JSON.parse(body);
        } catch (e) {}
        /* eslint-enable brace-style */
    }

    return body;
};


internals.testBucketAccess = function (client, settings, callback) {

    const putParams = {
        Bucket : settings.bucket,
        Key    : internals.getStoragePathForKey({ segment: 'catbox-s3', id: 'accesstest' }),
        Body   : 'ok'
    };

    if (settings.setACL !== false) {
        putParams.ACL = settings.ACL ? settings.ACL : 'public-read';
    }

    const getParams = {
        Bucket : settings.bucket,
        Key    : internals.getStoragePathForKey({ segment: 'catbox-s3', id: 'accesstest' })
    };

    client.putObject(putParams, (err) => {

        Hoek.assert(!err, `Error writing to bucket ${settings.bucket} ${err}`);

        client.getObject(getParams, (err, data) => {

            Hoek.assert(!err && data.Body.toString('utf8') === 'ok', `Error reading from bucket ${settings.bucket} ${err}`);
            callback();
        });
    });
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

    const clientOptions = {
        accessKeyId     : this.settings.accessKeyId,
        secretAccessKey : this.settings.secretAccessKey
    };

    if (this.settings.region) {
        clientOptions.region = this.settings.region;
    }

    if (this.settings.endpoint) {
        clientOptions.endpoint = this.settings.endpoint;
    }

    if (this.settings.signatureVersion) {
        clientOptions.signatureVersion = this.settings.signatureVersion;
    }

    this.client = new AWS.S3(clientOptions);

    internals.testBucketAccess(this.client, this.settings, (err, data) => {

        if (err) {
            self.isConnected = false;
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

    const params = {
        Bucket : this.settings.bucket,
        Key    : internals.getStoragePathForKey(key)
    };

    this.client.getObject(params, (err, data) => {

        if (err) {
            return callback(null, null);
        }

        const now    = new Date().getTime();
        const stored = new Date(data.Metadata['catbox-stored']);
        let ttl      = Number(data.Metadata['catbox-ttl']) || 0;

        // Calculate remaining ttl
        ttl = (stored.getTime() + ttl) - now;

        // Cache item has expired
        if (ttl <= 0) {
            return callback(null, null);
        }

        const result = {
            item   : internals.parseBody(data.ContentType, data.Body),
            stored : stored,
            ttl    : ttl
        };

        callback(null, result);

    });
};


internals.Connection.prototype.set = function (key, value, ttl, callback) {

    if (!this.isConnected) {
        return callback(new Error('Connection not started'));
    }

    let type = 'application/octet-stream';

    if (['String', 'Number', 'Boolean'].indexOf(value.constructor.name) > -1) {
        type = 'text/plain';
    }

    if (['Object', 'Array'].indexOf(value.constructor.name) > -1) {
        /* eslint-disable brace-style */
        try {
            value = JSON.stringify(value);
            type = 'application/json';
        } catch (e) {
            return callback(new Error('Could not convert object to JSON'));
        }
        /* eslint-enable brace-style */
    }

    const now = new Date();
    const params = {
        Bucket      : this.settings.bucket,
        Key         : internals.getStoragePathForKey(key),
        Expires     : new Date(now.getTime() + ttl),
        ContentType : type,
        Body        : value
    };

    if (this.settings.setACL !== false) {
        params.ACL = this.settings.ACL ? this.settings.ACL : 'public-read';
    }

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
        Key    : internals.getStoragePathForKey(key)
    };

    this.client.deleteObject(params, (err) => {

        if (err) {
            return callback(err);
        }
        callback();
    });
};
