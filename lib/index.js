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
        // needs an s3 mock
        try {
            body = JSON.parse(body);
        } catch (e) {}
        /* eslint-enable brace-style */
    }

    return body;
};


internals.testBucketAccess = function (client, settings) {

    const putParams = {
        Bucket : settings.bucket,
        Key    : internals.getStoragePathForKey({ segment: 'catbox-s3', id: 'accesstest' }),
        Body   : 'ok'
    };

    // needs s3 mock; public-read needs to be nabled
    if (settings.setACL !== false) {
        putParams.ACL = settings.ACL ? settings.ACL : 'public-read';
    }

    const getParams = {
        Bucket : settings.bucket,
        Key    : internals.getStoragePathForKey({ segment: 'catbox-s3', id: 'accesstest' })
    };

    return new Promise((resolve, reject) => {

        client.putObject(putParams, (err) => {

            if (err) {
                return reject(new Error(`Error writing to bucket ${settings.bucket} ${err}`));
            }

            client.getObject(getParams, (err, data) => {

                if (err || !data.Body.toString('utf8') === 'ok') {
                    return reject(new Error(`Error reading from bucket ${settings.bucket} ${err}`));
                }
                resolve();
            });
        });
    });
};


exports = module.exports = internals.Connection = function S3Cache (options) {

    Hoek.assert(this.constructor === internals.Connection, 'S3 cache client must be instantiated using new');
    Hoek.assert(options && options.bucket, 'Invalid Amazon S3 bucket value');

    this.settings = Hoek.clone(options || {});
    this.client = null;
};


internals.Connection.prototype.start = function () {

    const self = this;

    const clientOptions = {};

    if (this.settings.secretAccessKey && this.settings.accessKeyId) {
        clientOptions.accessKeyId = this.settings.accessKeyId;
        clientOptions.secretAccessKey = this.settings.secretAccessKey;
    }

    if (this.settings.region) {
        clientOptions.region = this.settings.region;
    }

    if (this.settings.endpoint) {
        clientOptions.endpoint = this.settings.endpoint;
    }

    if (this.settings.signatureVersion) {
        clientOptions.signatureVersion = this.settings.signatureVersion;
    }

    if (this.settings.s3ForcePathStyle) {
        clientOptions.s3ForcePathStyle = this.settings.s3ForcePathStyle;
    }

    this.client = new AWS.S3(clientOptions);

    return internals.testBucketAccess(this.client, this.settings)
        .then((data) => {

            self.isConnected = true;
        })
        .catch((err) => {

            self.isConnected = false;

            throw err;
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


internals.Connection.prototype.get = function (key) {

    return new Promise((resolve, reject) => {

        if (!this.isConnected) {
            return reject(new Error('Connection not started'));
        }

        const params = {
            Bucket : this.settings.bucket,
            Key    : internals.getStoragePathForKey(key)
        };

        this.client.getObject(params, (err, data) => {

            if (err) {
                return resolve(null);
            }

            const now    = new Date().getTime();
            const stored = new Date(data.Metadata['catbox-stored']);
            let ttl      = Number(data.Metadata['catbox-ttl']) || 0;

            // Calculate remaining ttl
            ttl = (stored.getTime() + ttl) - now;

            // Cache item has expired
            if (ttl <= 0) {
                return resolve(null);
            }

            const result = {
                item: internals.parseBody(data.ContentType, data.Body),
                stored,
                ttl
            };

            resolve(result);
        });
    });
};


internals.Connection.prototype.set = function (key, value, ttl) {

    return new Promise((resolve, reject) => {

        if (!this.isConnected) {
            return reject(new Error('Connection not started'));
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
                return reject(new Error('Could not convert object to JSON'));
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
            // TODO
            params.ACL = this.settings.ACL ? this.settings.ACL : 'public-read';
        }

        const req = this.client.putObject(params);
        req.on('build', () => {

            req.httpRequest.headers['x-amz-meta-catbox-stored'] = now;
            req.httpRequest.headers['x-amz-meta-catbox-ttl']    = ttl;
        });
        req.send((err) => {

            if (err) {
                return reject(err);
            }
            resolve();
        });
    });
};


internals.Connection.prototype.drop = function (key) {

    // this would require mockery and a full mock to test
    return new Promise((resolve, reject) => {

        if (!this.isConnected) {
            return reject(new Error('Connection not started'));
        }

        const params = {
            Bucket : this.settings.bucket,
            Key    : internals.getStoragePathForKey(key)
        };

        this.client.deleteObject(params, (err) => {

            if (err) {
                return reject(err);
            }
            resolve();
        });
    });
};
