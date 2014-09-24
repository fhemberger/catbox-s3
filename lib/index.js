'use strict';

// Load modules
var request = require('request');
var Hoek = require('hoek');
var AWS  = require('aws-sdk');


// Declare internals
var internals = {};


internals.defaults = {
    region: 'eu-west-1'
};


exports = module.exports = internals.Connection = function S3Cache (options) {
    Hoek.assert(this.constructor === internals.Connection, 'S3 cache client must be instantiated using new');
    Hoek.assert(options && options.bucket, 'Invalid Amazon S3 bucket value');
    Hoek.assert(options && options.accessKeyId, 'Invalid Amazon S3 accessKeyId value');
    Hoek.assert(options && options.secretAccessKey, 'Invalid Amazon S3 secretAccessKey value');

    this.settings = Hoek.applyToDefaults(internals.defaults, options || {});
    this.client = null;
};


internals.Connection.prototype.start = function (callback) {
    var self = this;
    var params = {
        Bucket: this.settings.bucket
    };

    this.client = new AWS.S3({
        accessKeyId     : this.settings.accessKeyId,
        secretAccessKey : this.settings.secretAccessKey,
        region          : this.settings.region
    });

    // Check if we can access the bucket
    // FIXME: This only checks if the given bucket exists at all,
    // not if the current user has access rights to it.
    this.client.headBucket(params, function(err, data) {
        if (err) { return callback(err); }
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

    var url     = 'http://s3-' + this.settings.region + '.amazonaws.com/' + this.settings.bucket + '/' + key.segment + '/' + key.id;
    var options = { url: url, encoding: null };
    request.get(options, function(err, res, body) {
        if (err) { return callback(err); }

        if (res.statusCode !== 200) {
            return callback(null, null);
        }

        var now    = new Date().getTime();
        var stored = new Date(res.headers['x-amz-meta-catbox-stored']);
        var ttl    = Number(res.headers['x-amz-meta-catbox-ttl']) || 0;

        // Calculate remaining ttl
        ttl = (stored.getTime() + ttl) - now;

        // Cache item has expired
        if (ttl <= 0) {
            return callback(null, null);
        }

        // Let's see if the string is JSON in disguise …
        // The RegExp check is horrible, but we need to prevent accidental
        // type casting: JSON.parse('123') => 123
        if (body.constructor.name === 'String' && /[[]{}:]/.test(body)) {
            try {
                body = JSON.parse(body);
            } catch(e) {}
        }

        var result = {
            item   : body,
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

    if (value.constructor.name === 'Object') {
        try {
            value = JSON.stringify(value);
        } catch(e) {
            return callback(new Error('Could not convert object to JSON'));
        }
    }

    var now = new Date();
    var params = {
        Bucket  : this.settings.bucket,
        Key     : key.segment + '/' + key.id,
        ACL     : 'public-read',
        Expires : new Date(now.getTime() + ttl),
        Body    : value
    };
    var req = this.client.putObject(params);
    req.on('build', function() {
        req.httpRequest.headers['x-amz-meta-catbox-stored'] = now;
        req.httpRequest.headers['x-amz-meta-catbox-ttl']    = ttl;
    });
    req.send(function(err, data) {
        if (err) { return callback(err); }
        callback();
    });
};


internals.Connection.prototype.drop = function (key, callback) {
    if (!this.isConnected) {
        return callback(new Error('Connection not started'));
    }

    var params = {
        Bucket : this.settings.bucket,
        Key    : key.segment + '/' + key.id
    };
    this.client.deleteObject(params, function(err, data) {
        if (err) { return callback(err); }
        callback();
    });
};
