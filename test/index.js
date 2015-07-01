'use strict';

// Load modules
var Lab = require('lab');
var Code = require('code');
var Catbox = require('catbox');
var S3 = require('..');


var options = {
    accessKeyId     : process.env.S3_ACCESS_KEY,
    secretAccessKey : process.env.S3_SECRET_KEY,
    bucket          : process.env.S3_BUCKET
};

if (process.env.S3_REGION) {
    options.region = process.env.S3_REGION
}


// Test shortcuts
var lab = exports.lab = Lab.script();
var describe = lab.describe;
var it = lab.it;
var before = lab.before;
var after = lab.after;
var expect = Code.expect;


describe('S3', function () {

    it('throws an error if not created with new', function (done) {
        var fn = function () {
            var s3 = S3();
        };

        expect(fn).to.throw(Error);
        done();
    });

    it('creates a new connection', function (done) {
        var client = new Catbox.Client(S3, options);
        client.start(function (err) {
            expect(client.isReady()).to.equal(true);
            done();
        });
    });

    it('closes the connection', function (done) {
        var client = new Catbox.Client(S3, options);
        client.start(function (err) {
            expect(client.isReady()).to.equal(true);
            client.stop();
            expect(client.isReady()).to.equal(false);
            done();
        });
    });

    it('gets an item after setting it', function (done) {
        var client = new Catbox.Client(S3, options);
        client.start(function (err) {
            var key = { id: 'x', segment: 'test' };
            client.set(key, '123', 5000, function (err) {
                expect(err).to.not.exist;
                client.get(key, function (err, result) {
                    expect(err).to.equal(null);
                    expect(result.item).to.equal('123');
                    done();
                });
            });
        });
    });

    it.skip('buffers can be set and retrieved', function (done) {
        var buffer = new Buffer('string value');
        var client = new Catbox.Client(new S3(options));

        client.start(function (err) {
            var key = { id: 'x', segment: 'test' };

            client.set(key, buffer, 1000, function (err) {

                expect(err).to.not.exist;
                client.get(key, function (err, result) {
                    expect(err).to.not.exist;
                    expect(result.item instanceof Buffer).to.equal(true);
                    expect(result.item).to.deep.equal(buffer);
                    done();
                });
            });
        });
    });

    it.skip('buffers are copied before storing', function (done) {
        var buffer = new Buffer('string value');
        var client = new Catbox.Client(new S3(options));

        client.start(function (err) {
            var key = { id: 'x', segment: 'test' };
            client.set(key, buffer, 1000, function (err) {
                expect(err).to.not.exist;
                client.get(key, function (err, result) {
                    expect(err).to.not.exist;
                    expect(result.item).to.not.equal(buffer);
                    done();
                });
            });
        });
    });

    it('fails setting an item circular references', function (done) {
        var client = new Catbox.Client(S3, options);
        client.start(function (err) {
            var key = { id: 'x', segment: 'test' };
            var value = { a: 1 };
            value.b = value;
            client.set(key, value, 10, function (err) {
                expect(err.message).to.equal('Could not convert object to JSON');
                done();
            });
        });
    });

    it('ignored starting a connection twice on same event', function (done) {
        var client = new Catbox.Client(S3, options);
        var x = 2;
        var start = function () {
            client.start(function (err) {
                expect(client.isReady()).to.equal(true);
                --x;
                if (!x) {
                    done();
                }
            });
        };

        start();
        start();
    });

    it('ignored starting a connection twice chained', function (done) {
        var client = new Catbox.Client(S3, options);
        client.start(function (err) {
            expect(err).to.not.exist;
            expect(client.isReady()).to.equal(true);
            client.start(function (err) {
                expect(err).to.not.exist;
                expect(client.isReady()).to.equal(true);
                done();
            });
        });
    });

    it('returns not found on get when using null key', function (done) {
        var client = new Catbox.Client(S3, options);
        client.start(function (err) {

            client.get(null, function (err, result) {

                expect(err).to.equal(null);
                expect(result).to.equal(null);
                done();
            });
        });
    });

    it('returns not found on get when item expired', function (done) {
        var client = new Catbox.Client(S3, options);
        client.start(function (err) {
            var key = { id: 'x', segment: 'test' };
            client.set(key, 'x', 1, function (err) {
                expect(err).to.not.exist;
                setTimeout(function () {
                    client.get(key, function (err, result) {
                        expect(err).to.equal(null);
                        expect(result).to.equal(null);
                        done();
                    });
                }, 1000);
            });
        });
    });

    it('errors on set when using null key', function (done) {
        var client = new Catbox.Client(S3, options);
        client.start(function (err) {
            client.set(null, {}, 1000, function (err) {
                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });

    it('errors on get when using invalid key', function (done) {
        var client = new Catbox.Client(S3, options);
        client.start(function (err) {
            client.get({}, function (err) {
                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });

    it('errors on set when using invalid key', function (done) {
        var client = new Catbox.Client(S3, options);
        client.start(function (err) {
            client.set({}, {}, 1000, function (err) {
                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });

    it('ignores set when using non-positive ttl value', function (done) {
        var client = new Catbox.Client(S3, options);
        client.start(function (err) {
            var key = { id: 'x', segment: 'test' };
            client.set(key, 'y', 0, function (err) {
                expect(err).to.not.exist;
                done();
            });
        });
    });

    it('errors on get when stopped', function (done) {
        var client = new Catbox.Client(S3, options);
        client.stop();
        var key = { id: 'x', segment: 'test' };
        client.connection.get(key, function (err, result) {
            expect(err).to.exist;
            expect(result).to.not.exist;
            done();
        });
    });

    it('errors on set when stopped', function (done) {
        var client = new Catbox.Client(S3, options);
        client.stop();
        var key = { id: 'x', segment: 'test' };
        client.connection.set(key, 'y', 1, function (err) {
            expect(err).to.exist;
            done();
        });
    });

    it('errors on missing segment name', function (done) {
        var config = {
            expiresIn: 50000
        };
        var fn = function () {
            var client = new Catbox.Client(S3, options);
            var cache = new Catbox.Policy(config, client, '');
        };
        expect(fn).to.throw(Error);
        done();
    });

    it('errors on bad segment name', function (done) {
        var config = {
            expiresIn: 50000
        };
        var fn = function () {
            var client = new Catbox.Client(S3, options);
            var cache = new Catbox.Policy(config, client, 'a\0b');
        };
        expect(fn).to.throw(Error);
        done();
    });

    it('supports empty keys', function (done) {
        var client = new Catbox.Client(S3, options);
        client.start(function (err) {
            expect(err).to.not.exist();

            var key = { id: '', segment: 'test' };
            client.set(key, '123', 5000, function (err) {
                expect(err).to.not.exist();
                client.get(key, function (err, result) {
                    expect(err).to.not.exist();
                    expect(result.item).to.equal('123');
                    done();
                });
            });
        });
    });

    describe('#start', function () {

        it('creates an empty client object', function (done) {
            var s3 = new S3(options);
            expect(s3.client).to.not.exist;
            s3.start(function () {
                expect(s3.client).to.exist;
                done();
            });
        });

    });

    describe('#stop', function () {

        it('sets the cache client to null', function (done) {
            var s3 = new S3(options);
            expect(s3.client).to.not.exist;
            s3.start(function () {
                expect(s3.client).to.exist;
                s3.stop();
                expect(s3.client).to.not.exist;
                done();
            });
        });

    });

    describe('#get', function () {

        it('returns not found on missing segment', function (done) {
            var key = {
                segment : 'unknownsegment',
                id      : 'test'
            };
            var s3 = new S3(options);
            expect(s3.client).to.not.exist;
            s3.start(function () {
                expect(s3.client).to.exist;
                s3.get(key, function (err, result) {
                    expect(err).to.not.exist;
                    expect(result).to.not.exist;
                    done();
                });
            });
        });
    });

    describe('#set', function () {

        it('adds an item to the cache object', function (done) {
            var key = {
                segment : 'test',
                id      : 'test'
            };

            var s3 = new S3(options);
            expect(s3.client).to.not.exist;

            s3.start(function () {
                expect(s3.client).to.exist;
                s3.set(key, 'myvalue', 2000, function () {
                    s3.get(key, function (err, result) {
                        expect(result.item).to.equal('myvalue');
                        done();
                    });
                });
            });
        });

        it('removes an item from the cache object when it expires', function (done) {
            var key = {
                segment: 'test',
                id: 'test'
            };

            var s3 = new S3(options);
            expect(s3.client).to.not.exist;
            s3.start(function () {
                expect(s3.client).to.exist;
                s3.set(key, 'myvalue', 2000, function () {
                    s3.get(key, function (err, result) {
                        expect(result.item).to.equal('myvalue');
                        setTimeout(function () {
                            s3.get(key, function (err, result) {
                                expect(result).to.not.exist;
                                done();
                            });
                        }, 1500);
                    });
                });
            });
        });

    });

    describe('#drop', function () {

        it('drops an existing item', function (done) {
            var client = new Catbox.Client(S3, options);
            client.start(function (err) {
                var key = { id: 'x', segment: 'test' };
                client.set(key, '123', 5000, function (err) {
                    expect(err).to.not.exist;
                    client.get(key, function (err, result) {
                        expect(err).to.equal(null);
                        expect(result.item).to.equal('123');
                        client.drop(key, function (err) {
                            expect(err).to.not.exist;
                            done();
                        });
                    });
                });
            });
        });

        it('drops an item from a missing segment', function (done) {
            var client = new Catbox.Client(S3, options);
            client.start(function (err) {
                var key = { id: 'x', segment: 'test' };
                client.drop(key, function (err) {
                    expect(err).to.not.exist;
                    done();
                });
            });
        });

        it('drops a missing item', function (done) {
            var client = new Catbox.Client(S3, options);
            client.start(function (err) {
                var key = { id: 'x', segment: 'test' };
                client.set(key, '123', 1000, function (err) {
                    expect(err).to.not.exist;
                    client.get(key, function (err, result) {
                        expect(err).to.equal(null);
                        expect(result.item).to.equal('123');
                        client.drop({ id: 'y', segment: 'test' }, function (err) {
                            expect(err).to.not.exist;
                            done();
                        });
                    });
                });
            });
        });

        it('errors on drop when using invalid key', function (done) {
            var client = new Catbox.Client(S3, options);
            client.start(function (err) {
                client.drop({}, function (err) {
                    expect(err instanceof Error).to.equal(true);
                    done();
                });
            });
        });

        it('errors on drop when using null key', function (done) {
            var client = new Catbox.Client(S3, options);
            client.start(function (err) {
                client.drop(null, function (err) {
                    expect(err instanceof Error).to.equal(true);
                    done();
                });
            });
        });

        it('errors on drop when stopped', function (done) {
            var client = new Catbox.Client(S3, options);
            client.stop();
            var key = { id: 'x', segment: 'test' };
            client.connection.drop(key, function (err) {
                expect(err).to.exist;
                done();
            });
        });

        it('errors when cache item dropped while stopped', function (done) {
            var client = new Catbox.Client(S3, options);
            client.stop();
            client.drop('a', function (err) {
                expect(err).to.exist;
                done();
            });
        });
    });

    describe('#validateSegmentName', function () {

        it('errors when the name is empty', function (done) {
            var s3 = new S3(options);
            var result = s3.validateSegmentName('');

            expect(result).to.be.instanceOf(Error);
            expect(result.message).to.equal('Empty string');
            done();
        });

        it('errors when the name has a null character', function (done) {
            var s3 = new S3(options);
            var result = s3.validateSegmentName('\0test');

            expect(result).to.be.instanceOf(Error);
            done();
        });

        it('errors when the name has less than three characters', function (done) {
            var s3 = new S3(options);
            var result = s3.validateSegmentName('yo');

            expect(result).to.be.instanceOf(Error);
            done();
        });

        it('errors when the name has more than 64 characters', function (done) {
            var s3 = new S3(options);
            var result = s3.validateSegmentName('abcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890');

            expect(result).to.be.instanceOf(Error);
            done();
        });

        it('returns null when there are no errors', function (done) {
            var s3 = new S3(options);
            var result = s3.validateSegmentName('valid');

            expect(result).to.not.be.instanceOf(Error);
            expect(result).to.equal(null);
            done();
        });
    });
});
