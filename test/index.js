'use strict';

// Load modules
const Lab = require('lab');
const Code = require('code');
const Catbox = require('catbox');
const S3 = require('..');


const options = {
    accessKeyId     : process.env.S3_ACCESS_KEY,
    secretAccessKey : process.env.S3_SECRET_KEY,
    bucket          : process.env.S3_BUCKET,
    setACL          : process.env.S3_SET_ACL && process.env.S3_SET_ACL === 'false' ? false : true
};

if (process.env.S3_REGION) {
    options.region = process.env.S3_REGION;
}

if (process.env.S3_ENDPOINT) {
    options.endpoint = process.env.S3_ENDPOINT;
}

if (process.env.S3_SIGNATURE_VERSION) {
    options.signatureVersion = process.env.S3_SIGNATURE_VERSION;
}


// Test shortcuts
const lab = exports.lab = Lab.script();
const describe = lab.describe;
const it = lab.it;
const expect = Code.expect;


describe('S3', () => {

    it('throws an error if not created with new', (done) => {

        const fn = () => {

            S3();
        };

        expect(fn).to.throw(Error);
        done();
    });


    it('creates a new connection', (done) => {

        const client = new Catbox.Client(S3, options);
        client.start((err) => {

            expect(err).to.not.exist();
            expect(client.isReady()).to.equal(true);
            done();
        });
    });

    it('closes the connection', (done) => {

        const client = new Catbox.Client(S3, options);
        client.start((err) => {

            expect(err).to.not.exist();
            expect(client.isReady()).to.equal(true);
            client.stop();
            expect(client.isReady()).to.equal(false);
            done();
        });
    });


    it('gets an item after setting it', (done) => {

        const client = new Catbox.Client(S3, options);
        client.start((err) => {

            expect(err).to.not.exist();
            const key = { id: 'test/id?with special%chars&', segment: 'test' };
            client.set(key, '123', 5000, (err) => {

                expect(err).to.not.exist();
                client.get(key, (err, result) => {

                    expect(err).to.equal(null);
                    expect(result.item).to.equal('123');
                    done();
                });
            });
        });
    });


    it('buffers can be set and retrieved', (done) => {

        const buffer = new Buffer('string value');
        const client = new Catbox.Client(new S3(options));

        client.start((err) => {

            expect(err).to.not.exist();
            const key = { id: 'buffer', segment: 'test' };

            client.set(key, buffer, 2000, (err) => {

                expect(err).to.not.exist();
                client.get(key, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result.item instanceof Buffer).to.equal(true);
                    expect(result.item).to.equal(buffer);
                    done();
                });
            });
        });
    });


    it('buffers are copied before storing', (done) => {

        const buffer = new Buffer('string value');
        const client = new Catbox.Client(new S3(options));

        client.start((err) => {

            expect(err).to.not.exist();
            const key = { id: 'buffer-copied', segment: 'test' };
            client.set(key, buffer, 2000, (err) => {

                expect(err).to.not.exist();
                client.get(key, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result.item).to.not.shallow.equal(buffer);
                    done();
                });
            });
        });
    });


    it('fails setting an item circular references', (done) => {

        const client = new Catbox.Client(S3, options);
        client.start((err) => {

            expect(err).to.not.exist();
            const key = { id: 'circular', segment: 'test' };
            const value = { a: 1 };
            value.b = value;

            client.set(key, value, 10, (err) => {

                expect(err.message).to.equal('Could not convert object to JSON');
                done();
            });
        });
    });


    it('ignored starting a connection twice on same event', (done) => {

        let x = 2;
        const client = new Catbox.Client(S3, options);
        const start = () => {

            client.start((err) => {

                expect(err).to.not.exist();
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


    it('ignored starting a connection twice chained', (done) => {

        const client = new Catbox.Client(S3, options);
        client.start((err) => {

            expect(err).to.not.exist();
            expect(client.isReady()).to.equal(true);
            client.start((err) => {

                expect(err).to.not.exist();
                expect(client.isReady()).to.equal(true);
                done();
            });
        });
    });


    it('returns not found on get when using null key', (done) => {

        const client = new Catbox.Client(S3, options);
        client.start((err) => {

            expect(err).to.not.exist();
            client.get(null, (err, result) => {

                expect(err).to.equal(null);
                expect(result).to.equal(null);
                done();
            });
        });
    });


    it('returns not found on get when item expired', (done) => {

        const client = new Catbox.Client(S3, options);
        client.start((err) => {

            expect(err).to.not.exist();
            const key = { id: 'x', segment: 'test' };
            client.set(key, 'x', 1, (err) => {

                expect(err).to.not.exist();
                setTimeout(() => {

                    client.get(key, (err, result) => {

                        expect(err).to.equal(null);
                        expect(result).to.equal(null);
                        done();
                    });
                }, 1000);
            });
        });
    });


    it('errors on set when using null key', (done) => {

        const client = new Catbox.Client(S3, options);
        client.start((err) => {

            expect(err).to.not.exist();
            client.set(null, {}, 1000, (err) => {

                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });


    it('errors on get when using invalid key', (done) => {

        const client = new Catbox.Client(S3, options);
        client.start((err) => {

            expect(err).to.not.exist();
            client.get({}, (err) => {

                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });


    it('errors on set when using invalid key', (done) => {

        const client = new Catbox.Client(S3, options);
        client.start((err) => {

            expect(err).to.not.exist();
            client.set({}, {}, 1000, (err) => {

                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });


    it('ignores set when using non-positive ttl value', (done) => {

        const client = new Catbox.Client(S3, options);
        client.start((err) => {

            expect(err).to.not.exist();
            const key = { id: 'x', segment: 'test' };
            client.set(key, 'y', 0, (err) => {

                expect(err).to.not.exist();
                done();
            });
        });
    });


    it('errors on get when stopped', (done) => {

        const client = new Catbox.Client(S3, options);
        client.stop();
        const key = { id: 'x', segment: 'test' };
        client.connection.get(key, (err, result) => {

            expect(err).to.exist;
            expect(result).to.not.exist;
            done();
        });
    });


    it('errors on set when stopped', (done) => {

        const client = new Catbox.Client(S3, options);
        client.stop();
        const key = { id: 'x', segment: 'test' };
        client.connection.set(key, 'y', 1, (err) => {

            expect(err).to.exist;
            done();
        });
    });


    it('errors on missing segment name', (done) => {

        const config = {
            expiresIn: 50000
        };

        const fn = () => {

            const client = new Catbox.Client(S3, options);
            new Catbox.Policy(config, client, '');
        };
        expect(fn).to.throw(Error);
        done();
    });


    it('errors on bad segment name', (done) => {

        const config = {
            expiresIn: 50000
        };
        const fn = () => {

            const client = new Catbox.Client(S3, options);
            new Catbox.Policy(config, client, 'a\0b');
        };
        expect(fn).to.throw(Error);
        done();
    });


    it('supports empty keys', (done) => {

        const client = new Catbox.Client(S3, options);
        client.start((err) => {

            expect(err).to.not.exist();

            const key = { id: '', segment: 'test' };
            client.set(key, '123', 5000, (err) => {

                expect(err).to.not.exist();
                client.get(key, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result.item).to.equal('123');
                    done();
                });
            });
        });
    });


    describe('#start', () => {

        it('creates an empty client object', (done) => {

            const s3 = new S3(options);
            expect(s3.client).to.not.exist;
            s3.start(() => {

                expect(s3.client).to.exist;
                done();
            });
        });

    });

    describe('#stop', () => {

        it('sets the cache client to null', (done) => {

            const s3 = new S3(options);
            expect(s3.client).to.not.exist;
            s3.start(() => {

                expect(s3.client).to.exist;
                s3.stop();
                expect(s3.client).to.not.exist;
                done();
            });
        });

    });

    describe('#get', () => {

        it('returns not found on missing segment', (done) => {

            const key = {
                segment : 'unknownsegment',
                id      : 'test'
            };
            const s3 = new S3(options);
            expect(s3.client).to.not.exist;
            s3.start(() => {

                expect(s3.client).to.exist;
                s3.get(key, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.not.exist;
                    done();
                });
            });
        });
    });


    describe('#set', () => {

        it('adds an item to the cache object', (done) => {

            const key = {
                segment : 'test',
                id      : 'test'
            };

            const s3 = new S3(options);
            expect(s3.client).to.not.exist;

            s3.start(() => {

                expect(s3.client).to.exist;
                s3.set(key, 'myvalue', 2000, () => {

                    s3.get(key, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result.item).to.equal('myvalue');
                        done();
                    });
                });
            });
        });

        it('removes an item from the cache object when it expires', (done) => {

            const key = {
                segment: 'test',
                id: 'test'
            };

            const s3 = new S3(options);
            expect(s3.client).to.not.exist;
            s3.start(() => {

                expect(s3.client).to.exist;
                s3.set(key, 'myvalue', 2000, () => {

                    s3.get(key, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result.item).to.equal('myvalue');
                        setTimeout(() => {

                            s3.get(key, (err, result) => {

                                expect(err).to.not.exist();
                                expect(result).to.not.exist;
                                done();
                            });
                        }, 1500);
                    });
                });
            });
        });
    });


    describe('#drop', () => {

        it('drops an existing item', (done) => {

            const client = new Catbox.Client(S3, options);
            client.start((err) => {

                expect(err).to.not.exist();
                const key = { id: 'x', segment: 'test' };
                client.set(key, '123', 5000, (err) => {

                    expect(err).to.not.exist();
                    client.get(key, (err, result) => {

                        expect(err).to.equal(null);
                        expect(result.item).to.equal('123');
                        client.drop(key, (err) => {

                            expect(err).to.not.exist();
                            done();
                        });
                    });
                });
            });
        });


        it('drops an item from a missing segment', (done) => {

            const client = new Catbox.Client(S3, options);
            client.start((err) => {

                expect(err).to.not.exist();
                const key = { id: 'x', segment: 'test' };
                client.drop(key, (err) => {

                    expect(err).to.not.exist();
                    done();
                });
            });
        });


        it('drops a missing item', (done) => {

            const client = new Catbox.Client(S3, options);
            client.start((err) => {

                expect(err).to.not.exist();
                const key = { id: 'x', segment: 'test' };
                client.set(key, '123', 2000, (err) => {

                    expect(err).to.not.exist();
                    client.get(key, (err, result) => {

                        expect(err).to.equal(null);
                        expect(result.item).to.equal('123');
                        client.drop({ id: 'y', segment: 'test' }, (err) => {

                            expect(err).to.not.exist();
                            done();
                        });
                    });
                });
            });
        });


        it('errors on drop when using invalid key', (done) => {

            const client = new Catbox.Client(S3, options);
            client.start((err) => {

                expect(err).to.not.exist();
                client.drop({}, (err) => {

                    expect(err instanceof Error).to.equal(true);
                    done();
                });
            });
        });


        it('errors on drop when using null key', (done) => {

            const client = new Catbox.Client(S3, options);
            client.start((err) => {

                expect(err).to.not.exist();
                client.drop(null, (err) => {

                    expect(err instanceof Error).to.equal(true);
                    done();
                });
            });
        });


        it('errors on drop when stopped', (done) => {

            const client = new Catbox.Client(S3, options);
            client.stop();
            const key = { id: 'x', segment: 'test' };
            client.connection.drop(key, (err) => {

                expect(err).to.exist;
                done();
            });
        });


        it('errors when cache item dropped while stopped', (done) => {

            const client = new Catbox.Client(S3, options);
            client.stop();
            client.drop('a', (err) => {

                expect(err).to.exist;
                done();
            });
        });
    });


    describe('#validateSegmentName', () => {

        it('errors when the name is empty', (done) => {

            const s3 = new S3(options);
            const result = s3.validateSegmentName('');

            expect(result).to.be.instanceOf(Error);
            expect(result.message).to.equal('Empty string');
            done();
        });


        it('errors when the name has a null character', (done) => {

            const s3 = new S3(options);
            const result = s3.validateSegmentName('\0test');

            expect(result).to.be.instanceOf(Error);
            done();
        });


        it('errors when the name has less than three characters', (done) => {

            const s3 = new S3(options);
            const result = s3.validateSegmentName('yo');

            expect(result).to.be.instanceOf(Error);
            done();
        });


        it('errors when the name has more than 64 characters', (done) => {

            const s3 = new S3(options);
            const result = s3.validateSegmentName('abcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890');

            expect(result).to.be.instanceOf(Error);
            done();
        });


        it('returns null when there are no errors', (done) => {

            const s3 = new S3(options);
            const result = s3.validateSegmentName('valid');

            expect(result).to.not.be.instanceOf(Error);
            expect(result).to.equal(null);
            done();
        });
    });
});
