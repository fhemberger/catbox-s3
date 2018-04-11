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

if (process.env.S3_FORCE_PATH_STYLE) {
    options.s3ForcePathStyle = process.env.S3_FORCE_PATH_STYLE;
}


// Test shortcuts
const lab = exports.lab = Lab.script();
const describe = lab.describe;
const it = lab.it;
const expect = Code.expect;


describe('S3', () => {

    it('throws an error if not created with new', () => {

        const fn = () => {

            S3();
        };

        expect(fn).to.throw(Error);
    });

    it('creates a new connection', async () => {

        const client = new Catbox.Client(S3, options);
        await client.start()
        expect(client.isReady()).to.equal(true);
    });

    it('closes the connection', async () => {

        const client = new Catbox.Client(S3, options);
        await client.start();
        expect(client.isReady()).to.equal(true);
        client.stop();
        expect(client.isReady()).to.equal(false);
    });


    it('gets an item after setting it', async () => {

        const client = new Catbox.Client(S3, options);
        await client.start();

        const key = { id: 'test/id?with special%chars&', segment: 'test' };
        await client.set(key, '123', 5000);
        const result = await client.get(key);

        expect(result.item).to.equal('123');
    });


    it('buffers can be set and retrieved', async () => {

        const buffer = new Buffer('string value');
        const client = new Catbox.Client(new S3(options));
        await client.start();

        const key = { id: 'buffer', segment: 'test' };
        await client.set(key, buffer, 2000);
        const result = await client.get(key);

        expect(result.item instanceof Buffer).to.equal(true);
        expect(result.item).to.equal(buffer);
    });


    it('buffers are copied before storing', async () => {

        const buffer = new Buffer('string value');
        const client = new Catbox.Client(new S3(options));
        await client.start();

        const key = { id: 'buffer-copied', segment: 'test' };
        await client.set(key, buffer, 2000);
        const result = await client.get(key);

        expect(result.item).to.not.shallow.equal(buffer);
    });


    it('fails setting an item circular references', async () => {

        const client = new Catbox.Client(S3, options);
        await client.start();

        const key = { id: 'circular', segment: 'test' };
        const value = { a: 1 };
        value.b = value;

        await expect(client.set(key, value, 10)).to.reject(new Error('Could not convert to JSON'));
    });


    it('ignored starting a connection twice on same event', () => {

        const client = new Catbox.Client(S3, options);
        const start = async function () {

            await client.start();
            expect(client.isReady()).to.equal(true);
        };

        start();
        start();
    });


    it('ignored starting a connection twice chained', async () => {

        const client = new Catbox.Client(S3, options);
        await client.start();
        expect(client.isReady()).to.equal(true);

        await client.start();
        expect(client.isReady()).to.equal(true);
    });


    it('returns not found on get when using null key', async () => {

        const client = new Catbox.Client(S3, options);
        await client.start();

        const result = await client.get(null);

        expect(result).to.equal(null);
    });


    it('returns not found on get when item expired', async () => {

        const client = new Catbox.Client(S3, options);
        await client.start();

        const key = { id: 'x', segment: 'test' };

        await client.set(key, 'x', 1);
        await new Promise((resolve) => {

            setTimeout(async () => {

                const result = await client.get(key);
                expect(result).to.equal(null);
                resolve();
            }, 2);
        });
    });


    it('errors on set when using null key', async () => {

        const client = new Catbox.Client(S3, options);
        await client.start();

        await expect(client.set(null, {}, 1000)).to.reject();
    });


    it('errors on get when using invalid key', async () => {

        const client = new Catbox.Client(S3, options);
        await client.start();

        await expect(client.get({})).to.reject();
    });


    it('errors on set when using invalid key', async () => {

        const client = new Catbox.Client(S3, options);
        await client.start();

        await expect(client.set({}, {}, 1000)).to.reject();
    });


    it('ignores set when using non-positive ttl value', async () => {

        const client = new Catbox.Client(S3, options);
        await client.start();

        const key = { id: 'x', segment: 'test' };
        await client.set(key, 'y', 0);
    });


    it('returns error on get when stopped', async () => {

        const client = new Catbox.Client(S3, options);
        client.stop();
        const key = { id: 'x', segment: 'test' };

        await expect(client.get(key)).to.reject();
    });


    it('errors on missing segment name', () => {

        const config = {
            expiresIn: 50000
        };

        const fn = () => {

            const client = new Catbox.Client(S3, options);
            new Catbox.Policy(config, client, '');
        };

        expect(fn).to.throw(Error);
    });


    it('errors on bad segment name', () => {

        const config = {
            expiresIn: 50000
        };

        const fn = () => {

            const client = new Catbox.Client(S3, options);
            new Catbox.Policy(config, client, 'a\0b');
        };

        expect(fn).to.throw(Error);
    });


    it('supports empty keys', async () => {

        const client = new Catbox.Client(S3, options);
        await client.start();

        const key = { id: '', segment: 'test' };
        await client.set(key, '123', 5000);
        const result = await client.get(key);

        expect(result.item).to.equal('123');
    });


    describe('#start', () => {

        it('creates an empty client object', async () => {

            const s3 = new S3(options);
            expect(s3.client).to.not.exist;

            await s3.start();
            expect(s3.client).to.exist;
        });
    });

    describe('#stop', () => {

        it('sets the cache client to null', async () => {

            const s3 = new S3(options);
            expect(s3.client).to.not.exist;

            await s3.start();
            expect(s3.client).to.exist;
            s3.stop();
            expect(s3.client).to.not.exist;
        });

    });

    describe('#get', () => {

        it('returns not found on missing segment', async () => {

            const key = {
                segment : 'unknownsegment',
                id      : 'test'
            };
            const s3 = new S3(options);
            expect(s3.client).to.not.exist;

            await s3.start();
            expect(s3.client).to.exist;
            const result = await s3.get(key);

            expect(result).to.not.exist;
        });
    });


    describe('#set', () => {

        it('adds an item to the cache object', async () => {

            const key = {
                segment : 'test',
                id      : 'test'
            };
            const s3 = new S3(options);
            expect(s3.client).to.not.exist;

            await s3.start();
            expect(s3.client).to.exist;
            await s3.set(key, 'myvalue', 2000);
            const result = await s3.get(key);

            expect(result.item).to.equal('myvalue');
        });

        it('removes an item from the cache object when it expires', async () => {

            const key = {
                segment: 'test',
                id: 'test'
            };
            const s3 = new S3(options);
            expect(s3.client).to.not.exist;

            await s3.start();
            expect(s3.client).to.exist;
            await s3.set(key, 'myvalue', 2000);
            const result = await s3.get(key);

            expect(result.item).to.equal('myvalue');
            setTimeout(async () => {

                const result = await s3.get(key);
                expect(result).to.not.exist;
            }, 1500);
        });
    });


    describe('#drop', () => {

        it('drops an existing item', async () => {

            const client = new Catbox.Client(S3, options);
            await client.start();

            const key = { id: 'x', segment: 'test' };
            await client.set(key, '123', 5000);
            const result = await client.get(key);

            expect(result.item).to.equal('123');
            await client.drop(key);
        });


        it('drops an item from a missing segment', async () => {

            const client = new Catbox.Client(S3, options);
            await client.start();

            const key = { id: 'x', segment: 'test' };
            await client.drop(key);
        });


        it('drops a missing item', async () => {

            const client = new Catbox.Client(S3, options);
            await client.start();

            const key = { id: 'x', segment: 'test' };
            await client.set(key, '123', 2000);
            const result = await client.get(key);

            expect(result.item).to.equal('123');
            await client.drop({ id: 'y', segment: 'test' });
        });


        it('errors on drop when using invalid key', async () => {

            const client = new Catbox.Client(S3, options);
            await client.start();

            expect(client.drop({})).to.reject();
        });


        it('errors on drop when using null key', async () => {

            const client = new Catbox.Client(S3, options);
            await client.start();

            expect(client.drop(null)).to.reject();
        });


        it('errors on drop when stopped', async () => {

            const client = new Catbox.Client(S3, options);
            client.stop();
            const key = { id: 'x', segment: 'test' };

            expect(client.connection.drop(key)).to.reject();
        });


        it('errors when cache item dropped while stopped', async () => {

            const client = new Catbox.Client(S3, options);
            client.stop();

            expect(client.connection.drop('a')).to.reject();
        });
    });


    describe('#validateSegmentName', () => {

        it('errors when the name is empty', () => {

            const s3 = new S3(options);
            const result = s3.validateSegmentName('');

            expect(result).to.be.instanceOf(Error);
            expect(result.message).to.equal('Empty string');
        });


        it('errors when the name has a null character', () => {

            const s3 = new S3(options);
            const result = s3.validateSegmentName('\0test');

            expect(result).to.be.instanceOf(Error);
        });


        it('errors when the name has less than three characters', () => {

            const s3 = new S3(options);
            const result = s3.validateSegmentName('yo');

            expect(result).to.be.instanceOf(Error);
        });


        it('errors when the name has more than 64 characters', () => {

            const s3 = new S3(options);
            const result = s3.validateSegmentName('abcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890');

            expect(result).to.be.instanceOf(Error);
        });


        it('returns null when there are no errors', () => {

            const s3 = new S3(options);
            const result = s3.validateSegmentName('valid');

            expect(result).to.not.be.instanceOf(Error);
            expect(result).to.equal(null);
        });
    });
});
