/* eslint-env node, mocha */

'use strict';

const Bluebird = require('bluebird');
const should = require('should');
const assert = require('assert');
const path = require('path');
const fs = require('fs');
const aws = require('aws-sdk');
const Server = require('../lib/Server');
const DataStore = require('../lib/stores/DataStore');
const S3DataStore = require('../lib/stores/S3DataStore');
const File = require('../lib/models/File');

const EVENTS = require('../lib/constants').EVENTS;
const STORE_PATH = '/files';
const AWS_PROFILE = process.env.AWS_PROFILE;
const AWS_BUCKET = process.env.AWS_BUCKET;

const TEST_FILE_SIZE = 960244;
const TEST_FILE_PATH = path.resolve(__dirname, 'test.mp4');

describe('S3DataStore', function() {
  this.timeout(0);

  let server, s3Client;
  let deleteKeys = [];

  beforeEach(() => {
    server = new Server();
    server.datastore = new S3DataStore({
      path: STORE_PATH,
      profile: AWS_PROFILE,
      bucket: AWS_BUCKET,
    });

    s3Client = new aws.S3({
      credentials: new aws.SharedIniFileCredentials({
        profile: AWS_PROFILE,
      }),
      params: {
        Bucket: AWS_BUCKET,
      },
    });
  });

  afterEach(() => {
    return Bluebird.all([
      Bluebird.mapSeries(deleteKeys, (Key) => s3Client.deleteObject({ Key }).promise()),
    ]);
  });

  describe('constructor', () => {
    it('must inherit from Datastore', (done) => {
      assert.equal(server.datastore instanceof DataStore, true);
      done();
    });

    it('must have a create method', (done) => {
      server.datastore.should.have.property('create');
      done();
    });

    it('must have a write method', (done) => {
      server.datastore.should.have.property('write');
      done();
    });

    it('must have a getOffset method', (done) => {
      server.datastore.should.have.property('getOffset');
      done();
    });
  });

  describe('create', () => {
    const req = { headers: { 'upload-length': 1000 }, url: STORE_PATH };

    it('should reject if upload-length is not a number', () => {
      return server.datastore
        .create({
          headers: { 'upload-length': 'non-number' },
          url: STORE_PATH,
        })
        .should.be.rejected();
    });

    it('should reject if both upload-length and upload-defer-length are not provided', () => {
      return server.datastore
        .create({ headers: {}, url: STORE_PATH })
        .should.be.rejected();
    });

    it('should reject when namingFunction is invalid', () => {
      server.datastore.generateFileName = (incomingReq) => incomingReq.body.filename.replace(/\//g, '-');

      return server.datastore
        .create(req)
        .should.be.rejected();
    });

    it('should reject when the s3 bucket doesnt exist', () => {
      const datastore = new S3DataStore({
        path: STORE_PATH,
        profile: AWS_PROFILE,
        bucket: 'non-existant',
      });

      return datastore
        .create(req)
        .should.be.rejected();
    });

    it('should resolve to the File model', (done) => {
      return server.datastore
        .create(req)
        .then((newFile) => {
          assert.equal(newFile instanceof File, true);
          deleteKeys.push(`${newFile.id}.info`);
          done();
        })
        .catch(done);
    });

    it('should use custom naming function when provided', (done) => {
      server.datastore.generateFileName = (incomingReq) => incomingReq.url.replace(/\//g, '-');

      server.datastore
        .create(req)
        .then((newFile) => {
          assert.equal(newFile instanceof File, true);
          assert.equal(newFile.id, '-files');
          deleteKeys.push(`${newFile.id}.info`);
          done();
        })
        .catch(done);
    });

    it(`should fire the ${EVENTS.EVENT_FILE_CREATED} event`, (done) => {
      server.datastore.on(EVENTS.EVENT_FILE_CREATED, (event) => {
        event.should.have.property('file');
        assert.equal(event.file instanceof File, true);
        deleteKeys.push(`${event.file.id}.info`);
        done();
      });

      server.datastore.create(req);
    });
  });

  describe('write', () => {
    it('should open a stream and resolve the new offset', (done) => {
      server.datastore.on(EVENTS.EVENT_FILE_CREATED, (event) => {
        deleteKeys.push(`${event.file.id}.info`);

        const write_stream = fs.createReadStream(TEST_FILE_PATH);

        server.datastore
          .write(write_stream, event.file.id, 0)
          .then((offset) => {
            assert.equal(offset, TEST_FILE_SIZE);
            deleteKeys.push(event.file.id);
            return done();
          })
          .catch(done);
      });

      server.datastore.create({
        headers: { 'upload-length': TEST_FILE_SIZE },
        url: STORE_PATH,
      });
    });

    it(`should fire the ${EVENTS.EVENT_UPLOAD_COMPLETE} event`, (done) => {
      server.datastore.on(EVENTS.EVENT_UPLOAD_COMPLETE, (event) => {
        event.should.have.property('file');
        deleteKeys.push(event.file.id);
        done();
      });

      server.datastore.create({
        headers: { 'upload-length': TEST_FILE_SIZE },
        url: STORE_PATH,
      }).then((file) => {
        const write_stream = fs.createReadStream(TEST_FILE_PATH);

        deleteKeys.push(`${file.id}.info`);
        server.datastore.write(write_stream, file.id, 0);
      });
    });
  });

  describe('getOffset', () => {
    it('should reject non-existant files', () => {
      return server.datastore.getOffset('doesnt_exist')
        .should.be.rejectedWith(404);
    });

    it('should reject directories', () => {
      return server.datastore.getOffset('')
        .should.be.rejectedWith(404);
    });

    it('should resolve existing files with the metadata', (done) => {
      server.datastore.on(EVENTS.EVENT_UPLOAD_COMPLETE, (event) => {
        deleteKeys.push(event.file.id);

        server.datastore.getOffset(event.file.id)
          .should.be.fulfilledWith({
            size: TEST_FILE_SIZE,
            upload_length: TEST_FILE_SIZE,
          });

        return done();
      });

      return server.datastore.create({
        headers: { 'upload-length': TEST_FILE_SIZE },
        url: STORE_PATH,
      }).then((file) => {
        const write_stream = fs.createReadStream(TEST_FILE_PATH);

        server.datastore.write(write_stream, file.id, 0).catch(done);
      }).catch(done);
    });
  });
});
