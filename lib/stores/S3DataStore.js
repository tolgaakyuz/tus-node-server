'use strict';

const Bluebird = require('bluebird');
const DataStore = require('./DataStore');
const File = require('../models/File');
const aws = require('aws-sdk');
const _ = require('lodash');

const ERRORS = require('../constants').ERRORS;
const EVENTS = require('../constants').EVENTS;
const TUS_RESUMABLE = require('../constants').TUS_RESUMABLE;

/**
 * S3 Data Store for tusd protocol
 *
 * @author Tolga Akyuz <me@tolga.io>
 */

class S3DataStore extends DataStore {
  constructor(options) {
    super(options);

    if (!options.bucket) {
      throw new Error('S3DataStore must have a bucket');
    }

    this.debug = options.debug || false;
    this.extensions = ['creation', 'creation-defer-length'];
    this.inMemory = {};
    this.chunkSize = 5 * 1024 * 1024;

    if (options.chunkSize > this.chunkSize) {
      this.chunkSize = options.chunkSize;
    }

    const awsOptions = {};

    if (options.profile) {
      const { profile, bucket: Bucket } = options;
      awsOptions.credentials = new aws.SharedIniFileCredentials({profile});
      awsOptions.params = { Bucket };
    } else {
      awsOptions = {
        accessKeyId: options.key,
        secretAccessKey: options.secret,
        apiVersion: '2006-03-01',
        region: options.region || 'us-east-1',
        params: {
          Bucket: options.bucket,
        },
      }
    }

    this.s3 = new aws.S3(awsOptions);
  }

  log(file_id, tags, data, message) {
    if (!this.debug) {
      return;
    }

    console.log(_.compact([
      '[S3DataStore]',
      (file_id) ? `[${file_id}]` : null,
      (tags.length) ? `[${_.map(tags, tag => `#${tag}`)}]` : null,
      (data) ? `[${JSON.stringify(data)}]` : null,
      message || null,
    ]).join(' '));
  }

  /**
   * Create an empty file in S3 to store the metatdata.
   *
   * @param  {object} req http.incomingMessage
   * @param  {File} file
   * @return {Promise}
   */
  create(req) {
    let upload_length = req.headers['upload-length'];
    const upload_defer_length = req.headers['upload-defer-length'];
    const upload_metadata = req.headers['upload-metadata'];
    let file_id;

    if (typeof upload_length === 'undefined' && typeof upload_defer_length === 'undefined') {
      return Bluebird.reject(ERRORS.INVALID_LENGTH);
    }

    upload_length = parseInt(upload_length);

    if (_.isNaN(upload_length)) {
      return Bluebird.reject(ERRORS.INVALID_LENGTH);
    }

    try {
      file_id = this.generateFileName(req);
      this.inMemory[file_id] = [];
    } catch (err) {
      this.log(null, ['create'], null, `check your namingFunction. ${err}`);
      return Bluebird.reject(ERRORS.FILE_WRITE_ERROR);
    }

    this.log(file_id, ['create'], {
      upload_length,
      upload_defer_length,
    }, 'file name generated');

    return this.s3
      .createMultipartUpload({ Key: file_id })
      .promise()
      .then(({ UploadId: upload_id }) => {
        this.log(file_id, ['create'], { upload_id }, 'session created');

        return this.s3
          .putObject({
            Key: `${file_id}.info`,
            Body: new Buffer(JSON.stringify({
              upload_id,
              upload_length,
              upload_offset: 0,
              upload_metadata,
              upload_defer_length,
              upload_part_num: 0,
            }))
          })
          .promise()
          .then(() => {
            this.log(file_id, ['create'], null, 'info file written');

            const file = new File(
              file_id,
              upload_length,
              upload_defer_length,
              upload_metadata
            );

            this.emit(EVENTS.EVENT_FILE_CREATED, { file });
            return Bluebird.resolve(file);
          });
      });
  }

  /**
   * dump(file_id, metadata, )
   *
   * sends `this.chunkSize` amount of in memory data to s3
   *
   * @param  {string} file_id     Name of file
   * @param  {object} metadata    Current metadata of the file
   * @return {Promise}
   */
  dump(file_id, metadata) {
    this.log(file_id, ['dump'], metadata, 'started');

    const { upload_id, upload_part_num } = metadata;
    const memo = Buffer.concat(this.inMemory[file_id]);
    const data = memo.slice(0, this.chunkSize);

    this.inMemory[file_id] = [memo.slice(this.chunkSize)];

    this.log(file_id, ['dump'], { value: data.length }, 'sliced');
    this.log(file_id, ['dump'], { value: Buffer.concat(this.inMemory[file_id]).length }, 'new in memory');
    this.log(file_id, ['dump'], { upload_id, upload_part_num: upload_part_num + 1 }, 'uploading a part');

    return this.s3
      .uploadPart({
        UploadId: upload_id,
        Key: file_id,
        PartNumber: upload_part_num + 1,
        Body: data,
      })
      .promise()
      .then((response) => {
        this.log(file_id, ['dump'], null, 'part uploaded');

        metadata.upload_part_num += 1;
        metadata.upload_offset += data.length;
        metadata.upload_parts = metadata.upload_parts || [];
        metadata.upload_parts.push({
          ETag: response.ETag,
          PartNumber: metadata.upload_part_num
        });

        this.log(file_id, ['dump'], metadata, 'new metadata');

        if (metadata.upload_length === metadata.upload_offset) {
          this.log(file_id, ['dump'], null, 'reached the end');

          return this
            .finish(file_id, upload_id, metadata.upload_parts)
            .then(() => Bluebird.resolve(metadata));
        }

        return this
          .putMetadata(file_id, metadata)
          .then(() => Bluebird.resolve(metadata));
      });
  }

  /**
   * Drain all the in memory data to s3
   *
   * @param  {string} file_id     Name of file
   * @param  {object} metadata    Current metadata of the file
   * @return {Promise}
   */
  drain(file_id, metadata) {
    this.log(file_id, ['drain'], metadata, 'start');

    return this
      .dump(file_id, metadata)
      .then(metadata => {
        this.log(file_id, ['drain'], metadata, 'dumped');
        this.log(file_id, ['drain'], { value: Buffer.concat(this.inMemory[file_id]).length }, 'remaining in memory');

        if (Buffer.concat(this.inMemory[file_id]).length) {
          return this.drain(file_id, metadata);
        }

        this.log(file_id, ['drain'], metadata, 'drained');

        return Bluebird.resolve(metadata);
      });
  }

  /**
   * Get the file metatata from the object in S3,
   * then upload a new version passing through
   * the metadata to the new version.
   *
   * @param  {object} req         http.incomingMessage
   * @param  {string} file_id     Name of file
   * @param  {integer} offset     starting offset
   * @return {Promise}
   */
  write(req, file_id, offset) {
    return new Promise((resolve, reject) => {
      return this.getMetadata(file_id)
        .then((metadata) => {
          const {
            upload_part_num,
            upload_id,
            upload_method,
            upload_length,
            upload_offset,
          } = metadata;

          this.log(file_id, ['write'], metadata, 'start');

          req.on('data', (buffer) => {
            this.inMemory[file_id].push(buffer);
          });

          req.on('end', () => {
            const memo = Buffer.concat(this.inMemory[file_id]);

            this.log(file_id, ['write'], null, 'request drained');
            this.log(file_id, ['write'], { value: memo.length }, 'currently in memory');

            const sendResponse = (metadata) => {
              const file = new File(
                file_id,
                metadata.upload_length,
                metadata.upload_defer_length,
                metadata.upload_metadata
              );

              const new_offset = metadata.upload_offset + Buffer.concat(this.inMemory[file_id]).length;

              this.log(file_id, ['write'], { new_offset }, 'new offset calculated, resolving');

              this.emit(EVENTS.EVENT_UPLOAD_COMPLETE, { file });
              return resolve(new_offset);
            }

            if (upload_offset + memo.length < upload_length) {
              if (memo.length >= this.chunkSize) {
                this.log(file_id, ['write'], { memory: memo.length, chunk: this.chunkSize }, 'memory reached to chunkSize');

                return this
                  .dump(file_id, metadata)
                  .then(metadata => sendResponse(metadata));
              }

              return sendResponse(metadata);
            }

            this.log(file_id, ['write'], null, 'done, draining');

            // done, drain memory
            return this
              .drain(file_id, metadata)
              .then(metadata => sendResponse(metadata));
          });
        });
    });
  }

  finish(file_id, upload_id, parts) {
    this.log(file_id, ['finish'], { upload_id, parts }, 'started');

    return Bluebird.all([
      this.s3.completeMultipartUpload({
        Key: file_id,
        UploadId: upload_id,
        MultipartUpload: { Parts: parts }
      }).promise(),

      this.s3.deleteObject({
        Key: `${file_id}.info`
      }).promise(),
    ]);
  }

  abort(file_id) {
    return this
      .getMetadata(file_id)
      .then(({ upload_id }) => {
        return Bluebird.all([
          this.s3.deleteObject({ Key: `${file_id}.info` }).promise(),
          this.s3.abortMultipartUpload({ Key: file_id, UploadId: upload_id }).promise(),
        ]);
      });
  }

  /**
   * Get file metadata from the S3 Object.
   *
   * @param  {string} fileId name of the file
   * @return {Promise}
   */
  getMetadata(file_id) {
    return this.s3
      .getObject({ Key: `${file_id}.info`})
      .promise()
      .then(({ Body }) => Bluebird.resolve(JSON.parse(Body)))
      .catch((err) => {
        if (err.message === 'NoSuchKey') {
          return Bluebird.reject(ERRORS.FILE_NOT_FOUND);
        }

        return Bluebird.reject(err);
      });
  }

  /**
   * Updates file metadata from the S3 Object.
   *
   * @param  {string} fileId name of the file
   * @return {Promise}
   */
  putMetadata(file_id, new_metadata) {
    return this.s3
      .putObject({
        Key: `${file_id}.info`,
        Body: new Buffer(JSON.stringify(new_metadata))
      }).promise();
  }

  /**
   * Called in HEAD requests. This method should return the bytes
   * writen to the DataStore, for the client to know where to resume
   * the upload.
   *
   * @param  {string} id     filename
   * @return {Promise}       bytes written
   */
  getOffset(file_id) {
    return this
      .getMetadata(file_id)
      .then((metadata) => {
        return Bluebird.resolve(Object.assign(metadata, {
          size: metadata.upload_offset + Buffer.concat(this.inMemory[file_id]).length
        }));
      });
  }
}

module.exports = S3DataStore;
