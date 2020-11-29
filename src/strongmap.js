import fs from 'fs';
import Path from 'path';

import {discohash} from 'bebb4185';
import JSON36 from 'json36';

const DEBUG = true;

const MAGIC_PREFIX = "SMV";
const VERSION = "1.0";
const VERSION_STRING = MAGIC_PREFIX + VERSION;
const INITIAL_RECORD_LENGTH = 256;
const RECORD_MODE = 0o600;
const HashTable = Map; // could also be WeakMap
const {O_RDWR, O_RDONLY, O_NOATIME, O_NOFOLLOW, O_DSYNC, O_DIRECT} = fs.constants;
const RECORD_OPEN_MODE = O_RDWR | O_NOATIME | O_NOFOLLOW | O_DSYNC; 
const RECORD_READ_MODE = O_RDONLY | O_NOATIME | O_NOFOLLOW | O_DSYNC; 
const HASH_SHARDS = [
  [0,1],
  [1,2],
  [2,9],
  [9,16]
];
const RECORD_ID_SHARD = HASH_SHARDS[HASH_SHARDS.length-1];
const MIN_RECORD_ID = 0;
const MAX_RECORD_ID = 16**(RECORD_ID_SHARD[1] - RECORD_ID_SHARD[0]);
const MAX_RECORDID_LENGTH = MAX_RECORD_ID.toString().length + 1;
const RECORD_ID_RANGE = MAX_RECORD_ID - MIN_RECORD_ID;
const FILE_PATHS = new Map();

class StrongMap extends HashTable {
  name() {}
}
const GeneralFunction = function(...a) { return a; }

const T = Symbol('[[Target]]');
const P = Symbol('[[Proxy]]');
const N = Symbol('[[Name]]');

// static APIHandler
  class StaticAPIHandler {
    // constructor trap to ensure we return an instance that is a proxy
    // so we can intercept its calls and put them to disk
    construct(constructor, args, newTarget) {
      const target = new StrongMap(...args);
      const handler = new APIHandler(target);
      return new Proxy(target, handler); 
    }
  }

// instance APICallHandler
  class APICallHandler {
    constructor(target, handler) {
      this.target = target;
      this.handler = handler;
    }

    construct(target, args, newTarget) {
      DEBUG && console.log(target, args, newTarget);
      return new this.target[this.handler[T]](...args);
    }

    apply (target, thisArg, args) {
      DEBUG && console.log({
        thisArg,                        // the proxy
        target, 
        args,
        thisHandler: this.handler,      // the API handler
        thisTarget: this.target,        // the original object being proxied
        thisArgIsProxy: require('util').types.isProxy(thisArg),
        thisTargetIsProxy: require('util').types.isProxy(target)
      });
      const funcName = this.handler[T];
      this.handler[T] = null;
      const result = this[funcName](...args);

      // don't expose the Map object just the Proxy (thisArg)

        if ( result === this.target ) {
          return thisArg;
        } else {
          return result;
        }

      //this.target[funcName].apply(this.target, args);
    }

    // extended StrongMap specific methods
      name(name) {
        this.handler[N] = name;
      }

    // standard Map API methods
      set(key, value) {
        const {path,fileName,recordId, keyString} = locate(key, this.handler);
        const valueString = JSON36.stringify(value);
        DEBUG && console.log({path,fileName,recordId,keyString,valueString});
        const result = create(path,fileName,recordId,keyString,valueString, this.handler);
        return this.target.set(key, value);  
      }

      has(key) {
        const {path,fileName,recordId,keyString} = locate(key, this.handler);
        DEBUG && console.log({path,fileName,recordId,keyString});
        const result = retrieve(path, fileName, recordId, keyString, this.handler, false);
        return result;
      }

      get(key) {
        const {path,fileName,recordId,keyString} = locate(key, this.handler);
        const rawResult = retrieve(path, fileName, recordId, keyString, this.handler, true);

        if ( rawResult === undefined ) {
          return undefined;
        } else {
          DEBUG && console.log({rawResult, path, fileName, recordId, keyString});
          const [
            RecordIdStr,
            KeyString,
            ValueString
          ] = rawResult.split(/\s+/g);

          // we don't check this earlier, but we could

          if ( keyString !== KeyString ) {
            DEBUG && console.info({
              RecordIdStr,
              KeyString,
              ValueString,
              keyString,
              recordId,
              path,
              fileName,
              name: this.handler[N]
            });
            throw new TypeError(`Collission at recordId ${recordId}`);
          }

          const Key = JSON36.parse(KeyString);
          const Value = JSON36.parse(ValueString);

          return Value 
        }
      }

      delete(key) {

      }

      get [Symbol.iterator]() {
        return this.entries();
      }

      keys() {

      }

      values() {

      }

      entries() {

      }
  }

// instance APIHandler
  class APIHandler {
    constructor(target) {
      const callHandler = new APICallHandler(target, this);
      const funcProxy = new Proxy(GeneralFunction, callHandler);
      let funcName = null;
      Object.defineProperty(this, T, {
        get: () => funcName,
        set: val => funcName = val
      });
      Object.defineProperty(this, P, {
        get: () => funcProxy
      });
    }

    get (target, prop, receiver) {
      DEBUG && console.log(target, prop, receiver);
      switch(prop) {
        default:
          if ( target[prop] instanceof Function ) {
            // save the func name
            this[T] = prop;
            // return the func call proxy;
            return this[P];
          } 
          return target[prop];
          break;
      }
    }
  }

const StrongMapStaticAPI = new Proxy(StrongMap, new StaticAPIHandler());

export default StrongMapStaticAPI;

// record helpers
  function getName(handler) {
    let name = handler[N];

    if ( ! name ) {
      name = handler[N] = newRandomName();
    }

    return name;
  }

  function locate(key, handler) {
    const keyString = JSON36.stringify(key);
    const name = getName(handler);
    const hash = discohash(keyString).toString(16).padStart(16, '0');
    const parts = [
      'dicts', 
      name, 
      'keys', 
      hash.slice(...HASH_SHARDS[0]), 
      hash.slice(...HASH_SHARDS[1]), 
    ];
    const path = Path.resolve(...parts)
    const fileName = `${hash.slice(...HASH_SHARDS[2])}.dat`;
    const recordId = parseInt(hash.slice(...HASH_SHARDS[3]), 16);

    return {path, parts, fileName, recordId, keyString};
  }

  function retrieve(path, fileName, recordId, keyString, handler, fullRecord = false) {
    const fullPath = Path.resolve(path, fileName);
    if ( ! fs.existsSync(fullPath) ) {
      return undefined; 
    } else {
      return getRecord(fullPath, recordId, keyString, handler, fullRecord);
    }
  }

  function create(path, fileName, recordId, keyString, valueString, handler) {
    const dirPath = Path.resolve(path);
    const fullPath = Path.resolve(dirPath, fileName);
    if ( ! fs.existsSync(dirPath) ) {
      fs.mkdirSync(dirPath, {recursive:true}); 
    }

    if ( ! fs.existsSync(fullPath) ) {
      createEmptyRecordFile(fullPath, handler);
    }

    createRecord(fullPath, recordId, keyString, valueString, handler);
  }

  function createEmptyRecordFile(fullPath, handler) {
    const SLOT_COUNT = 1;
    const RECORD_COUNT = 0;
    const recordLength = INITIAL_RECORD_LENGTH;

    const name = getName(handler);

    const header = [
      VERSION_STRING,
      name,
      recordLength,
      RECORD_COUNT,
      SLOT_COUNT
    ];

    const headerLine = header.join(' ').padEnd(recordLength-1, ' ');
    const slotLine = newBlankSlot(recordLength-1);
    const record = [
      headerLine,
      slotLine
    ].join('\n') + '\n';

    // write the file
    const fd = fs.openSync(fullPath, 'ax', RECORD_MODE); 
    fs.fdatasyncSync(fd);    // boss level
    FILE_PATHS.set(fd, fullPath);
    const bytesWritten = fs.writeSync(fd, record, 0, record.length, 0);

    DEBUG && console.log({bytesWritten, fd, path:getPath(fd)});

    // but actually we need to call fsync/fdatasync on the directory
    fs.fdatasyncSync(fd);    // boss level
    fs.closeSync(fd);
    FILE_PATHS.delete(fd);
  }

  function createRecord(fullPath, recordId, keyString, valueString, handler) {
    const fd = fs.openSync(fullPath, RECORD_OPEN_MODE);
    fs.fdatasyncSync(fd);    // boss level
    FILE_PATHS.set(fd, fullPath);

    // check integrity and extract header information
      const PossibleRecordLength = INITIAL_RECORD_LENGTH;
      const HeaderBuf = Buffer.alloc(PossibleRecordLength);

      fs.readSync(fd, HeaderBuf, 0, PossibleRecordLength, 0);
      fs.fdatasyncSync(fd);    // boss level

      const dictName = getName(handler);

      const Header = HeaderBuf.toString().split(/\s+/g);
      const [ 
        VERSION_STRING,
        name,
        RECORD_LENGTH,
        RECORD_COUNT,
        SLOT_COUNT
      ] = Header;

      // we don't actually check version number (for now)
      if ( !VERSION_STRING.startsWith(MAGIC_PREFIX) ) {
        console.warn({
          fullPath, recordId, dictName, VERSION_STRING, name, Header
        });
        throw new TypeError('Incorrect header in record');
      }

      if ( name !== dictName ) {
        console.warn({
          fullPath, recordId, dictName, VERSION_STRING, name, Header
        });
        throw new TypeError('Incorrect dict name in record');
      }

      let slotCount, recordCount, recordLength;
      let newSlotCount, newRecordCount, newRecordLength;

      slotCount = newSlotCount = parseInt(SLOT_COUNT);
      recordCount = newRecordCount = parseInt(RECORD_COUNT);
      recordLength = newRecordLength = parseInt(RECORD_LENGTH);

    // write the record
      const record = Buffer.from(
        [
          recordId,
          keyString,
          valueString
        ]
        .join(' ')
        .padEnd(recordLength-1, ' ') 
        + '\n'
      );

      // check length is OK
        if ( record.length > recordLength ) {
          DEBUG && console.info(`Update record length: ${JSON.stringify({
            newRecordLengthAtLeast: record.length,
            recordLength
          })}`);

          newRecordLength = Math.ceil(record.length * 1.618);
          // get all records and rewrite everything
          throw new Error(`implement record length update`);
        } else {
          newRecordLength = recordLength;
        }

      let slotPosition = getSlot(fd, recordId, newRecordLength, recordCount, slotCount, true);

      DEBUG && console.log({slotPosition});
      if ( slotPosition > 0 ) {
        const bytesWritten = fs.writeSync(fd, record, 0, newRecordLength, slotPosition); 
        fs.fdatasyncSync(fd);    // boss level
        console.log({bytesWritten, record, file: getPath(fd)});
      } else {
        newSlotCount = Math.ceil(slotCount * 1.618);
        // grab all records, and emptyy slots, 
        // map these into a higher space with more empty slots
        // and call try to write our record again
        // repeat until we can write it
      }

    fs.closeSync(fd);
    FILE_PATHS.delete(fd);
  }

  // something like interpolation search
  function getSlot(fd, recordId, recordLength, total_records, total_slots, emptyPosOnly = false, fullValue = false) {
    const expectedSlotIndex = Math.floor((recordId-MIN_RECORD_ID)/RECORD_ID_RANGE*total_slots);
    const probabilityFree = 1-(total_records/total_slots);
    const expectedSlotPosition = recordLength /* for header */ + expectedSlotIndex*recordLength;

    let nextGuess = expectedSlotPosition;

    DEBUG && console.log({fd,recordId,recordLength,total_records,total_slots,expectedSlotIndex,probabilityFree, expectedSlotPosition,nextGuess});
    // allow the guess + 2 probes higher
    for( let i = 0; i < 3; i++ ) {
      const is = isSlotEmpty(fd, nextGuess);

      if ( is.eof ) {
        // some sort of error, but we need to expand record
        DEBUG && console.warn("File should not be end of file at this point", {
          nextGuess,
          recordId, 
          recordLength, 
          total_records, 
          total_slots,
          path: getPath(fd)
        });
        return -1;
      } 

      if ( is.free ) {
        DEBUG && console.info("Slot free", {recordId, nextGuess});
        if ( emptyPosOnly ) {
          return nextGuess;
        } else {
          return undefined;
        }
      } else {
        if ( ! emptyPosOnly ) {
          DEBUG && console.info("Retrieving slot", {recordId, at: nextGuess});
          const readBuffer = Buffer.alloc(recordLength);
          const bytesRead = fs.readSync(fd, readBuffer, 0, MAX_RECORDID_LENGTH, nextGuess);
          let readString = readBuffer.toString('utf8', 0, MAX_RECORDID_LENGTH);
          if ( bytesRead !== MAX_RECORDID_LENGTH ) {
            DEBUG && console.info({
              bytesRead,
              readData: readBuffer.toString(), 
              at: nextGuess,
              recordId, 
              recordLength, 
              total_records, 
              total_slots,
              path: getPath(fd)
            });
            throw new Error(`Corruption at slot ${recordId} at position ${nextGuess}`);
          } else if ( readString.startsWith(recordId+'') ) {
            if ( fullValue ) {
              const remainingBytesToRead = recordLength-MAX_RECORDID_LENGTH;
              const secondBytesRead = fs.readSync(fd, readBuffer, MAX_RECORDID_LENGTH, remainingBytesToRead, nextGuess+MAX_RECORDID_LENGTH);
              if ( secondBytesRead !== remainingBytesToRead ) {
                DEBUG && console.info({
                  bytesRead,
                  readData: readBuffer.toString(), 
                  at: nextGuess,
                  recordId, 
                  recordLength, 
                  total_records, 
                  total_slots,
                  path: getPath(fd)
                });
                throw new Error(`Corruption at slot ${recordId} at position ${nextGuess}`);
              }
              readString = readBuffer.toString();
              return readString;
            } else {
              return true;
            }
          } else {
            // didn't find it yet, keep probing
          }
        } else {
          DEBUG && console.info("Slot taken", {recordId, nextGuess});
        }
      }

      nextGuess += recordLength;
    }

    if ( emptyPosOnly ) {
      return -1;
    } else {
      if ( fullValue ) {
        return undefined;
      } else {
        return false;
      }
    }
  }

  function getPath(fd) {
    return FILE_PATHS.get(fd);
  }

  function isSlotEmpty(fd, pos) {
    const checkBuffer = Buffer.alloc(1);
    const numRead = fs.readSync(fd,checkBuffer,0,1,pos);

    const checkStr = checkBuffer.toString();
    if ( numRead === 0 ) {
      return {eof:true};
    }
    if ( checkStr === ' ' ) {
      return {free:true};
    } else {
      return {free:false};
    }
  }

  function getRecord(fullPath, recordId, keyString, handler, fullRecord) {
    let result;
    let recordExists;

    const fd = fs.openSync(fullPath, RECORD_READ_MODE);
    fs.fdatasyncSync(fd);    // boss level
    FILE_PATHS.set(fd, fullPath);

    // check integrity and extract header information
      const PossibleRecordLength = INITIAL_RECORD_LENGTH;
      const HeaderBuf = Buffer.alloc(PossibleRecordLength);

      fs.readSync(fd, HeaderBuf, 0, PossibleRecordLength, 0);
      fs.fdatasyncSync(fd);    // boss level

      const dictName = getName(handler);

      const Header = HeaderBuf.toString().split(/\s+/g);
      const [ 
        VERSION_STRING,
        name,
        RECORD_LENGTH,
        RECORD_COUNT,
        SLOT_COUNT
      ] = Header;

      // we don't actually check version number (for now)
      if ( !VERSION_STRING.startsWith(MAGIC_PREFIX) ) {
        console.warn({
          fullPath, recordId, dictName, VERSION_STRING, name, Header
        });
        throw new TypeError('Incorrect header in record');
      }

      if ( name !== dictName ) {
        console.warn({
          fullPath, recordId, dictName, VERSION_STRING, name, Header
        });
        throw new TypeError('Incorrect dict name in record');
      }

      let slotCount, recordCount, recordLength;
      let newSlotCount, newRecordCount, newRecordLength;

      slotCount = newSlotCount = parseInt(SLOT_COUNT);
      recordCount = newRecordCount = parseInt(RECORD_COUNT);
      recordLength = newRecordLength = parseInt(RECORD_LENGTH);

    result = getSlot(fd, recordId, recordLength, recordCount, slotCount, false, fullRecord);

    fs.closeSync(fd);
    FILE_PATHS.delete(fd);

    return result;
  }

  function newRandomName() {
    const value = (+new Date*Math.random()).toString(36);
    console.log({value});
    return value;
  }

  function newBlankSlot(len) {
    let str = '';
    for( let i = 0; i < len; i++ ) {
      str += ' ';
    }
    return str;
  }

