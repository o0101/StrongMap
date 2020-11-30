import fs from 'fs';
import Path from 'path';

import {discohash} from 'bebb4185';
import JSON36 from 'json36';

const DEBUG = false;

const MAGIC_PREFIX = "SMV";
const VERSION = "1.0";
const VERSION_STRING = MAGIC_PREFIX + VERSION;
const INITIAL_RECORD_LENGTH = 1024;
const ENTRY_FILE_HEADER_LENGTH = 128;
const RECORD_MODE = 0o600;
const HashTable = Map; // could also be WeakMap
const {O_CREATE, O_RDWR, O_RDONLY, O_NOATIME, O_NOFOLLOW, O_DSYNC, O_DIRECT} = fs.constants;
const RECORD_OPEN_MODE = O_CREATE | O_RDWR | O_NOATIME | O_NOFOLLOW | O_DSYNC; 
const RECORD_READ_MODE = O_RDONLY | O_NOATIME | O_NOFOLLOW | O_DSYNC; 
const HASH_LENGTH = 16;
const HASH_SHARDS = [
  [0,1],
  [1,2],
  [2,9],
  [9,HASH_LENGTH]
];
const RECORD_ID_SHARD = HASH_SHARDS[HASH_SHARDS.length-1];
const MIN_RECORD_ID = 0;
const MAX_RECORD_ID = 16**(RECORD_ID_SHARD[1] - RECORD_ID_SHARD[0]);
const MAX_RECORDID_LENGTH = MAX_RECORD_ID.toString().length + 1;
const RECORD_ID_RANGE = MAX_RECORD_ID - MIN_RECORD_ID;
const FILE_PATHS = new Map();

class StrongMap extends HashTable {
  name() {}
  root() {}
}
const GeneralFunction = function(...a) { return a; }

const T = Symbol('[[Target]]');
const P = Symbol('[[Proxy]]');
const N = Symbol('[[Name]]');
const R = Symbol('[[Root]]');

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

      root(root) {
        this.handler[R] = root;
      }

    // standard Map API methods
      set(key, value) {
        const {path,fileName,recordId, keyString} = locate(key, this.handler);
        const valueString = JSON36.stringify(value);
        DEBUG && console.log({path,fileName,recordId,keyString,valueString});
        const result = create(path,fileName,recordId,keyString,valueString, this.handler);
        // make apply return the Proxy
        return this.target;
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
        const {path,fileName,recordId, keyString} = locate(key, this.handler);
        DEBUG && console.log({path,fileName,recordId,keyString});
        const result = remove(path,fileName,recordId,keyString,this.handler);
        return result;
      }

      *keys() {
        for( const [key, value] of this.entries() ) {
          yield key;
        }
      }

      *values() {
        for( const [key, value] of this.entries() ) {
          yield value;
        }
      }

      *entries() {
        for( const [key, value] of streamEntries(this.handler) ) {
          yield [key, value];
        }
      }

    // standard Map API altered for our call semantics
      [Symbol.iterator]() {
        return this.entries();
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
        case 'size':
          return getSize(this);
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

  function getRoot(handler) {
    let root = handler[R];

    if ( ! root ) {
      root = handler[R] = '.';
    }

    return root;
  }

  function locate(key, handler) {
    const keyString = JSON36.stringify(key);
    const name = getName(handler);
    const hash = discohash(keyString).toString(16).padStart(16, '0');
    const parts = [
      getRoot(handler),
      'dicts', 
      name, 
      'keys', 
      hash.slice(...HASH_SHARDS[0]), 
      hash.slice(...HASH_SHARDS[1]), 
    ];
    const path = Path.resolve(...parts)
    const fileName = `${hash.slice(...HASH_SHARDS[2])}.dat`;
    const recordId = parseInt(hash.slice(...HASH_SHARDS[3]), 16);

    const entryFile = Path.resolve(getRoot(handler), 'dicts', name, 'entries.dat');

    return {path, parts, fileName, recordId, keyString, hash, entryFile};
  }

  function locateFromHash(hash, handler) {
    const name = getName(handler);
    const parts = [
      getRoot(handler),
      'dicts', 
      name, 
      'keys', 
      hash.slice(...HASH_SHARDS[0]), 
      hash.slice(...HASH_SHARDS[1]), 
    ];
    const path = Path.resolve(...parts)
    const fileName = `${hash.slice(...HASH_SHARDS[2])}.dat`;
    const recordId = parseInt(hash.slice(...HASH_SHARDS[3]), 16);

    return {path, parts, fileName, recordId};
  }

  function retrieve(path, fileName, recordId, keyString, handler, 
      fullRecord = false, showEmpty = false) {
    const fullPath = Path.resolve(path, fileName);
    if ( ! fs.existsSync(fullPath) ) {
      if ( showEmpty ) {
        return {empty:true};
      } else {
        return undefined; 
      }
    } else {
      return getRecord(fullPath, recordId, keyString, handler, fullRecord, showEmpty);
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

    return createRecord(fullPath, recordId, keyString, valueString, handler);
  }

  function remove(path, fileName, recordId, keyString, handler) {
    const dirPath = Path.resolve(path);
    const fullPath = Path.resolve(dirPath, fileName);
    if ( ! fs.existsSync(dirPath) ) {
      return false;
    }

    if ( ! fs.existsSync(fullPath) ) {
      return false;
    }

    removeRecord(fullPath, recordId, keyString, handler);
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
    const record = Buffer.from([
      headerLine,
      slotLine
    ].join('\n') + '\n');

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
    let result;

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

      const slot = getSlot(fd, recordId, newRecordLength, recordCount, slotCount, true);

      DEBUG && console.log({slot});
      if ( slot.position > 0 ) {
        const bytesWritten = fs.writeSync(fd, record, 0, newRecordLength, slot.position); 
        fs.fdatasyncSync(fd);    // boss level
        DEBUG && console.log({bytesWritten, record, file: getPath(fd)});
        if ( bytesWritten === newRecordLength ) {
          result = true;
        } else {
          console.warn(`Error writing record`, {
            recordId, record, slotPosition, recordCount, 
            slotCount, path: getPath(fd)
          });
          throw new Error(`Error writing record ${recordId}`);
        }
      } else {
        newSlotCount = Math.ceil(slotCount * 1.618);
        // grab all records, and emptyy slots, 
        // map these into a higher space with more empty slots
        // and call try to write our record again
        // repeat until we can write it

        throw new Error(`Implement add more slots`);
      }

    if ( result ) {
      if ( slot.nomatch ) {
        recordCount += 1;
        addEntry(keyString, handler);
      }
      Header[3] = recordCount;
      const HeaderLine = Header.join(' ').padEnd(recordLength-1, ' ') + '\n';
      const bytesWritten = fs.writeSync(fd, HeaderLine, 0, newRecordLength, 0); 
      fs.fdatasyncSync(fd);    // boss level
    }

    fs.closeSync(fd);
    FILE_PATHS.delete(fd);
  }

  function removeRecord(fullPath, recordId, keyString, handler) {
    const fd = fs.openSync(fullPath, RECORD_OPEN_MODE);
    fs.fdatasyncSync(fd);    // boss level
    FILE_PATHS.set(fd, fullPath);

    let result;

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
      const record = Buffer.from(newBlankSlot(recordLength-1) + '\n');

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

      const slot = getSlot(fd, recordId, newRecordLength, recordCount, slotCount, true);

      DEBUG && console.log({slot});
      if ( slot.position > 0 && slot.match ) {
        const bytesWritten = fs.writeSync(fd, record, 0, newRecordLength, slot.position); 
        fs.fdatasyncSync(fd);    // boss level
        DEBUG && console.log({bytesWritten, record, file: getPath(fd)});
        if ( bytesWritten === newRecordLength ) {
          result = true;
        } else {
          console.warn(`Error deleting record`, {
            recordId, record, slot, recordCount, 
            slotCount, path: getPath(fd)
          });
          throw new Error(`Error deleting record ${recordId}`);
        }
      } else {
        result = false;
      }

    if ( result ) {
      recordCount -= 1;
      Header[3] = recordCount;
      const HeaderLine = Header.join(' ').padEnd(recordLength-1, ' ') + '\n';
      const bytesWritten = fs.writeSync(fd, HeaderLine, 0, newRecordLength, 0); 
      fs.fdatasyncSync(fd);    // boss level
      removeEntry(keyString, handler);
    }

    fs.closeSync(fd);
    FILE_PATHS.delete(fd);


    return result;
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
        return {nomatch:true, position:-1};
      } 

      if ( is.free ) {
        DEBUG && console.info("Slot free", {recordId, nextGuess});
        if ( emptyPosOnly ) {
          return {nomatch:true, position:nextGuess};
        } else {
          return undefined;
        }
      } else {
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
          if ( ! emptyPosOnly ) {
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
            return {match:true, position:nextGuess};
          }
        } else {
          DEBUG && console.info("Slot taken but not matching recordId", {recordId, nextGuess});
        }
      }

      nextGuess += recordLength;
    }

    if ( emptyPosOnly ) {
      return {nomatch:true, position:-1};
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

  function getRecord(fullPath, recordId, keyString, handler, fullRecord, showEmpty) {
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

    if ( result === undefined && showEmpty ) {
      return {empty: true}; 
    }
    return result;
  }

  function newRandomName() {
    const value = (+new Date*Math.random()).toString(36);
    DEBUG && console.log({value});
    return value;
  }

  function newBlankSlot(len) {
    let str = '';
    for( let i = 0; i < len; i++ ) {
      str += ' ';
    }
    return str;
  }

  function addEntry(keyString, handler) {
    const key = JSON36.parse(keyString);
    const {hash,entryFile} = locate(key, handler);
     
    if ( ! fs.existsSync(entryFile) ) {
      createEntryFile(entryFile, handler);
    }

    const fd = fs.openSync(entryFile, RECORD_OPEN_MODE);
    fs.fdatasyncSync(fd);    // boss level

    const Header = Buffer.alloc(ENTRY_FILE_HEADER_LENGTH);

    const bytesRead = fs.readSync(fd, Header, 0, ENTRY_FILE_HEADER_LENGTH, 0);

    if ( bytesRead !== ENTRY_FILE_HEADER_LENGTH ) {
      console.warn({bytesRead, entryFile, hash, headerRead: Header.toString()});
      throw new TypeError(`Error reading entry file`);
    }

    const [
      magic,
      name, 
      RECORD_COUNT,
      LINE_COUNT
    ] = Header.toString().split(/\s+/g);

    if ( magic !== VERSION_STRING ) {
      console.warn({name, magic, VERSION_STRING, entryFile, hash, headerRead: Header.toString()});
      throw new TypeError(`Corrupt entry file: magic prefix doesn't match version string`);
    }

    if ( name !== getName(handler) ) {
      console.warn({name, entryFile, hash, headerRead: Header.toString()});
      throw new TypeError(`Corrupt entry file: name doesn't match dict name`);
    }

    const recordCount = parseInt(RECORD_COUNT);
    const lineCount = parseInt(LINE_COUNT);

    if ( Number.isNaN(recordCount) ) {
      console.warn({name, entryFile, hash, recordCount, headerRead: Header.toString()});
      throw new TypeError(`Corrupt entry file: record count is not a number`);
    }

    if ( Number.isNaN(lineCount) ) {
      console.warn({name, entryFile, hash, lineCount, headerRead: Header.toString()});
      throw new TypeError(`Corrupt entry file: line count is not a number`);
    }

    const writePosition = ENTRY_FILE_HEADER_LENGTH+lineCount*(HASH_LENGTH+1) 

    // write the entry
    const bytesWritten = fs.writeSync(fd, hash+'\n', writePosition);
    if ( bytesWritten !== HASH_LENGTH+1 ) {
      console.warn({bytesWritten, entryFile, hash, headerRead: Header.toString()});
      throw new TypeError(`Error writing entry file`);
    }

    // update the header
    fs.fdatasyncSync(fd);    // boss level
    const newRecordCount = recordCount + 1;
    const newLineCount = lineCount + 1;

    const header = [
      VERSION_STRING,
      name,
      newRecordCount,
      newLineCount
    ];

    const headerLine = header.join(' ').padEnd(ENTRY_FILE_HEADER_LENGTH-1, ' ');
    const record = Buffer.from(headerLine + '\n');

    const headerBytesWritten = fs.writeSync(fd, record, 0, record.length, 0);
    if ( headerBytesWritten !== ENTRY_FILE_HEADER_LENGTH) {
      console.warn({
        bytesWritten, entryFile, hash, 
        newHeader: record.toString(), headerRead: Header.toString()
      });
      throw new TypeError(`Error writing entry file header`);
    }

    fs.fdatasyncSync(fd);    // boss level
    fs.closeSync(fd);
  }

  function removeEntry(keyString, handler) {
    const key = JSON36.parse(keyString);
    const {hash,entryFile} = locate(key, handler);
     
    const fd = fs.openSync(entryFile, RECORD_OPEN_MODE);
    fs.fdatasyncSync(fd);    // boss level

    const Header = Buffer.alloc(ENTRY_FILE_HEADER_LENGTH);

    const bytesRead = fs.readSync(fd, Header, 0, ENTRY_FILE_HEADER_LENGTH, 0);

    if ( bytesRead !== ENTRY_FILE_HEADER_LENGTH ) {
      console.warn({bytesRead, entryFile, hash, headerRead: Header.toString()});
      throw new TypeError(`Error reading entry file`);
    }

    const [
      magic,
      name, 
      RECORD_COUNT,
      LINE_COUNT
    ] = Header.toString().split(/\s+/g);

    if ( magic !== VERSION_STRING ) {
      console.warn({name, magic, VERSION_STRING, entryFile, hash, headerRead: Header.toString()});
      throw new TypeError(`Corrupt entry file: magic prefix doesn't match version string`);
    }

    if ( name !== getName(handler) ) {
      console.warn({name, entryFile, hash, headerRead: Header.toString()});
      throw new TypeError(`Corrupt entry file: name doesn't match dict name`);
    }

    const recordCount = parseInt(RECORD_COUNT);

    if ( Number.isNaN(recordCount) ) {
      console.warn({name, entryFile, hash, recordCount, headerRead: Header.toString()});
      throw new TypeError(`Corrupt entry file: record count is not a number`);
    }

    const newRecordCount = recordCount - 1;

    const header = [
      VERSION_STRING,
      name,
      newRecordCount,
      LINE_COUNT
    ];

    const headerLine = header.join(' ').padEnd(ENTRY_FILE_HEADER_LENGTH-1, ' ');
    const record = Buffer.from(headerLine + '\n');

    const headerBytesWritten = fs.writeSync(fd, record, 0, record.length, 0);
    if ( headerBytesWritten !== ENTRY_FILE_HEADER_LENGTH) {
      console.warn({
        bytesWritten, entryFile, hash, 
        newHeader: record.toString(), headerRead: Header.toString()
      });
      throw new TypeError(`Error writing entry file header`);
    }

    fs.fdatasyncSync(fd);    // boss level
    fs.closeSync(fd);
  }

  function createEntryFile(entryFile, handler) {
    const RECORD_COUNT = 0;
    const LINE_COUNT = 0;
    const name = getName(handler);

    const header = [
      VERSION_STRING,
      name,
      RECORD_COUNT,
      LINE_COUNT
    ];

    const headerLine = header.join(' ').padEnd(ENTRY_FILE_HEADER_LENGTH-1, ' ');
    const record = Buffer.from(headerLine + '\n');

    // write the file
    if ( ! fs.existsSync(Path.dirname(entryFile) ) ) {
      fs.mkdirSync(Path.dirname(entryFile), {recursive:true});
    }

    let fd;

    if ( ! fs.existsSync(entryFile) ) {
      fd = fs.openSync(entryFile, 'ax', RECORD_MODE);
    } else {
      fd = fs.openSync(entryFile, RECORD_OPEN_MODE);
    }

    fs.fdatasyncSync(fd);    // boss level
    const bytesWritten = fs.writeSync(fd, record, 0, record.length, 0);

    console.log({bytesWritten});

    DEBUG && console.log({bytesWritten, fd, path:getPath(fd)});

    // but actually we need to call fsync/fdatasync on the directory
    fs.fdatasyncSync(fd);    // boss level
    fs.closeSync(fd);
  }

  function getSize(handler) {
    const {entryFile} = locate('', handler);
     
    if ( ! fs.existsSync(entryFile) ) {
      createEntryFile(entryFile, handler);
    }

    const fd = fs.openSync(entryFile, RECORD_READ_MODE);
    fs.fdatasyncSync(fd);    // boss level

    const Header = Buffer.alloc(ENTRY_FILE_HEADER_LENGTH);

    const bytesRead = fs.readSync(fd, Header, 0, ENTRY_FILE_HEADER_LENGTH, 0);

    if ( bytesRead !== ENTRY_FILE_HEADER_LENGTH ) {
      console.warn({bytesRead, entryFile, hash, headerRead: Header.toString()});
      throw new TypeError(`Error reading entry file`);
    }

    const [
      magic,
      name, 
      RECORD_COUNT
    ] = Header.toString().split(/\s+/g);

    if ( magic !== VERSION_STRING ) {
      console.warn({name, magic, VERSION_STRING, entryFile, hash, headerRead: Header.toString()});
      throw new TypeError(`Corrupt entry file: magic prefix doesn't match version string`);
    }

    if ( name !== getName(handler) ) {
      console.warn({name, entryFile, hash, headerRead: Header.toString()});
      throw new TypeError(`Corrupt entry file: name doesn't match dict name`);
    }

    const recordCount = parseInt(RECORD_COUNT);

    if ( Number.isNaN(recordCount) ) {
      console.warn({name, entryFile, hash, recordCount, headerRead: Header.toString()});
      throw new TypeError(`Corrupt entry file: record count is not a number`);
    }

    fs.fdatasyncSync(fd);    // boss level
    fs.closeSync(fd);

    return recordCount;
  }

  function *streamEntries(handler) {
    const {entryFile} = locate('', handler);
     
    if ( ! fs.existsSync(entryFile) ) {
      createEntryFile(entryFile, handler);
    }

    const fd = fs.openSync(entryFile, RECORD_READ_MODE);
    fs.fdatasyncSync(fd);    // boss level

    const Header = Buffer.alloc(ENTRY_FILE_HEADER_LENGTH);

    const bytesRead = fs.readSync(fd, Header, 0, ENTRY_FILE_HEADER_LENGTH, 0);

    if ( bytesRead !== ENTRY_FILE_HEADER_LENGTH ) {
      console.warn({bytesRead, entryFile, hash, headerRead: Header.toString()});
      throw new TypeError(`Error reading entry file`);
    }

    const [
      magic,
      name, 
      RECORD_COUNT
    ] = Header.toString().split(/\s+/g);

    if ( magic !== VERSION_STRING ) {
      console.warn({name, magic, VERSION_STRING, entryFile, hash, headerRead: Header.toString()});
      throw new TypeError(`Corrupt entry file: magic prefix doesn't match version string`);
    }

    if ( name !== getName(handler) ) {
      console.warn({name, entryFile, hash, headerRead: Header.toString()});
      throw new TypeError(`Corrupt entry file: name doesn't match dict name`);
    }

    const recordCount = parseInt(RECORD_COUNT);

    if ( Number.isNaN(recordCount) ) {
      console.warn({name, entryFile, hash, recordCount, headerRead: Header.toString()});
      throw new TypeError(`Corrupt entry file: record count is not a number`);
    }

    const HashBuffer = Buffer.alloc(HASH_LENGTH);
    let readBytes;
    let nextPosition = ENTRY_FILE_HEADER_LENGTH;
    DEBUG && console.log({nextPosition});
    while((readBytes = fs.readSync(fd, HashBuffer, 0, HASH_LENGTH, nextPosition)) === HASH_LENGTH) {
      fs.fsyncSync(fd);       // boss level
      const hash = HashBuffer.toString(); 
      const {empty, key,value} = getRecordFromHash(hash, handler);
      if ( !empty ) {
        yield [key, value];
      } else {
        DEBUG && console.log({hashEmpty:empty, hash});
      }
      nextPosition += HASH_LENGTH + 1;
      DEBUG && console.log({nextPosition});
    }

    fs.fdatasyncSync(fd);    // boss level
    fs.closeSync(fd);

    return recordCount;
  }

  function getRecordFromHash(hash, handler) {
    const {path,fileName,recordId} = locateFromHash(hash, handler);
    DEBUG && console.log({path,fileName,recordId});
    const result = retrieve(path, fileName, recordId, undefined, handler, true, true)
    if ( result.empty ) {
      return {empty: true};
    } else {
      const [
        RecordIdStr,
        KeyString,
        ValueString
      ] = result.split(/\s+/g);

      const key = JSON36.parse(KeyString);
      const value = JSON36.parse(ValueString);

      return {key,value};
    }
  }
