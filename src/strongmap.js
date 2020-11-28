import fs from 'fs';
import Path from 'path';

import {discohash} from 'bebb4185';
import JSON36 from 'json36';

const DEBUG = false;

const VERSION = "1.0";
const VERSION_STRING = "SMV" + VERSION;
const INITIAL_RECORD_LENGTH = 128;
const HashTable = Map; // could also be WeakMap

class StrongMap extends HashTable {};
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
        console.log({path,fileName,recordId,keyString,valueString});
        create(path,fileName,recordId,keyString,valueString);
        return this.target.set(key, value);  
      }

      has(key) {
        const {path,fileName,recordId} = locate(key, this.handler);
        return this.target.has(key);
      }

      get(key) {
        const {path,fileName,recordId} = locate(key, this.handler);
        return this.target.get(key);
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
  function locate(key, handler) {
    const keyString = JSON36.stringify(key);
    let name = handler[N];

    if ( ! name ) {
      name = handler[N] = newRandomName();
    }

    const hash = discohash(keyString).toString(16).padStart(16, '0');
    const parts = ['dicts', name, 'keys', hash.slice(0,2), hash.slice(2,4), hash.slice(4,6)];
    const path = Path.resolve(...parts)
    const fileName = `${hash.slice(6,11)}.dat`;
    const recordId = parseInt(hash.slice(11,16), 16);

    return {path, parts, fileName, recordId, keyString};
  }

  function retrieve(path, fileName, recordId, keyString) {
    const fullPath = Path.resolve(path, fileName);
    if ( ! fs.existsSync(fullPath) ) {
      return undefined; 
    } else {
      return getRecord(path, fileName, recordId, keyString);
    }
  }

  function create(path, fileName, recordId, keyString, valueString) {
    const dirPath = Path.resolve(path);
    const fullPath = Path.resolve(dirPath, fileName);
    if ( ! fs.existsSync(dirPath) ) {
      fs.mkdirSync(dirPath, {recursive:true}); 
    }

    if ( ! fs.existsSync(fullPath) ) {
      createEmptyRecordFile(fullPath);
    }

    createRecord(path, fileName, recordId, keyString, valueString);
  }

  function createEmptyRecordFile(fullPath, name) {
    const SLOT_COUNT = 1;
    const RECORD_COUNT = 0;
    const recordLength = INITIAL_RECORD_LENGTH;

    const header = [
      VERSION_STRING,
      name,
      recordLength,
      RECORD_COUNT,
      SLOT_COUNT
    ];

    const headerLine = header.join(' ');
    const slotLine = newBlankSlot(recordLength);

    // write the file
  }

  function newBlankSlot(len) {
    let str = '';
    for( let i = 0; i < len; i++ ) {
      str += ' ';
    }
    return str;
  }

  function createRecord(path, fileName, recordId, keyString, valueString) {

  }

  function getRecord(path, fileName, recordId, keyString) {

  }

  function newRandomName() {
    const value = (+new Date*Math.random()).toString(36);
    console.log({value});
    return value;
  }
