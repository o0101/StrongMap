const name = "StrongMap";
const HashTable = Map; // could also be WeakMap

class StrongMap extends HashTable {};

class API {
  get (target, prop, receiver) {
    console.log(target, prop, receiver);
    switch(prop) {
      default:
        if ( target[prop] instanceof Function ) {
          return target[prop].bind(target);
        }
        return target[prop];
        break;
    }
  }

  apply (target, thisArg, args) {
    console.log(target, thisArg, args);
  }
}

class StaticAPI {
  construct(target, args, newTarget) {
    return new Proxy(new StrongMap(...args), new API()); 
  }

  get (target, prop, receiver) {
    console.log(target, prop, receiver);
    switch(prop) {
      case Symbol.species:
        console.log("Symbol.species");
        return StrongMap;
      default:
        return target[prop];
        break;
    }
  }

  apply (target, thisArg, args) {
    console.log(target, thisArg, args);
  }
}


const StrongMapStaticAPI = new Proxy(StrongMap, new StaticAPI());

export default StrongMapStaticAPI;
