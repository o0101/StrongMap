import util from 'util';
import StrongMap from '../strongmap.js';

typeTests();

function typeTests() {
  const instance = new StrongMap();
  const minstance = new Map();

  const typeMap = instance instanceof Map;
  const mTypeStrongMap = minstance instanceof StrongMap;
  const typeStrongMap = instance instanceof StrongMap;

  const derived = new instance.constructor;

  const dTypeStrongMap = derived instanceof StrongMap;

  console.log({StrongMap, instance, typeMap, typeStrongMap, mTypeStrongMap, dTypeStrongMap});

  instance.set(1,2);
  console.log({has:instance.has(1)});
  console.log({get:instance.get(1)});

  console.log(instance);
}


