import util from 'util';
import StrongMap from '../strongmap.js';

typeTests();

function typeTests() {
  const instance = new StrongMap();
  instance.name('happy-test');
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

  const alpha = {alpha:[1,2,3]};

  const beta = {happy:"CRIS", yes:[999,2.12e-94]};

  instance.set(alpha, beta);

  const gamma = instance.get(alpha);

  console.log({alpha, beta, gamma});

  instance.set(new Set([1]), {hi: new Map([['a', {b:2}]])}).set(1,2);

  console.log(instance.get(new Set([1])));

  instance.delete(1);

  console.log(instance.get(1));

  console.log(instance.size);
}


