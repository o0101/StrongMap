# :dvd: [StrongMap](https://github.com/c9fe/StrongMap) [![visitors+++](https://hits.seeyoufarm.com/api/count/incr/badge.svg?url=https%3A%2F%2Fgithub.com%2Fc9fe%2FStrongMap&count_bg=%2379C83D&title_bg=%23555555&icon=&icon_color=%23E7E7E7&title=%28today%2Ftotal%29%20visitors%2B%2B%2B%20since%20Dec%201%202020&edge_flat=false)](https://hits.seeyoufarm.com) ![version](https://img.shields.io/npm/v/sirdb?label=version) ![npm](https://img.shields.io/npm/dt/node-strongmap)

JavaScript Map meets your Hard Disk

## API

[Like Map](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Map)

## Get

```console
$ npm i --save node-strongmap
```

## Naming the map?

```js
const map = new StrongMap();
map.name('/dev/sda1`); // not smart
map.name('happy-map'); // more smart
```

## Getting a named map?

```js
const map = StrongMap();
map.name('/dev/sda1');
// it's a map
```

## What else?

Nothing. 

~## Why this?~

## Design

- sync file access (and use fsyncSync and fsyncdataSync)

## Caveats

- very slow
- inefficient
- incomplete
- buggy and leaky

## How to set the root directory for maps?

```js
map.root('path/to/where/i/store/my/dicts');
```
