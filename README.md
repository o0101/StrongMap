# StrongMap

JavaScript Map meets your Hard Disk

## API

[Like Map](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Map)

## Naming the map?

```js
const map = new StrongMap();
map.name('/dev/sda1`); // not smart
map.name('happy-map'); // more smart
```

## Getting a named map?

```js
const map = StrongMap.fromDisk('/dev/sda1');
// it's a map
```

## What else?

Nothing. 

~## Why this?~

## Design

- sync file access (and use fsyncSync and fsyncdataSync)

## Caveats

- slow
- buggy

