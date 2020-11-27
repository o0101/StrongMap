# StrongMap

JavaScript Map meets your Hard Disk

## API

[Like Map](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Map)

## Naming the map?

```js
const map = new StrongMap();
map.name('/dev/sda1`); // not smart
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

- no delete (not really, just mark it)
- no instanceof Map (I don't want people confusing them ~ so we can make it a proxy around Map)
- sync file access (and use fsyncSync and fsyncdataSync)
- /prefix1/prefix2/prefix3.dat where prefixes are prefixes of the hash of the primary key and records with similar prefixes are merged into the one record file which has 1 fixed width record per line but width can also change per file (~ binary search) 
