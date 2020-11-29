# How to make a Hash into a Disk system with fast lookup?

First, consider that Map can have object keys and object values.

So we need a way to stringify random, general objects.

Enter JSON36.

Now we can turn any object and any key into a string, we need a standard way to address these.

Enter discohash.

This gives us 16 hex characters (64 bits) of address space for our keys. Plenty. Now we need a way to put these on disk that will not fail at scale.

Enter sharding.

Given a hash, 'abcdef0123456789', we shard the hash like so:

'ab'
'cd'
'ef'
01234.dat
56789

Which means we have 3 layers of directories, each holding a max 256 entries.

The final directory holds dat files, there can be:

1 million files per directory (actually 16*2**16)
1 million records per file (actually 2**20, same thing)

Of course our 64 bit address space will put us over limit of a filesystem's max number of files, 
but hopefully we will never get close to that.

The point of the sharding is not to stay below the upper limits of the filesystem, but rather to avoid, weird second-order affects that would occur if we simply went:


'abcdef0123456789.dat' and had 1 file for every record.

Or had billions of files in a directory.

Keeping files relatively small, directories relatively small, and files and directories relatively few, gives better performance at scale on almost all filesystems. This is important since we don't want to care what filesystem we are on.


## Revision 2

I can get better performance with the following:

16 layer 1 directories

16 layer 2 directories

256 million files per directory

256 million records per file
