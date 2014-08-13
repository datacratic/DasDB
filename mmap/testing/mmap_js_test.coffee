# mmap_js_test.coffee
# Rémi Attab, 19 April 2012
# Copyright (c) 2012 Datacratic.  All rights reserved.
#
# Constants forwarded from the mmap_const.h headers
#


vows   = require "vows"
assert = require "assert"
fs     = require "fs"
util   = require "util"
mmap   = require "mmap_js"

filePrefix = "mmap_js_test_" + process.getuid() + "_"

cleanup = (file) ->
    mmap.cleanup(file) # This should only be called after a crash or during tests.
    fs.unlink(file)
    fs.unlink(file + ".log")

vows.describe('mmap_file_test').export(module).addVows

    "constructor_test":
        topic: ->
            file = filePrefix + "const"
            cleanup(file)
            return { filename: file }

        "param_test": (topic) ->
            assert.throws -> new mmap.MMapFile()
            assert.throws -> new mmap.MMapFile("weeeeee")
            assert.throws -> new mmap.MMapFile(mmap.Create, topic.filename, 4)
            assert.throws -> new mmap.MMapFile(mmap.Create, topic.filename, 7)
            assert.throws -> new mmap.MMapFile(mmap.Create, topic.filename, mmap.Read, 10)
            assert.throws -> new mmap.MMapFile(mmap.Create, topic.filename, mmap.Read, 0, "weee")

        teardown: (topic) -> cleanup(topic.filename)


    "basic_test":
        topic: ->
            file = filePrefix + "basic"
            cleanup(file)
            mmapFile = new mmap.MMapFile(mmap.Create, file, mmap.ReadWrite, mmap.PageSize*8)
            return { filename: file, mmapFile: mmapFile }

        "exists": (topic) ->
            assert.doesNotThrow -> fs.statSync(topic.filename)

        "snapshot": (topic) ->
            assert.doesNotThrow -> topic.mmapFile.snapshot()

        "trieAlloc": (topic) ->
            assert.doesNotThrow -> topic.mmapFile.allocateTrie(10);
            assert.throws -> topic.mmapFile.allocateTrie(10);
            assert.doesNotThrow -> topic.mmapFile.deallocateTrie(10);
            assert.throws -> topic.mmapFile.deallocateTrie(10);

        "open": (topic) ->
            assert.throws -> new mmap.MMapFile(mmap.Create, topic.filename)
            assert.doesNotThrow -> new mmap.MMapFile(mmap.Open, topic.filename)

        teardown: (topic) -> cleanup(topic.filename)


    "unlink_test":
        topic: ->
            file = filePrefix + "basic"
            cleanup(file)
            mmapFile = new mmap.MMapFile(mmap.Create, file)
            return { filename: file, mmapFile: mmapFile }

        "unlink": (topic) ->
            topic.mmapFile.unlink()
            assert.throws -> fs.statSync(topic.filename)

        teardown: (topic) -> cleanup(topic.filename)


    "map_test":
        topic: ->
            file = filePrefix + "map"
            id = 20  # Must be between 1 and 56

            cleanup(file)
            mmapFile = new mmap.MMapFile(mmap.Create, file)
            return { filename: file, mmapFile: mmapFile, id: id }

        # When we add type checks, most of these will throw.
        "good constructor": (topic) ->
            assert.doesNotThrow -> new mmap.MapIntInt(topic.mmapFile, topic.id)
            assert.doesNotThrow -> new mmap.MapIntStr(topic.mmapFile, topic.id)
            assert.doesNotThrow -> new mmap.MapStrInt(topic.mmapFile, topic.id)
            assert.doesNotThrow -> new mmap.MapStrStr(topic.mmapFile, topic.id)

            assert.doesNotThrow -> new mmap.MapSnapshotIntInt(topic.mmapFile, topic.id)
            assert.doesNotThrow -> new mmap.MapSnapshotIntStr(topic.mmapFile, topic.id)
            assert.doesNotThrow -> new mmap.MapSnapshotStrInt(topic.mmapFile, topic.id)
            assert.doesNotThrow -> new mmap.MapSnapshotStrStr(topic.mmapFile, topic.id)

        "bad constructor": (topic) ->
            assert.throws -> new mmap.MapIntInt()
            assert.throws -> new mmap.MapIntInt(topic.mmapFile)
            assert.throws -> new mmap.MapIntInt(topic.mmapFile, "foobar")
            assert.throws -> new mmap.MapIntInt("foobar", topic.id)
            assert.throws -> new mmap.MapIntInt(topic.mmapFile, topic.id, 0)

        "is mutable": (topic) ->
            map = new mmap.MapIntInt(topic.mmapFile, topic.id)
            assert map.clear?
            assert map.size?
            assert map.get?
            assert map.set?

            map = new mmap.MapSnapshotIntInt(topic.mmapFile, topic.id)
            assert !(map.clear?)
            assert !(map.set?)
            assert map.size?
            assert map.get?

        "basic operations": (topic) ->
            longString = "I'm just rambling here so don't bother to read this..."

            map = new mmap.MapStrStr(topic.mmapFile, topic.id)
            assert map.empty()

            assert map.set(longString, longString)
            assert !map.set(longString, "blah")
            assert map.exists(longString)
            assert.equal map.size(), 1

            assert.equal map.get(longString), longString
            assert map.exists(longString)

            assert map.del(longString)
            assert !map.del(longString)
            assert !(map.get(longString)?)
            assert !map.exists(longString)
            assert.equal map.size(), 0
            assert map.empty()

            assert.doesNotThrow -> map.clear()

        "foreach": (topic) ->
            map = new mmap.MapIntInt(topic.mmapFile, topic.id)

            n = 100
            map.set(i,i) for i in [n..0]

            i = 0;
            sum = 0;
            map.foreach (key, value)->
                assert.equal key, i
                assert.equal key, value
                sum += value
                i++;

            assert.equal sum, (n*(n+1))/2
            map.clear()

        "foreach with prefix": (topic) ->
            map = new mmap.MapStrInt(topic.mmapFile, topic.id)

            map.set("abc", 20);
            map.set("acb", 21);
            map.set("b",   1);
            map.set("bac", 2);
            map.set("bca", 4);
            map.set("cab", 22);
            map.set("cba", 23);

            sum = 0;
            map.foreach "b", (key, value)->
                sum += value

            assert.equal sum, 7
            map.clear()


        "snapshot": (topic) ->
            map = new mmap.MapIntInt(topic.mmapFile, topic.id)

            n = 100
            map.set(i,i) for i in [n..0]

            # snapshot the map
            snapshot = new mmap.MapSnapshotIntInt(topic.mmapFile, topic.id)

            # modifying the original map won't affect the snapshot
            map.clear()

            sum = 0
            snapshot.foreach (key, value) -> sum += value
            assert.equal sum, (n*(n+1))/2

        teardown: (topic) ->
            cleanup(topic.filename)
