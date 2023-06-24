# PromiseDB

PromiseDB is a high-performance key-value storage engine built upon the BitCask model.

## Key features

- **Fast read and write:** Efficiently read operations are achieved by directly accessing the data's position in memory using an index, enabling fast retrieval. Write operations are performed in an append-only manner, ensuring speed.
- **Fast startup:** By batching the reading of log entries instead of processing them individually, the number of IO operations is minimized, leading to a considerably faster startup. Moreover, smaller log entries contribute to even greater speed.
- **Fast compaction**: The garbage manager asynchronously counts the number of invalid records in each file. Only files that exceed the threshold of invalid records proportion are compacted during each compaction cycle, ensuring faster compaction process.

**warn**: Due to the bitcask model, all keys must fit in memory.

## Simple example

```go
func main() {
	option := promisedb.DefaultOption()
	db, err := promisedb.OpenDB(option)
	defer db.Close()
	if err != nil {
		panic(err.Error())
	}

	err = db.Put([]byte("hello"), []byte("world"), 0)
	if err != nil {
		panic(err.Error())
	}

	et, err := db.Get([]byte("hello"))
	if err != nil {
		panic(err.Error())
	}

	if bytes.Compare(et.Value, []byte("world")) != 0 {
		panic(et.Value)
	}

	err = db.Del([]byte("hello"))
	if err != nil {
		panic(err.Error())
	}

	et, err = db.Get([]byte("hello"))
	if err != promisedb.ErrKeyNotExist {
		panic(err.Error())
	}

	err = db.Put([]byte("bigboss"), []byte("2063"), 10*time.Second)
	if err != nil {
		panic(err.Error())
	}

	os.RemoveAll(option.Path)
}
```

## Todo

- [ ] Support transaction(Serializable Isolation) to ensure ACID properties.(Since bitcask needs to put all indexes in memory, the cost of implementing MVCC is too high, so the SSI isolation level is not considered).
- [x] Support TTL(Implement expired deletion based on time heap).
- [x] Support Watch.
- [ ] Support Redis protocol and commands.
- [ ] Support some more complex data structures, such as List, Hash, etc.
- [ ] Support distributed cluster based on Raft algorithm.

**If you have any good ideas, just open a new issue.**

## Contribute

**PRs always welcome.**

I warmly invite developers who are interested in this project to actively engage in its development and join the discussions.

## Contact me

You can contact me from this email: bigboss2063@outlook.com.

## License

Apache License v2 [@bigboss2063](https://github.com/bigboss2063)