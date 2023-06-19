# PromiseDB

PromiseDB is a high-performance key-value storage engine built upon the BitCask model.

## Key features

- **Fast read and write:** Efficiently read operations are achieved by directly accessing the data's position in memory using an index, enabling fast retrieval. Write operations are performed in an append-only manner, ensuring speed.
- **Fast startup:** By batching the reading of log entries instead of processing them individually, the number of IO operations is minimized, leading to a considerably faster startup. Moreover, smaller log entries contribute to even greater speed.
- **Fast compaction**: The garbage manager asynchronously counts the number of invalid records in each file. Only files that exceed the threshold of invalid records proportion are compacted during each compaction cycle, ensuring faster compaction process.

**warn**: Due to the bitcask model, all keys must fit in memory.

## Todo

- [ ] Support transaction to ensure ACID properties.
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