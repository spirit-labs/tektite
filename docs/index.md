# Introducing Tektite

## What is Tektite?

Tektite is a powerful Kafka™ compatible event streaming database that combines functionality seen in vanilla
event streaming platforms such as Apache Kafka™ or RedPanda™ with event processing functionality found in platforms such as
Apache Flink™.

With Tektite, you can create Topics just like Kafka or RedPanda and access them using any Kafka client.

*But, you can also:*

* Filter, Transform and process data using a powerful expression language and function library.
* Implement custom processing as WebAssembly modules running in the server
* Maintain real-time windowed aggregations and materialized views over your data
* Perform stream/stream and stream/table joins to create new streams
* Bridge to and from existing external Kafka compatible servers
* Query the data in any stream or table as if it were a database table

*All in a single platform.*

Unlike most streaming offerings, Tektite is not just a bolt on layer over an existing database or event streaming platform.

It is a true database, designed from first principles to be fast and scale to huge sizes.

Tektite nodes do not use the file system for storing data, instead, they provide a replicated cache for hot data.

Data is stored persistently in an object store like Amazon S3 or MinIO and Tektite includes its own distributed log structured
merge tree ([LSM](https://en.wikipedia.org/wiki/Log-structured_merge-tree)) for efficient retrieval and compaction of stored data.

Removing local file storage makes Tektite clusters easier to deploy and manage, and potentially cheaper to run than existing
alternatives. Not having file(s) per topic partition potentially allows Tektite to scale to many more topics and partitions.

* Learn about Tektite [concepts](conceptual_model.md)
* Try the [getting started](getting_started.md)

It's suggested that the rest of the documentation is read in the order it appears in the left hand navigation list.

## Current Status

* Tektite is currently in active development and working towards a production-ready 1.0 release later in 2024
* Development is advanced with most features complete.
* We will be working heavily on automated testing and performance over the next few months to make sure Tektite is rock-solid and fast for the 1.0 release.
