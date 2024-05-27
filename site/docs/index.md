# Tektite

## What is Tektite?

Tektite is a powerful Apache Kafka compatible event streaming database that combines the functionality seen in vanilla
event streaming platforms such as Apache Kafka or RedPanda with the event processing functionality found in platforms such as
Apache Flink.

* Create Topics just like Kafka or RedPanda. Access them using any Kafka client.
* Filter, Transform and process data using a powerful expression language and function library.
* Implement custom processing as WebAssembly modules running in the server
* Maintain real-time windowed aggregations and materialized views over your data
* Perform stream/stream and stream/table joins to create new streams
* Bridge to and from existing external Kafka compatible servers
* Query the data in any stream or table as if it were a database table

Unlike other offerings, Tektite is not just a bolt on layer over an existing database or event streaming platform.

It is designed from first principles to be fast and scale to any size. It contains its own distributed log structured merge
tree (LSM) for storage of data. At the low level, data is stored in an object store such Amazon S3 or MinIO.

* Learn about Tektite [concepts](conceptual_model.md)
* Try the [getting started](getting_started.md)

It's suggested that the rest of the documentation is read in the order it appears in the left hand navigation list.

## Current Status

* Tektite is currently in active development and working towards a production-ready 1.0 release later in 2024
* Tektite is usable and development is very advanced with most features complete.
* We will be working heavily on automated testing and performance over the next few months to make sure Tektite is rock-solid and fast for the 1.0 release.
