# Tektite

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

Please see [the docs](http://www.tektitedb.com) to learn more about Tektite.

Please also consult [the wiki](https://github.com/spirit-labs/tektite/wiki)
