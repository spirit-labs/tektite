# Object stores

Tektite uses an object store such as [MinIO](https://min.io/) or [Amazon S3](https://aws.amazon.com/pm/serv-s3/) for durably storing data. Object stores are great
for reliably storing huge amounts of data, but usually work best when data is stored and retrieved in large chunks.
Also, operations, especially write operations, can also have high latency as they may have to touch replicas which might
be in different availability zones for reliability.

This makes object stores a poor choice as a *direct* data store for an event streaming / processing platform where we often
need to make many small reads and writes, and we want low latency for serving data quickly to consumers.

Tektite implements a distributed level based log structured merge tree, where the data is maintained as SSTables in multiple levels 
of the tree. The SSTables are persisted in the object store and 'hot' tables are cached in the Tektite cluster for fast access.
SSTables are asynchronously flushed to the object store.

## MinIO object store client

[MinIO](https://min.io/) is an easy-to-use object store that can scale to very large number of objects, but also be easily deployed as single
server on your laptop during development.

Tektite uses the MinIO golang client to access an object store. MinIO is Amazon S3 compatible so this client also works when
your object store is S3.

### Setting up a MinIO cluster

Please see the [MinIO docs](https://min.io/docs/minio/macos/index.html) for how to setup a MinIO cluster

### Using MinIO for development

Docs to install MinIO for MacOS are [here](https://min.io/docs/minio/macos/index.html) 

In summary:

```
brew install minio/stable/minio
```

### Starting MinIO

Start it with

```
minio server ~/tektite-data
```

This would store the MinIO data in the `~/tektite-data` directory

### MinIO console

This is at [`http://localhost:9000`](http://localhost:9000)

Default username/password is `minioadmin/minioadmin`

Here you can view and create/delete buckets and create keys.


