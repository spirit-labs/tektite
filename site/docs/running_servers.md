# Running Tektite servers

> This guide assumes you have installed Tektite and the Tektite binaries directory is on the `PATH` environment variable

## Starting Tektite servers

A tektite server is started using the `tektited` command. At minimum, it takes a path to a server configuration file:

```
> tektited --config cfg/standalone.conf
```

The Tektite distribution contains a selection of server configuration files that can be used for different purposes, e.g.
running a standalone server, or a cluster. They can be used as a starting point for your own installation.

## Running a local Tektite server during development

When working with Tektite during development it's useful to spin up a Tektite server on your local machine to interact
with. We provide a couple of pre-made configurations for working with standalone servers, depending on whether you want
a persistent server or non-persistent one.

## Running a local non-persistent server

The simplest way to spin-up a Tektite server is using the [cfg/standalone.conf](https://github.com/spirit-labs/tektite/blob/main/cfg/standalone.conf) configuration file. This starts a Tektite
server with a built-in non-persistent object store and cluster manager and doesn't require any other servers to be running.
However, all data will be lost when the server is restarted.

```
> tektited --config cfg/standalone.conf
```

## Running a local persistent server

At the low-level Tektite stores data in an [object store](object_stores.md) like MinIO or Amazon S3. If you want a local
development server but don't want to lose data when it restarts you can use a configuration that uses a local MinIO instance
for persistence.

First, please make sure [MinIO is installed](object_stores.md#installing-minio) and [started](object_stores.md#starting-minio) and you've
created a bucket called `tektite-dev` and a [secret key and access key](object_stores.md#creating-keys).

Now, edit `cfg/standalone-minio.conf` and enter your secret key and access key in the `minio-secret-key` and
`minio-access-key` configuration properties.

Now you can start a Tektite server with:

```
> tektited --config cfg/standalone-minio.conf
```

## Shutting down Tektite servers

A tektite cluster (including a persistent 'cluster' of one node) must always be shutdown using the `shutdown` option. You shouldn't
shut down the whole cluster using `CTRL-C` or sending a KILL signal to the process. A tektite cluster stores data in memory
which is asynchronously flushed to object storage. If you kill the whole cluster it's possible that you could lose data that
has not been flushed yet.

The `shutdown` option starts a clean shutdown process - it contacts each server in the cluster and ensures that it has
flushed all data and performs other cleanup operations.

When shutting down a cluster, you provide the same config that you used to start the cluster

E.g.
```
> tektited --config cfg/standalone-minio.conf --shutdown
```

Or
```
> tektited --config cfg/cluster-minio.conf --shutdown
```

This can be run from any machine that has network access to the Tektite servers remoting listeners (as defined by the `cluster-addresses` 
property in the server configuration).

## Starting a Tektite cluster

A tektite cluster requires an object store and `etcd` to be running.

* Object store. Tektite uses an object store such as [MinIO](https://min.io/) or [Amazon S3](https://aws.amazon.com/pm/serv-s3/) to store data persistently.
Please see the [chapter on object stores](object_stores.md) for more information. For development purposes, MinIO is recommended as it's easy to set up locally.
* Tektite uses `etcd` so Tektite cluster nodes can agree about cluster membership. Tektite has a fairly lightweight usage of `etcd` and **does not** use it for
 storing stream data.

## Starting a cluster for local development

### Start MinIO

First, please make sure [MinIO is installed](object_stores.md#installing-minio) and [started](object_stores.md#starting-minio) and you've created a bucket called `tektite-dev`
and a [secret key and access key](object_stores.md#creating-keys).

Now, edit `cfg/cluster-minio.conf` and enter your secret key and access key in the `minio-secret-key` and
`minio-access-key` configuration properties.

### Start etcd

Start etcd in standalone mode with:

```
> etcd
```

### Start Tektite nodes

The config `cfg/cluster-minio.conf` is configured for three Tektite nodes.

In separate consoles run:

```
> tektited --config cfg/cluster-minio.conf --node-id 0
2024-05-18 14:44:09.860009	INFO	tektite server 0 started
```

```
> tektited --config cfg/cluster-minio.conf --node-id 1
2024-05-18 14:44:09.860025	INFO	tektite server 1 started
```

```
> tektited --config cfg/cluster-minio.conf --node-id 2
2024-05-18 14:44:10.151445	INFO	tektite server 2 started
```

## Setting up a Tektite cluster

### Object store

Tektite currently supports the MinIO object store client which can be used with a MinIO cluster or Amazon S3.

### etcd

An etcd cluster is required for managing cluster membership. It is not used for storing data. A single etcd cluster
can be used many Tektite clusters

### Configuring Tektite servers

As a starting point you can base your server config on [cfg/cluster-minio.conf](https://github.com/spirit-labs/tektite/blob/main/cfg/cluster-minio.conf)

MinIO is Amazon S3 API compatible so the MinIO client will work with S3 as well as MinIO clusters.

Configure the following properties to connect to the object store you wish to use:

* `minio-endpoint` - the address of the MinIO/S3 endpoint
* `minio-access-key` - your access key
* `minio-secret-key` = - your secret key
* `minio-bucket-name` - the name of the bucket where Tektite data will be stored. You should use a separate bucket per \
writable Tektite cluster

Configure `cluster-name` to give each of your Tektite clusters a unique name.

Configure `cluster-addresses`. These are the addresses at which Tektite nodes connect to each other for internal
cluster traffic. They need to be accessible to each other, but they don't need to be accessible from the client side.

Configure `http-api-addresses`. These are the addresses from which the Tektite HTTP API is served. This is used
by the CLI and Tektite client applications.

Configure `kafka-server-addresses`. These are the addresses that the Kafka server listens at. Kafka clients connect here.

Configure `admin-console-addresses`. These are the addresses that the admin console is served from.

Configure `cluster-state-manager-listen-addresses`. These are the addresses of your etcd nodes. Note that one etcd cluster can be shared
by many Tektite clusters, as long as each Tektite cluster has a unique `cluster-name`.

Other properties that you might commonly want to configure:

* `processor-count` - this is the number of processors in the entire Tektite cluster. Typically, you would match this to
`2 * (available cores on the machine) * number of nodes in cluster`

### Starting Tektite cluster nodes

Start each node with the command line:

```
tektited --config cfg/cluster-minio.conf --node-id <node_id>
```

Where `<node_id>` is the unique id of each node.
If there are `n` nodes in the cluster, node id should be in the range `0` to `n - 1`.






