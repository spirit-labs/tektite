# Conceptual Model

Event Streaming platforms such as Apache Kafka or Red Panda and event processing platforms such as Apache Flink have evolved
as separate beasts, but the fundamental abstraction in both is the same - a stream of data.

Tektite exposes the stream processing primitives, so they can be assembled together in different ways to create all the things
you find in event streaming and event processing platforms, but in a single unified platform.

Wouldn’t it be nice if you could deploy a topic, but also with some filtering or processing happening on the server? Or to join
two Kafka topics with a one-liner, and avoid spinning up a separate Flink cluster to do this? Or maybe you want to run some
custom processing on your Kafka cluster?

Let's discuss the conceptual model of Tektite in more detail.

## Streams

### Creating streams

A stream is a sequence of operators. Data flows from left to right, from one operator to the next.

Here's an example of creating a new stream, this stream is just a vanilla Kafka compatible [topic](topics.md):

```
my-topic := (topic partitions = 16)
```

Here's another stream, let's look at the syntax in more detail:

```
my-stream := (kafka in) -> (filter by len(value) > 1000) -> (kafka out)
```

Here, we create a new stream called `my-stream` and it's composed of three operators. There's a `kafka in` operator, 
followed by a `filter` operator, followed by a `kafka out` operator. Operators are chained together using
an arrow `->` - the arrow represents the flow of data from one operator to the next.

Each operator is enclosed in parentheses. Inside the parentheses you will first find the operator name followed by any
specific arguments the operator takes. That's basically it!

Tektite comes with various different operators that you can use to compose streams that do many different things. We'll discuss
the different types of operators in detail in other sections of the documentation.

In this example, the `kafka in` operator exposes a Kafka topic endpoint to the outside world that receives produced messages.
I.e. you can produce messages to it from any Kafka compatible client. The `filter` operator only passes through rows
where the expression evaluates to `true`. The `kafka out` operator exposes the stream, so it can be consumed by any Kafka consumer.

So, what does this stream do? It's a stream that accepts produced messages but filters out messages where the length of the value
is less than or equal 1000 bytes.

Often, a stream takes its input from the output of another stream. In that case the first operator in the new stream is
replaced with the name of the stream that feeds it.

```
child_stream := my-stream -> (aggregate sum(foo) by country)
```

Because streams can feed into other streams, the network of streams forms a graph.

### Deleting streams

You delete streams with the `delete` command which takes the stream name as an argument.

```
delete(my_stream);
```

You won't be allowed to delete a stream if it has child streams as that would result in orphans. You'll have to delete
the child streams first.

## Queries

A query takes data from a source, e.g. by scanning a table or stream or performing a lookup and then passes it through
a sequence of operators which transform the data in some way.

Sounds just like a stream again! In Tektite, we also represent a query as a stream which is fed data from its source, does
some processing on it, then outputs the query results at the end. Queries are discussed in detail in the section on [queries](queries.md).

A couple of examples:

Scan the `cust-data` table from `"smith"` to `"wilson"` then pass through the `cust_name` (after upper-casing it), `age` and
`dob` columns. Filter out rows where age <= 30, then sort the results by `dob`

```
(scan "smith" to "wilson" from cust_data) ->
(project to_upper(cust_name), age, dob) ->
(filter by age > 30) ->
(sort by dob)
```

A simple lookup based on key and pass through the `tx_id`, `cust_id` and `amount` columns.

```
(get tx1234 from transactions) -> (project tx_id, cust_id, amount)
```

## Operators

Operators receive batches of data and output batches of data. Operators are composed into streams as detailed above.

Some operators such as `kafka in` and `bridge from` accept data from outside of Tektite, and have no previous operator, so they
can only appear as the first operator in a stream.

Other operators such as `kafka out` and `bridge to` send data outside of Tektite and have no next operator, so they
can only appear as the last operator in a stream.

Tektite operators include:

* [`topic`](topics.md) - exposes a Kafka producer endpoint and a Kafka consumer endpoint - i.e. it creates a Kafka compatible topic. This is really shorthand for `(kafka in) -> (kafka out)`, but its such a common thing to do we provide a distinct operator for it.
* [`kafka in`](topics.md) - exposes a Kafka endpoint that accepts messages from any Kafka producer.
* [`kafka out`](topics.md) - exposes a Kafka endpoint that can be consumed by any Kafka consumer.
* [`project`](projections.md) - evaluates a list of expressions over the incoming batch to create a new batch. Expressions use a powerful expression language and function library.
* [`filter`](filtering.md) - only passes on those rows where an expression evaluates to `true`.
* [`aggregate`](aggregating.md) - performs windowed or non-windowed aggregations on the incoming data. Outputs updates to the aggregation as windows close.
* [`join`](joins.md) - joins a stream with a stream, or stream with a table based on matching keys. Joins can be inner or outer joins.
* [`union`](union.md) - takes data from multiple inputs and combines them into a single stream
* [`store stream`](persistence.md) - persistently stores the stream
* [`store table`](persistence.md) - stores the stream as a table, i.e. later values of same key overwrite previous ones.
* [`bridge from`](bridging.md) - consumes messages from an external Kafka topic (e.g. in Apache Kafka or RedPanda or another Tektite cluster)
* [`bridge to`](bridging.md) - sends messages to an external Kafka topic (e.g. in Apache Kafka or RedPanda or another Tektite cluster)
* [`backfill`](backfill.md) - used for back-filling a new stream from an existing stream.

There are some operators that can only be used in [queries](queries.md):

* `scan` - performs a range scan based on key from an existing stored table/stream
* `get` - looks up a row based on key
* `sort` - sorts data according to an expression list

## Batches

Data flows through operators in batches. Like a database table, a batch has columns and rows, and the column names and
types are defined by a schema. An example schema for a batch might be:

```
customer_id: string
age: int
registration_date: timestamp
photo: bytes
```

An operator typically has an input schema and an output schema, which can be different.

## Data types

Columns have a datatype. We try and keep the data types as simple as we can - no need for lots of different int or string types.

* `int` - a 64 bit signed integer.
* `float` - a 64 bit floating point number.
* `bool` - a boolean - `true` or `false`
* `decimal(precision, scale)` - exact decimal type, parameterised by `precision` and `scale`.
* `string` - a variable length string.
* `bytes` - a variable length string of bytes
* `timestamp` - represents a date/time with millisecond precision

Like a relational database, column values can be a valid value of the appropriate type or `null`.

