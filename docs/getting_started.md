# Getting Started

First, please [build and install](installation.md) Tektite.

The rest of this guide will assume that you have the Tektite binaries on your `PATH` environment variable and the
`TEKTITE_HOME` environment variable points to the directory where you cloned Tektite.

## Prerequisites

This getting started uses [kcat](https://github.com/edenhill/kcat) which is a useful command line utility which allows you to
directly produce and consume messages to / from any Kafka compatible server.

You can install it on MacOS with

`> brew install kcat`

## Starting a local dev server

For simplicity, we're going to run Tektite with a standalone configuration. This configuration is designed for fast local development
and runs as a single non-clustered server with a built-in object store and cluster manager, so it doesn't need other stuff to 
be running. In this configuration, data is not persistent so it's only suitable for demos and fast local iteration.

Start Tektite:

```
> tektited --config $TEKTITE_HOME/cfg/standalone.conf
2024-05-07 14:37:03.647429	INFO	tektite server 0 started
```

## Start the CLI

Now, we're going to start the Command Line Interpreter (CLI) so we can talk to Tektite. It's started with the `tektite` command:

In a separate console:

```
> tektite --no-verify
```

(the `--no-verify` is necessary as we're running a dev server with a self-signed certificate that won't be trusted)

## Create a topic

```
tektite> my-topic := (topic partitions = 1);
OK
```

This creates a topic called `my-topic` with 1 partition.

(We choose 1 partition here as it makes it easier to see results later on without having to enter lots of messages. But in real life
you'd typically have more than one partition).

## Produce and consume some messages

In a separate console, use `kcat` to consume from the `my-topic` topic and output the message body to stdout:

```
> kcat -q -b 127.0.0.1:8880 -G mygroup my-topic
```

In another console, we will use kcat to send messages to the topic. We will use a bash one-liner that reads text from stdin and 
every time you hit enter, a message will be sent.

```
> bash -c 'while read -r line; do echo "$line" | kcat -b 127.0.0.1:8880 -t my-topic -P -K: ; done'
```

Now type some text in the producer console and hit enter. You should see the messages arriving at the consumer.

```
> bash -c 'while read -r line; do echo "$line" | kcat -b 127.0.0.1:8880 -t my-topic -P -K: ; done'

hello
from
tektite!
```

```
> kcat -q -b 127.0.0.1:8880 -G mygroup my-topic
hello
from
tektite!
```

We've shown that you can create topics and produce and consume from them just like any Kafka compatible server.

Next we'll show you some features that you won't find in existing Kafka implementations.

## Query the data in the topic

With Tektite, you can also query the data in your topic. At the CLI, we'll use the `scan` operator to scan all rows in
`my-topic` and then send the results to the `sort` operator which will sort it by `event-time`.

Operators are always enclosed in parentheses. The arrow operator `->` shows that data flows from one 
operator to the next.

```
tektite> (scan all from my-topic) -> (sort by event_time);
+---------------------------------------------------------------------------------------------------------------------+
| offset               | event_time                 | key                 | hdrs                | val                 |
+---------------------------------------------------------------------------------------------------------------------+
| 0                    | 2024-05-08 09:23:23.002000 | null                | .                   | hello               |
| 0                    | 2024-05-08 09:23:24.778000 | null                | .                   | from                |
| 0                    | 2024-05-08 09:23:28.578000 | null                | .                   | Tektite!            |
+---------------------------------------------------------------------------------------------------------------------+
```

We can use a `filter` operator to filter out small messages:

```
tektite> (scan all from my-topic) -> (filter by len(val) > 6) ->
    (sort by event_time);
+---------------------------------------------------------------------------------------------------------------------+
| offset               | event_time                 | key                 | hdrs                | val                 |
+---------------------------------------------------------------------------------------------------------------------+
| 0                    | 2024-05-08 09:23:28.578000 | null                | .                   | Tektite!            |
+---------------------------------------------------------------------------------------------------------------------+
1 row returned
```

## Create a windowed aggregation

We're going to create a windowed aggregation that keeps track of the count and average, min and max size of
messages in a one-minute window.

Let's go back to the CLI:

```
tektite> my-agg := my-topic ->
   (aggregate count(val), avg(len(val)), min(len(val)), max(len(val))
    size = 1m hop = 10s);
OK
```

This means we're going to create a new stream called `my-agg` and it's created by taking the data of `my-topic` and passing
it to an `aggregate` operator.

The aggregate operator will compute a windowed aggregation of the count of messages, the
average (mean) of the message size, the minimum of the message size and the maximum of the message size in a 1-minute
window. The column `val` in the output from `my-topic` contains the body of the Kafka message. The window has a *hop* of
10s, which means every 10 seconds a window closes and results for the last 10 seconds are output.

Now go back to the `kcat` producer console and send a bunch more messages. Do this for more than 10 seconds. This will
result in windows being closed and aggregate results made available.

We can then look at the aggregate results on the CLI:

```
tektite> (scan all from my-agg);
+----------------------------------------------------------------------------------------------------------------------+
| event_time                 | count(val)           | avg(len(val))      | min(len(val))        | max(len(val))        |
+----------------------------------------------------------------------------------------------------------------------+
| 2024-05-08 11:14:19.998000 | 6                    | 9.500000           | 6                    | 24                   |
+----------------------------------------------------------------------------------------------------------------------+
1 row returned
```

## Joining topics

Let's create another topic:

```
tektite> other-topic := (topic partitions = 1);
OK
```

And then we're going to create a new stream by joining `my-topic` with `other-topic` on the key of the message.

The new stream will receive a message if messages with matching keys arrive on `my-topic` and `other-topic` within a 10-second window.
The new stream is then persisted with a `store stream` operator.

```
tektite> joined := (join my-topic with other-topic by key = key within 10s) ->
    (store stream);
OK
```

Start a new console using `kcat` to produce messages to the topic `other-topic`.

This time when you type messages, type them in the form `key:value` - the part before the `:` will be the key of the message
and the part after will be the value.

Type messages in both `kcat` producer consoles, some with matching keys and some with not. 

```
> bash -c 'while read -r line; do echo "$line" | kcat -b 127.0.0.1:8880 -t my-topic -P -K: ; done'
apples:london
oranges:brazil
bananas:scotland
```

```
> bash -c 'while read -r line; do echo "$line" | kcat -b 127.0.0.1:8880 -t other-topic -P -K: ; done'
apples:france
oranges:uk
pears:uk
```

Now execute a scan in the CLI to see what's in the `joined` stream. We're going to use a `project` operator here
to select out some of the columns and give them new names. The result of a join gives us columns from both inputs and
we don't want them all here.

```
tektite> (scan all from joined) -> (project l_key as key, l_val, r_val);
+--------------------------------------------------------------------------------------------------------------------+
| key                                  | l_val                                | r_val                                |
+--------------------------------------------------------------------------------------------------------------------+
| apples                               | london                               | france                               |
| oranges                              | brazil                               | uk                                   |
+--------------------------------------------------------------------------------------------------------------------+
2 rows returned
```

As you can see, the join has successfully matched the messages with the same keys!

Hope you enjoyed the getting started. Please take a look at the rest of the documentation as this is just a taste of
the powerful things you can do with Tektite.

