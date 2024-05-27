# Back-filling streams

Let's say you have an existing stream which already contains data, in this case a topic:

```
my-topic := (topic partitions = 16)
```

And you want to create a child stream of `my-topic`:

```
child-stream := my-topic -> (filter by len(val) > 1000) -> (store stream)
```

When `child-stream` is created it won't receive any of the messages in `my-topic` that were there before it was created.
It will only receive new messages that arrive after it was created.

Sometimes you want to run all the existing data into the child stream. You do this with a `backfill` operator:

```
child-stream := my-topic -> (backfill) ->
    (filter by len(val) > 1000) -> (store stream)
```

The `backfill` operator will scan all the data in the parent stream and feed it into the child stream before the child
stream receives any new messages.

Please note, that if the parent stream has a lot of data, it can take a significant time to back-fill the child stream
