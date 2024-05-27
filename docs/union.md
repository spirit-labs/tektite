# Unions

The `union` operator is used to merge multiple streams into a single stream.

Let's say you have 4 different stock feed streams; `nasdaq-ticker`, `nyse-ticker`, `euro-ticker`, `lse-ticker`, each one
for different exchanges. Each feed stream has the following schema:

```
event_time: timestamp
ticker: string
price: decimal(10, 4)
```

The following would combine them into a single stream:

```
combined-stream :=
    (union nasdaq-ticker, nyse-ticker, euro-ticker, lse-ticker) ->
    (store stream)
```

The `union` operator requires that all input streams have the same column types and number of columns. If that is not the case a projection should be
applied to the input before it is sent to the `union`.

Lets sat the `nyse-ticker` actually had the following schema:

```
event_time: timestamp
ticker: string
price: float
```

So we need to convert the `price` field to a `decimal(10, 4)`:

```
nyse-adapted := nyse-ticker ->
   (project symbol, to_decimal(tick, 10, 4))

combined-stream :=
    (union nasdaq-ticker, nyse-adapted, euro-ticker, lse-ticker) ->
    (store stream)

```

The `union` output column names are taken from the column names of the first input.

