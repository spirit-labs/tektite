# Tektite Cookbook

## Topics

### Vanilla topic

```
my-topic := (topic partitions = 16);
```

### Topic with filter on JSON field in message

```
filtered-topic :=
    (kafka in partitions = 16) ->
    (filter by json_string("country", val) == "USA") ->
    (kafka out);    
```

### Topic with filter on Kafka header

```
filtered-topic :=
    (kafka in partitions = 16) ->
    (filter by kafka_header("sender_id", hdrs) == "sender1234") ->
    (kafka out);    
```

### Topic that transforms body

```
transformed-topic :=
    (kafka in partitions = 16) ->
    (project key, hdrs, to_bytes(to_upper(to_string(val)))) ->
    (kafka out);
```

### Topic that transforms body with WASM function

```
transformed-topic :=
    (kafka in partitions = 16) ->
    (project key, hdrs, my_mod.my_wasm_function(val)) ->
    (kafka out);
```

### Write only topic to Tektite stream

```
write-only-topic :=
    (kafka in partitions = 16) ->
    (store stream);
```

### Write only topic that sends to external topic

```
write-only-topic :=
    (kafka in partitions = 16) ->
    (bridge to external-topic props = ("bootstrap.servers" = "mykafka.foo.com:9092"));
```

### Expose existing stream as read-only topic

```
read-only-topic := existing-stream ->
    (project to_bytes(customer_id) as key, nil, to_bytes(description) as val) ->
    (kafka out);
```

### Expose external topic as read only Tektite topic

```
read-only-topic := 
    (bridge from external-topic partitions = 16
        props = ("bootstrap.servers" = "mykafka.foo.com:9092")) ->
    (kafka out);    
```

### Expose external topic as read only Tektite topic after filtering 

```
read-only-topic := 
    (bridge from external-topic partitions = 16
        props = ("bootstrap.servers" = "mykafka.foo.com:9092")) ->
    (filter by json_string("paymment_type", val) == "card") ->    
    (kafka out);    
```

## Joins

### Join two Tektite topics to create new read-only topic

```
sales := (topic partitions = 16);

payments := (topic partitions = 16);

joined := (join sales with payments on cust_id = c_id within 10m) ->
          (project l_key, nil, concat(l_val, r_val)) ->
          (kafka out);
```

### Join Tektite topic with external-topic and store as internal stream

```
sales := (bridge from sales-external partitions = 16
          props = ("bootstrap.servers" = "mykafka.foo.com:9092"));

payments := (topic partitions = 16);

joined := (join sales with payments on cust_id = c_id within 10m) ->
          (store stream);
```

### Enrich Tektite topic with customer data

```
sales := (topic partitions = 16);

enriched := (join sales with cust_data on cust_id *= id) -> (store stream);
```

## Repartitioning

### Repartition external topic

```
repartitioner :=
    (bridge from sales-external partitions = 16
          props = ("bootstrap.servers" = "mykafka.foo.com:9092")) ->
    (partition by json_string("cust_id", val) partitions = 100) ->
    (bridge to sales-by-customer props = ("bootstrap.servers" = "mykafka.foo.com:9092"));              
```

## Aggregations

## Sales totals by region last hour from external topic

```
sales_tots :=
    (bridge from sales-external partitions = 16
        props = ("bootstrap.servers" = "mykafka.foo.com:9092")) ->
    (project json_string("country", val) as country, json_float("amount") as amount) ->    
    (partition by country partitions = 10) ->
    (aggregate count(amount) as num, sum(amount) as tot by country);
```

### Union

