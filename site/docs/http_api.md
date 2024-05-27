# Tektite HTTP API

Tektite provides an HTTP API that allows Tektite statements and queries to be executed.

It's used by the Tektite CLI, and can be used by Tektite client applications directly or via the golang Tektite client
which is a wrapper around the HTTP API. The API is HTTP-2 only and requires TLS.

Please note, this is an API that uses HTTP as the transport, it is not designed to be a RESTful API.

## Configuration

The addresses from which the API is served is determined by the `http-api-addresses` server configuration
property. The path root for the API is determined by the `http-api-path` configuration property, and the default is
`tektite`.

## Errors

If the request succeeded HTTP response `200/OK` will be returned, otherwise a different HTTP response code will be returned
and the response body will contain an error message.


## Executing statements

To execute a statement you send a `POST` request to the path `tektite/statement`. The body of the request contains the
statement to execute.

For example:

```
POST mytektite.foo.com:7770/tektite/statement
my-topic := (topic partitions = 16)
```

```
POST mytektite.foo.com:7770/tektite/statement
delete(my-topic)
```

## Executing queries


To execute a query you send a `POST` request to the path `tektite/query`. The body of the request contains the
query to execute.

For example:

```
POST mytektite.foo.com:7770/tektite/query
(scan all from sales_figures) -> (sort by country, city)
```

```
POST mytektite.foo.com:7770/tektite/query
(get "customer1234" from cust_sales) -> (project cust_name, city, sales_total)
```

By default, query results are returned in the response in [JSON lines](https://jsonlines.org/) format. Each row of the result is
received as a JSON array, and lines are separated with newline (`\n`)

```
["dave smith", "london", 123.67]
["susan hill", "birmingham", 56.43]
["andrew wilson", "manchester", 12.99]
```

If you add the query parameter `col_headers` with the value `true`, then you will also receive column names and column types
as the first two lines received:

```
["cust_name", "city", "sales_total"]
["string", "string", "float"]
["dave smith", "london", 123.67]
["susan hill", "birmingham", 56.43]
["andrew wilson", "manchester", 12.99]
```

Any `decimal` values in the result will be returned as JSON strings so as not to lose any precision.

You can also ask to receive the results encoded in [Apache Arrow](https://arrow.apache.org/) format if you prefer. To do this
add an `accept` header in the HTTP request with value `x-tektite-arrow`

## Prepared queries

Tektite supports *prepared queries*. These are the same as *prepared statements* in other databases - we call them prepared
queries as we only support preparing for queries not statements in general.

A prepared query is parsed and the query state is set up before execution. At execution time just the parameters of the
query (if any) are passed to the server in order to execute it. This has less overhead than parsing it and constructing the query
state on each invocation of the query, if the query is executed many times.

You prepare a [query](queries.md) by executing a `prepare` statement:

```
POST mytektite.foo.com:7770/tektite/statement
prepare my_query := (scan $name_start:string to $name_end:string) ->
    (filter by country == $country:string)
```

The query can have zero or more parameters, these are denoted by `$<param_name>:<param_type>`, e.g. `$name_start:string` is 
a parameter called `name_start` with a type of `string`.

Parameters can be of any [Tektite data types](conceptual_model.md#data-types) and can appear in the query anywhere a column
identifier is legal.

To execute a prepared query, you send a `POST` request to the `execute` endpoint, and the body of the request must contain
a JSON object with the name of the query to execute in the `QueryName` field and the prepared query arguments to execute it as a JSON
array in the `Args` field.

```
POST POST mytektite.foo.com:7770/tektite/execute
{"QueryName": "my_query", "Args": ["fox", "smith", "UK"]}
```

Tektite will attempt to convert arguments of a particular JSON type to the corresponding Tektite type in a sensible way.

* Tektite type `int`: will convert from JSON types `number`, `string`.
* Tektite type `float`: will convert from JSON types `number`, `string`.
* Tektite type `bool`: will convert from JSON types `bool`, `string` (`"true"`/`"TRUE"` and `"false"`/`"FALSE"`).
* Tektite type `decimal(p,s)`: will convert from JSON types `string`, `number`.
* Tektite type `string`: will convert from JSON types `string`, `number`, `bool` (`"true"`, `"false"`).
* Tektite type `bytes`: **must** be passed as a base64 encoded JSON `string`.
* Tektite type `timestamp`: will convert from a JSON `number` that represents number of milliseconds from Unix Epoch.
* For a `null` Tektite argument value, a JSON `null` should be passed as the argument.

## Registering / unregistering WASM modules

You use the HTTP API to register / unregister WASM modules:

To register a module, send a `POST` request to the path `tektite/wasm-register`. The body of the request contains a JSON object
with the JSON metadata for the module in a field `MetaData` and the module bytes as a base 64 encoded string in a field `ModuleData`:

For example:

```
POST mytektite.foo.com:7770/tektite/wasm-register
{ 
   "MetaData": {
       "name": "my-wasm-mod",
        "functions": {
            "foo": {
                "paramTypes": ["string"],
                "returnType": "int"
            }
       }
   },
   "ModuleData": "<base64 encoded module bytes>"
}
```

To unregister a module, send a `POST` request to the path `tektite/wasm-unregister`. The body of the request contains the
name of the module to unregister

```
POST mytektite.foo.com:7770/tektite/wasm-unregister
my-wasm-mod
```