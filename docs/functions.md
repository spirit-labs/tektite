# Functions reference

Tektite has a built-in library of functions that can be used in any expression.

The library is very much growing, and
we will add to it gradually over time as needs demand. As well as the built-in functions you can write your own functions
using [WebAssembly](wasm.md).

## Function signature notation

When writing out a function signature in this guide we use the notation `<name>: <type>` to show the name and type of a function
parameter. We also use `: <type>` at the end of the signature to show the return type of the function.

Where `<type>` is a Tektite data type; one of `int`, `float`, `bool`, `decimal`, `string`, `bytes` or `timestamp`,
or a type parameter written as an upper case letter, e.g. `T` or `R` where the type parameter refers to a Tektite type.

If a type parameter is present, the set of concrete types that it allows will be specified like:

`T ∈ {int, bool, decimal}`

Sometimes parameters can be optional. If so, they are placed in square brackets.

For example:

```
foo(x: int, y: float, [z: bool]): string
```
Function name is `foo`. First parameter `x` is of type `int`. Second parameter `y` is of type float. Third parameter `z` is
of type bool and is optional. Function returns a `string`

```
bar(x: bool, r: R): R
R ∈ {int, float, bool, decimal, string, bytes, timestamp}
```
Function name is `bar`. First parameter `x` is of type `bool`. Second parameter `r` is of type `R`. Type `R` is one
of `int`, `float`, `bool`, `decimal`, `string`, `bytes` or `timestamp`. Function returns type `R`.

## Control flow functions

### if

```
if(e: bool, t: R, f: R): R
R ∈ {int, float, bool, decimal, string, bytes, timestamp}
```

Like an `if` statement.

If `e` is `true` then return `t`, else return `f`.

Examples:

```
(project if(col2 > 100, "antelopes", "zebras"))
```

### case

```
case(t: T, c1:T, r1:R, c2:T, r2:R, c3:T, r3:R, ..., d:R): R
```

Like a case statement. You provide the value to be tested followed by a list of (value to compare to, result to return)
pairs, and a default.

The value to be tested is `t`. It is tested against `c1`, `c2`, `c3`, etc. The first one it matches against, the corresponding
return value is returned. For example, if it matches `c2`, then `r2` is returned. If it matches `c3` then `r3` is returned. If
it does not match any of the `c` values, then the default value `d` is returned.

Examples:

```
project(
    case(col2, 
         17, "USA",
         23, "UK",
         100, "FRANCE",
         "EGYPT")
)
```

## Comparison functions

### is_null
```
is_null(e: T): bool
R ∈ {int, float, bool, decimal, string, bytes, timestamp}
```

If `e` is `null` then return `true`, else return `false.

Examples:

```
(filter by is_null(col2))
```

### is_not_null
```
is_not_null(e: T): bool
R ∈ {int, float, bool, decimal, string, bytes, timestamp}
```

If `e` is not `null` then return `true`, else return `false.

Examples:

```
(filter by is_not_null(col2))
```

Equivalent to:

```
!is_null(col2)
```

### in

```
in(t: E, e1: E, e2: E, e3: E, ...): bool
```

If `t` is equal to any of `e1`, `e2`, `e3`, ... then return `true`, else return `false`

Examples:

Returns `true` if `col7` is equal to `"usa"`, `"uk"` or `"germany"`
```
(filter by in(col7, "usa", "uk", "germany"))
```

Returns `true` if `col1`, `col2` or `col3` are equal to `"usa"`
```
(filter by in("usa", col1, col2, col3))
```

## Decimal functions

### decimal_shift

```
decimal_shift(d: decimal, places: int, [round: bool]): decimal
```

Shifts a decimal `d` by `places` decimal places. If optional `round` is `true`, then result is rounded. Default value of `round` is `true`.

Examples:

```
decimal_shift(col7, 2)
decimal_shift(col7, -4, false)
```

## String functions

### starts_with

```
starts_with(s: string, prefix: string): bool
```

Returns `true` if the string `s` starts with the prefix `prefix`, else returns `false`.

Examples:

```
(filter by starts_with(name, "smi"))
```

### ends_with

```
ends_with(s: string, suffix: string): bool
```

Returns `true` if the string `s` ends with the suffix `suffix`, else returns `false`.

Examples:

```
(filter by ends_with(filename, ".txt"))
```

### matches

```
matches(s: string, re: string): bool
```

Returns true if the string `s` matches the regular expression `re`, else returns `false`

The [regular expression syntax](https://pkg.go.dev/regexp/syntax) is that used by golang, and is the same general syntax as used by many other languages.

Examples:

Filter on `feed_name` starts with `"news"`
```
(filter by matches(feed_name, "^news")) 
```

Sort by whether `description` contains the string `"apples"`
```
(filter by matches(description, "apples")) 
```

### trim

```
trim(s: string, cutset: string): string
```

Trims any characters contained in `cutset` from the beginning and end of `s` and returns the result.

Examples:

Filter by `country` column == `"UK"` after trimming any leading and trailing whitespace (any of space character, tab character, newline or carriage return)
```
(filter by trim(country, " \t\n\r") == "UK")
```

### ltrim

```
ltrim(s: string, cutset: string): string
```

Trims any characters contained in `cutset` from the beginning of`s` and returns the result.

Examples:

Filter by the `city` column == `"Bristol"` after trimming any leading whitespace (any of space character, tab character, newline or carriage return)
```
(filter by trim(city, " \t\n\r") == "Bristol")
```

### rtrim

```
rtrim(s: string, cutset: string): string
```

Trims any characters contained in `cutset` from the end of`s` and returns the result.

Examples:

Filter by the `city` column == `"Bristol"` after trimming any trailing whitespace (any of space character, tab character, newline or carriage return)
```
(filter by trim(city, " \t\n\r") == "Bristol")
```

### to_lower

```
to_lower(s: string): string
```

Converts `s` to lower case characters and returns the result.

Examples:

```
(filter by to_lower(city) == "manchester")
```

### to_upper

```
to_upper(s: string): string
```

Converts `s` to upper case characters and returns the result.

Examples:

```
(filter by to_upper(city) == "MANCHESTER")
```

### sub_str

```
sub_string(s: string, start_index: int, end_index: int): string
```

Returns a sub string of `s` from index `start_index` (inclusive) to index `end_index` (exclusive)

Indexes start at zero.

Examples:

This filter would match if the `description` column contained `xyzapplesxyz`. Note that the end index is *exclusive*.
```
(filter by sub_str(description, 3, 9) == "apples") 
```

### replace

```
replace(s: string, old: string, new: string): string
```

Replaces all instances of `old` in `s` with `new`.

Examples:

Replaces any occurrence of the string `"my_secret_stuff"` in the column `confidential` with the string `"****"`

```
(project replace(confidential, "my_secret_stuff", "****") as redacted)
```

### sprintf

```
sprintf(pattern: string, p1: T1, p2: T2, p3: T3, ...) 
```

This acts like the `sprintf` function that you find in many languages such as C, C++, golang, etc.
See [supported syntax](https://pkg.go.dev/fmt)

It takes a pattern string `pattern` and a variable length list of values of any data type, and formats a string based on the
pattern and the values are returns it.

Examples:

```
(project sprintf("sensor_id:%d country:%s temperature:%.2f",
                 sensor_id, country, temper) as summary)
```

## Type conversion functions

### to_int

```
to_int(v: T): int
T ∈ {int, float, decimal, string, timestamp}
```

Converts the value to type `int`

### to_float

```
to_float(v: T): float
T ∈ {int, float, decimal, string, timestamp}
```

Converts the value to type `int`

### to_string

```
to_string(v: T): string
T ∈ {int, float, bool, decimal, string, bytes, timestamp}
```

Converts the value to type `string`

### to_decimal

```
to_decimal(v: T, prec: int, scale: int): decimal
T ∈ {int, float, decimal, string, timestamp}
```

Converts the value to type `decimal(prec, scale)`

### to_bytes

```
to_bytes(v: T): bytes
T ∈ {string, bytes}
```

Converts the value to type `bytes`


### to_timestamp

```
to_timestamp(v: T): bytes
T ∈ {int, timestamp}
```

Converts the value to type `timestamp`. If the argument is of type 'int' this is interpreted as milliseconds past Unix epoch.

## Date / time functions

### format_date

```
format_date(t: timestamp, format: string): string
```

Formats the timestamp `t` into a string using the format `format`.

The format syntax used is the [golang time format](https://go.dev/src/time/format.go)

Examples:

Format a timestamp to a string using RFC3339 format
```
(project format_date(event_time, "2006-01-02T15:04:05Z07:00"))
```

Format in "YYYY-MM-DD HH:MM:SS.mmm" format:

```
(project format_date(event_time, "2006-01-02 15:04:05.000"))
```

### parse_date

```
parse_date(s: string, format: string): timestamp
```

Parses the string `s` using the format `format` to give a timestamp value.

The format syntax used is the [golang time format](https://go.dev/src/time/format.go)

Examples:

```
(project format_date(date_str, "2006-01-02 15:04:05.000") as purchase_time)
```

### year

```
year(d: timestamp): int
```

Extracts the year as an int from the timestamp value `d`.

Examples:

```
(filter by year(event_time) == 2023)
```

### month

```
month(d: timestamp): int
```

Extracts the month as an int (from 1 = January to 12 = December) from the timestamp value `d`.

Examples:

```
(filter by month(event_time) == 5)
```

### day

```
day(d: timestamp): int
```

Extracts the day of the month from the timestamp value `d`.

Examples:

```
(filter by month(event_time) == 2 && day(event_time) == 29)
```

### hour

```
hour(d: timestamp): int
```

Extracts the hour of the day (from 0 to 23) from the timestamp value `d`.

Examples:

```
(filter by hour(event_time) == 7)
```

### minute

```
minute(d: timestamp): int
```

Extracts the minute of the hour (from 0 to 59) from the timestamp value `d`.

Examples:

```
(filter by minute(event_time) == 23)
```

### second

```
second(d: timestamp): int
```

Extracts the second of the minute (from 0 to 59) from the timestamp value `d`.

Examples:

```
(filter by second(event_time) == 23)
```

### millis

```
millis(d: timestamp): int
```

Extracts the milliseconds of the second (from 0 to 999) from the timestamp value `d`.

Examples:

```
(filter by millis(event_time) == 777)
```

### now

```
now(): timestamp
```

Returns the current server time as a timestamp.

Examples:

```
(project cust_id, name, value, now() as process_time)
```

## JSON functions

The `json_xxx` functions, where `xxx` is `int`, `string`, etc., extract a value from a JSON object.

They all take a `path` parameter as the first parameter. The `path` parameter traverses fields from the top object, separated
by `.`, to traverse into an element of the array, you use `array_field_name[index]`

Given a Kafka message whose body is JSON, with the following format:

```json
{
  "tx_id": 12345,
  "cust_id": "cust65432",
  "fruit": [
    "apples",
    "oranges"
  ],
  "teapot": {
    "colour": "blue",
    "age": 23
  }
}
```

Path `tx_id` would retrieve `12345`

Path `cust_id` would retrieve `"cust65432"`

Path `fruit[1]` would retrieve `"oranges"`

Path `teapot.colour` would retrieve "blue"

### json_int

```
json_int(path: string, json: J): int
J ∈ {string, bytes}
```

Extracts the JSON field given by `path` as an int from the JSON given by `json`. Returns null if the field does not exist.

Example, extracts value from the body of incoming Kafka message

```
my-topic := (kafka in topics = 16) ->
    (project(json_int("tx_id", val) as transaction_id,
        json_int("teapot.age") as teapot_age)) ->
    (store stream)
```

### json_float

```
json_float(path: string, json: J): float
J ∈ {string, bytes}
```

Extracts the JSON field given by `path` as a float from the JSON given by `json`. Returns null if the field does not exist.

Example, extracts value from the body of incoming Kafka message

```
my-topic := (kafka in topics = 16) ->
    (project(json_float("fraud_detect.params[3].fraud_prob", val) as fraud_prob)) ->
    (store stream)
```

### json_bool

```
json_bool(path: string, json: J): bool
J ∈ {string, bytes}
```

Extracts the JSON field given by `path` as a bool from the JSON given by `json`. Returns null if the field does not exist.

Example, extracts value from the body of incoming Kafka message

```
my-topic := (kafka in topics = 16) ->
    (project(json_bool("results.confirmed.card_declined", val) as card_declined)) ->
    (store stream)
```

### json_string

```
json_string(path: string, json: J): string
J ∈ {string, bytes}
```

Extracts the JSON field given by `path` as a string from the JSON given by `json`. Returns null if the field does not exist.

Example, extracts value from the body of incoming Kafka message

```
my-topic := (kafka in topics = 16) ->
    (project(json_string("customer.details.name", val) as customer_name)) ->
    (store stream)
```

### json_raw

```
json_raw(path: string, json: J): string
J ∈ {string, bytes}
```

Extracts the JSON field given by `path` as a raw string from the JSON given by `json`. Returns null if the field does not exist.

Examples:

Using the example JSON from the beginning of this section:

```
json_raw("teapot", val)
```

Would return the string:

```json
{
  "colour": "blue",
  "age": 23
}
```

```
json_raw("tx_id", val)
```

Would return a string containing:

```
12345
```

```
json_raw("fruit[0]", val)
```

Would return a string containing:

```
"apples"
```

Note, the double quotes. `json_raw` returns exactly what was present in the original JSON object at that field.

### json_is_null

```
json_is_null(path: string, json: J): bool
J ∈ {string, bytes}
```

Returns `true` if the JSON field given by the path has a value of JSON null, else returns `false`. Returns null if the field does not exist.

Examples:

```
(project if(json_is_null("country", val), "UK", json_string("country", val)))
```

### json_type

```
json_type(path: string, json: J): string
J ∈ {string, bytes}
```

Returns the JSON type of the field given by `path` in the JSON given by `json`. Returns null if the field does not exist.

Valid return values are:

* `"null"`: If field has JSON type `null`
* `"bool"`: If field has JSON type `bool`
* `"string"`: If field has JSON type `string`
* `"number"`: If field has JSON type `number`
* `"json"`: If field is a JSON object or array


## Kafka helper functions

### kafka_header

```
kafka_header(header_name: string, raw_headers: bytes): string
```

Extracts the Kafka message header with name `header_name` from the raw headers in `raw_headers`. Header is returned as a string.
Returns null if header does not exist in message.

Examples:

```
my-topic := (topic partitions = 16) ->
    (project to_string(key), kafka_header("sender_id", hdrs) as sender_id) ->
    (store stream)
```

### kafka_build_headers

```
kafka_build_headers(hName1: string, hVal1: A, hName1: string, hVal1: A, hName1: string, hVal1: A, ...)
A ∈ {string, bytes}
```

Given a list of (header name, header value) pairs construct a raw headers block containing those headers as type bytes, in the
format expected for a Kafka message.

This is used when writing headers from the server to outgoing Kafka messages.

Example:

Exposing an existing stream as a consumable Kafka topic (read only)

* The key of the Kafka message will be the `cust_id` field from stream
* The message will contain a `server_time` header with current server time as value
* The message will contain a `country` header with value of country field from stream
* The message body will be the description field from the stream

```
my-topic := existing stream ->
    (project to_bytes(cust_id),
        kafka_build_headers("server_time", to_string(now()), "country", country),
        to_bytes(description)) ->
    (kafka out)    
```


## Miscellaneous functions

### len

```
len(s: T): int
T ∈ {string, bytes}
```

Returns the number of *bytes* in `s`. Note that, in the case of a string that contains multibyte characters this will not be equal
to the number of characters in the string.

Examples:

```

(filter by len(col8) > 10)

(project if(len(col2) > 100, "big", "small"))

```

### concat

```
concat(s1: T, s2: T): T
T ∈ {string, bytes}
```

Concatenates `s1` with `s2` and returns the result. Works with `string` or `bytes`.

Examples:

```
(sort by concat(country, city))
```

### bytes_slice

```
bytes_slice(b: bytes, start: int, end: int): bytes
```

Returns a bytes value which is a slice of the byte value `b` from start index `start` (inclusive) to end index `end` (exclusive)

Examples:
```
(project bytes_slice(bytes_val, 3, 10))
```

### uint64_be

```
uint64_be(b: bytes): int
```

Decodes the bytes given by `b` as an unsigned 64-bit number in big-endian format.

Example:

If the incoming Kafka message key contains a 64-bit unsigned int in big-endian format, this will extract it as an int.
```
my-topic := (topic partitions = 16) ->
    (project uint64_be(key) as key, val) ->
    (store stream)
```

### uint64_le

```
uint64_le(b: bytes): int
```

Decodes the bytes given by `b` as an unsigned 64-bit number in little-endian format.

Example:

If the incoming Kafka message key contains a 64-bit unsigned int in little-endian format, this will extract it as an int.
```
my-topic := (topic partitions = 16) ->
    (project uint64_le(key) as key, val) ->
    (store stream)
```

### abs

```
abs(v: T): T
T ∈ {int, float, decimal}
```

Returns the absolute value of `v`. That is, if `v` >= 0, then return `v`, else return `-v`

