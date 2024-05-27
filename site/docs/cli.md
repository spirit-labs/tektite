# The command line interpreter (CLI)

> This guide assumes you have installed Tektite and the Tektite binaries directory is on the `PATH` environment variable

You interact with Tektite at the command line using the `tektite` command.

To see the options, use `tektite --help`:

```
> tektite --help
Usage: tektite

Flags:
  -h, --help                         Show context-sensitive help.
      --address="127.0.0.1:7770"     Address of tektite server to connect to.
      --trusted-certs-path=STRING    Path to a PEM encoded file containing certificate(s) of trusted servers and/or certificate authorities
      --key-path=STRING              Path to a PEM encoded file containing the client private key. Required with TLS client authentication
      --cert-path=STRING             Path to a PEM encoded file containing the client certificate. Required with TLS client authentication
      --no-verify                    Set to true to disable server certificate verification. WARNING use only for testing, setting this can expose you to
                                     man-in-the-middle attacks
      --command=STRING               Single command to execute, non interactively
```

The CLI can be run both interactively (as a shell) and non interactively, for executing single commands

## Connecting

You specify the address of the server to connect to with the `--address` option. This defaults to `127.0.0.1:7770` which is an
address a local development server using the `standalone.cfg` would be listening at.

The CLI uses the Tektite HTTP API, so the address to connect to corresponds to the address the HTTP API listens at. That is
configured on the server with the `http-api-addresses` configuration property.

### Connecting to server using self-signed certificates

If your Tektite HTTP API server is using self-signed certificates then will need to specify a path to a PEM file containing
the trusted certificates or certificate authorities on the command line:

```
tektite --address tektite.foo.com:7770 --trusted-certs-path path/to/mycerts.pem
```

### Connecting using client certificates

If you've configured your server to expect client certificates when connecting, you must provide you must paths to PEM
files containing the client private key and client certificate:

```
tektite --address tektite.foo.com:7770 --key-path /path/to/myclientkey.pem --cert-path /path/to/myclientcert.pem
```

### Using `no-verify`

When working with a local Tektite server for development and demos you can use the `--no-verify` option which prevents
the client from verifying whether server certificates are trusted.

## Executing single commands

You can run single commands with `tektite` using the `command` option:

For example, to create a topic:
```
tektite --command "my-topic := (topic partitions = 16)"
OK
```

And create another one:
```
tektite --command "other-topic := (topic partitions = 16)"
OK
```

List all streams:

```
tektite --command "list()"
+----------------------------------------------------------------------------------------------------------------------+
| stream_name                                                                                                          |
+----------------------------------------------------------------------------------------------------------------------+
| my-topic                                                                                                             |
| other-topic                                                                                                          |
+----------------------------------------------------------------------------------------------------------------------+
2 rows returned
```

Execute a query:
```
tektite --command "(scan all from my-topic)"
+---------------------------------------------------------------------------------------------------------------------+
| offset               | event_time                 | key                 | hdrs                | val                 |
+---------------------------------------------------------------------------------------------------------------------+
0 rows returned
```

## Using `tektite` interactively

If the `command` option is omitted, `tektite` will run interactively, as a shell, allowing you to input multiple commands:

You can enter commands over multiple lines, and terminate them with semicolon ';' followed by newline.

```
tektite --address tektite.foo.com:7770
tektite> list();
+----------------------------------------------------------------------------------------------------------------------+
| stream_name                                                                                                          |
+----------------------------------------------------------------------------------------------------------------------+
| my-topic                                                                                                             |
| other-topic                                                                                                          |
+----------------------------------------------------------------------------------------------------------------------+
2 rows returned
tektite> foo-stream :=
         my-topic ->
             (project key, to_upper(to_string(val))) ->
             (store stream);
OK
tektite>             
```

Let's take a look at the supported commands

## Creating topics and streams

You can create streams using the [standard syntax](conceptual_model.md):

```
tektite> my-topic := (topic partitions = 32);
OK
tektite> my-other-topic := (topic partitions = 16);
OK
tektite> filtered-stream := bar-topic -> (filter by len(val) > 1000) -> (store stream);
OK
```

## Listing streams

Use `list()` to list all streams:

```
tektite> list();
+----------------------------------------------------------------------------------------------------------------------+
| stream_name                                                                                                          |
+----------------------------------------------------------------------------------------------------------------------+
| filtered-stream                                                                                                      |
| my-other-topic                                                                                                       |
| my-topic                                                                                                             |
+----------------------------------------------------------------------------------------------------------------------+
3 rows returned
```

Use `list` with a regular expression to list all streams whose name matches the regular expression:

E.g. list all streams whose names start with `my`
```
tektite> list("^my");
+----------------------------------------------------------------------------------------------------------------------+
| stream_name                                                                                                          |
+----------------------------------------------------------------------------------------------------------------------+
| my-other-topic                                                                                                       |
| my-topic                                                                                                             |
+----------------------------------------------------------------------------------------------------------------------+
2 rows returned
```

## Showing a stream

Use `show(<stream_name>)` to show details of a stream:

```
tektite> show(my-topic);

stream_name:   my-topic
stream_def:    (topic partitions = 32)
in_schema:     {record_batch: bytes} partitions: 32 mapping_id: _default_
out_schema:    {offset: int, event_time: timestamp, key: bytes, hdrs: bytes, val: bytes} partitions: 32 mapping_id: _default_
child_streams: filtered-stream
```

The information shows:

* `stream_name`: This is the name of the stream
* `stream_def`: This is the TSL command used to define the stream
* `in_schema`: This is the schema that the stream receives. Number of partitions and mapping_id is also shown.
* `out_schema`: This is the schema that the stream outputs. Number of partitions and mapping_id is also shown.
* `child_streams`: the names of any child streams.

## Executing ad-hoc queries

You can execute ad-hoc [queries](queries.md) at the command line.

```
tektite> (scan all from my-topic);
+---------------------------------------------------------------------------------------------------------------------+
| offset               | event_time                 | key                 | hdrs                | val                 |
+---------------------------------------------------------------------------------------------------------------------+
| 0                    | 2024-05-18 06:33:59.168000 | apples              | .                   | quwhdqiuwhdiquwhd.. |
| 0                    | 2024-05-18 06:34:02.451000 | oranges             | .                   | wueiuqwediuqwduiqwd |
| 0                    | 2024-05-18 06:34:06.450000 | pears               | .                   | uiwqbuqwdiuqwdqwd   |
+---------------------------------------------------------------------------------------------------------------------+
3 rows returned
```

## Deleting streams

Streams are deleted with the `delete` command:

```
tektite> delete(my-other-topic);
OK
```

## Exiting the CLI

CTRL-C to exit

## Setting `max-line-width`

In interactive mode the CLI will try to format all results with a max width which has a default of 120 characters. You can
change this, e.g. if you have a lot of columns to display with `set max-line-width`

```
tektite> set max_line_width 200;
OK
```

## Registering and unregistering WebAssembly modules

You can use the CLI to register and unregister WebAssembly modules.

See the [chapter on WebAssembly](wasm.md) for more information.

```
tektite> register_wasm("path/to/my/modules/my_module.wasm");
OK
tektite> unregister_wasm("my_module");
OK
```