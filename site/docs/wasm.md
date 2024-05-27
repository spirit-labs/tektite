# Implementing custom functions using WebAssembly

You can implement custom functions using [WebAssembly (WASM)](https://webassembly.org/) and use them in any expression, just like built-in
functions.

WebAssembly is a binary execution format originally designed for executing high performance code in browsers 
but is now emerging as a popular way for components written in different languages to interoperate.

You write your function in your chosen language and compile to a WASM module. You then create a JSON descriptor file
which describes the functions that you want to expose, and then register the module with Tektite. Then you can use your
function(s) in Tektite just like any built-in function.

Let's walk through an example.

## Write and compile the module

> Note: WASM only supports a very limited set of types for function arguments and return values - basically just ints and floats. There
> is no direct support for more 'complex' types such as strings and byte arrays. The usual way around this is write the
> arguments / return values into WASM linear memory (which is accessible by both the module and the host) and encode
> a 'pointer' to that memory as an int which is passed to/from the function. The way this is done in practice is not standardised
> and depends on the particular programming language. Until a standard way to pass 'complex' types is provided in WASM
> (perhaps the Component Model will solve this) it's the best we can do.

### Create a module in golang

For writing golang WASM modules, we recommend using [TinyGo](https://tinygo.org/). This has the advantage of built-in support for compilation to WASM
and it creates small binaries. Please follow the instructions on the TinyGo website for [installing it](https://tinygo.org/getting-started/install/).

Let's compile an [example golang WASM module](https://github.com/spirit-labs/tektite/examples/wasm) from the repository. You will need
both the files: [`my_mod.go`](https://github.com/spirit-labs/tektite/examples/wasm/my_mod.go) and [`utils.go`](https://github.com/spirit-labs/tektite/examples/wasm/utils.go)

The former contains our WASM function, the latter contains some boilerplate utility functions used for converting WASM arguments and
return values to/from complex types such as `string` and `[]byte`.

Our WASM function is called `repeatString` - it takes a `string` parameter called `str` and an `int` parameter called `times`
and returns the string, repeated `times` times.

To compile it, from this directory execute:

```
tinygo build -o my_mod.wasm -scheduler=none -target=wasi ./
```

This will compile it to a WASM binary called `my_mod.wasm`

## Create the JSON descriptor

Next, we need to create a JSON descriptor file for the module. This tells Tektite which functions from the module you 
want to import into Tektite, and what the Tektite data types are for their parameters and return values.

> This is necessary
because Tektite cannot infer the Tektite function signature from the WASM function signature because a WASM function with a
particular signature could map to many different function signatures in Tektite. E.g. a WASM function that takes an i64 argument - this could
represent an integer, or it could be a pointer to a `string` or a pointer to a `[]byte`, etc.

The JSON descriptor for our module looks like this:

```json
{
    "name": "my_mod",
    "functions": {
        "repeatString": {
            "paramTypes": ["string", "int"],
            "returnType": "string"
        }
    }
}
```

The name of the module is specified, and each function is listed along with the Tektite types of its parameters and 
return value.

The JSON descriptor should be saved into a file called `my_mod.json` - i.e. the name of the module, with the `.json` file 
extension.

## Register the module

To upload the module using the Tektite CLI, you use the `register_wasm` command. This takes a path to the `.wasm` file.
It can be absolute or relative to the current working directory where the CLI was run from.

In the directory there must be the `.wasm` file and the corresponding `.json` descriptor file.

```
tektite> register_wasm("examples/wasm/my_mod.wasm");
```

You can also [register WASM modules using the HTTP API](http_api.md#registering--unregistering-wasm-modules) and using the
golang client.

## Create a stream that uses the module

Now that we've registered our module, our function `repeatString` should now be available. Let's try it.

At the CLI, we'll create a Kafka topic that takes the body of the Kafka message and applies the `repeatString` function
to it to "multiply" the message body by 3 before exposing the message for consumption.

```
tektite> repeat-topic :=
    (kafka in partitions = 16) ->
    (project key, hdrs, to_bytes(my_mod.repeatString(to_string(val), 3))) ->
    (kafka out);
```

Note that when we use our `repeatString` function it is scoped by the name of our WASM module `my_mod`, giving `my_mod.repeatString`. 
All custom functions must be called this way.

Now, using [kcat](https://github.com/edenhill/kcat) we can open two consoles to consume and produce a message to this topic:

In one console, start a consumer:

```
kcat -q -b 127.0.0.1:8880 -G mygroup repeat-topic
```

In another console, send a messsage:

```
echo "hello how are you?" | kcat -b 127.0.0.1:8880 -t repeat-topic -P
```

In the consumer console you should see:

```
hello how are you?hello how are you?hello how are you?
```

## Unregister a module

To unregister a module you use the `unregister_wasm` command. This takes the *module name* (not a path or file name) to
unregister, in this case `my_mod`.

> Before unregistering a module make sure you have deleted any streams that use the module, or they will fail when
> processing data.

```
tektite> unregister_wasm("my_mod");
```
