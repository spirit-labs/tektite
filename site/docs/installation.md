# Build and Install

Until we reach version 1.0 you should build Tektite yourself from the source:

```
git clone https://github.com/spirit-labs/tektite.git
cd tektite
go build -o bin ./...
```

This will create binaries in the `bin` directory

Assuming the directory you cloned Tektite into is `$TEKTITE_HOME`, then you can add the binary directory to your path with (on *nix):

`export PATH=$PATH:$TEKTITE_HOME/bin`



