# CefasDB

## What is CefasDB?

CefasDB is a NoSQL document database with HTTP/JSON and gRPC
APIs, server-side SQL, and an optional Raft multi-shard mode for
horizontal scale. It targets teams that need predictable
millisecond-class reads on operational data and run the binary
themselves.

For more information, please see [docs.cefasdb.com].

[docs.cefasdb.com]: https://docs.cefasdb.com

## Install

```sh
npm install -g @cefasdb/cefas
cefas --help
```

Node.js 18 or newer is required for the installer wrapper; the
installed command is the native Go CLI.

## Build from source

```sh
go build -o ./bin/cefasdb ./cmd/cefasdb
go build -o ./bin/cefas   ./cmd/cefasctl
```

For developer setup, see the [developer guide].

[developer guide]: https://github.com/CefasDB/cefasdb-docs/blob/main/dev/building.md

## Running

```sh
./bin/cefasdb \
  -data ./cefas-data \
  -http :8080 \
  -grpc :9090 \
  -grpc-reflection
```

In another shell:

```sh
./bin/cefas --endpoint localhost:9090 --insecure list-tables
```

For all run options:

```sh
./bin/cefasdb --help
```

## Testing

```sh
make test
```

See the [testing guide] for the full suite.

[testing guide]: https://github.com/CefasDB/cefasdb-docs/blob/main/dev/testing.md

## APIs

CefasDB exposes HTTP/JSON and gRPC APIs and accepts SQL through
`ExecuteStatement`. See the [API reference] for the full
contract.

[API reference]: https://docs.cefasdb.com/reference/apis

## Documentation

User and operator documentation lives at [docs.cefasdb.com]
and is sourced from [CefasDB/cefasdb-docs].

[CefasDB/cefasdb-docs]: https://github.com/CefasDB/cefasdb-docs

## Contributing

If you want to report a bug, submit a pull request, or propose a
change, please read the [contribution guide]. Developers working
on CefasDB itself should read the [developer guide].

[contribution guide]: https://github.com/CefasDB/cefasdb-docs/blob/main/CONTRIBUTING.md

## Contact

- [Community forum] for users to discuss configuration,
  management, and operations.
- [Developers mailing list] for development topics.

[Community forum]: https://forum.cefasdb.com
[Developers mailing list]: https://groups.google.com/g/cefasdb-dev

## License

See [docs.cefasdb.com/legal] for license terms.

[docs.cefasdb.com/legal]: https://docs.cefasdb.com/legal
