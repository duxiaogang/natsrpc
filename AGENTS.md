# Repository Guidelines

## Project Structure & Module Organization
The root package `natsrpc` contains the runtime library: client, server, transport, service registration, options, and protocol helpers (`client.go`, `server.go`, `service.go`, `header.go`, `reply.go`). Code generation for the custom `protoc` plugin lives in `cmd/protoc-gen-natsrpc/`. Encoder integrations are under `encode/`, with `encode/gogopb/` as the current implementation. Protobuf definitions and generated files live at the repo root (`natsrpc.proto`, `natsrpc.pb.go`) and in `example/`. Vendored proto dependencies are kept in `third_party/google/protobuf/`.

## Build, Test, and Development Commands
Use the Makefile targets at the repo root:

- `make install`: install `protoc-gen-natsrpc` into your Go bin path.
- `make types`: regenerate `natsrpc.pb.go` from `natsrpc.proto`.
- `make test`: run `go test ./...`.
- `make all`: install tools, regenerate types, and run tests.

Examples have their own workflow:

- `make -C example pb`: regenerate example protobuf and natsrpc stubs.
- `make -C example run`: run all example programs.
- `go run ./example/tool/request_bench -url=nats://127.0.0.1:4222`: run the request benchmark.

Run a local `nats-server` before executing examples or benchmarks.

## Coding Style & Naming Conventions
Follow standard Go formatting with `gofmt` and idiomatic import grouping. Use exported `PascalCase` names for public APIs and unexported `camelCase` names internally. Keep option helpers in the established `WithXxx` style, for example `WithCallHeader`. Generated files such as `*.pb.go` and `*_natsrpc.pb.go` should be regenerated, not edited by hand.

## Testing Guidelines
Tests use Go’s built-in `testing` package and live beside the code they cover, as in `util_test.go`. Name tests `TestXxx` and benchmarks `BenchmarkXxx`; prefer table-driven tests for helpers and protocol edge cases. There is no visible coverage gate, so add focused tests for any change to subject building, headers, encoding, or code generation behavior, then run `go test ./...`.

## Commit & Pull Request Guidelines
Recent history favors short, imperative subjects, sometimes with prefixes such as `feat:`, `fix:`, `refactor:`, `doc:`, `chore:`, or `update:`. Keep commits narrowly scoped. If you change `.proto` files or generator templates, include regenerated Go output in the same change. Pull requests should describe behavior changes, list validation commands you ran, and link the relevant issue or use case.
