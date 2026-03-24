# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Install the protoc-gen-natsrpc plugin
make install

# Regenerate protocol buffer definitions (natsrpc.pb.go)
make types

# Run all tests
make test
go test ./...

# Run benchmarks
go test -bench=. ./...
```

## Architecture Overview

NATSRPC is an RPC framework bridging NATS messaging and gRPC interface definitions. It allows gRPC-style service definitions while using NATS publish-subscribe for transport.

### Core Components

- **Server** (`server.go`): Hosts RPC services, manages subscriptions, handles concurrent message processing with optional goroutine pooling via `ants`
- **Client** (`client.go`): Makes RPC calls, supports request-response and publish patterns
- **Service** (`service.go`): Wrapper mapping method names to handlers

### Communication Flow

```
Client.Invoke() → Encode request → NATS message with:
  - Subject: namespace.service.id[._nr_pub]
  - Header: method name + custom headers
  - Data: encoded payload
→ Server subscription → Decode → Middleware chain → Handler → Reply
```

### Message Format

NATS header fields:
- `_ns_method`: RPC method name
- `_ns_user`: Custom user headers
- `_ns_error`: Error message in reply

Subject naming: `joinSubject("namespace", "service", "id")` → `namespace.service.id` (empty parts skipped)

### Code Generation

`cmd/protoc-gen-natsrpc/` generates from proto files:
- Client interface and implementation
- Server interface
- Handler wrapper with auto-marshaling
- Registration function
- Service descriptor

Proto extension `(natsrpc.publish) = true` marks publish-only methods.

### Key Patterns

- **Middleware**: Uses Kratos v2 middleware chain at server, service, and client levels
- **Encoders**: Pluggable via `Encoder` interface; default is `gogopb` (GoGo protobuf)
- **Delayed Reply**: Return `ErrReplyLater` from handler, use `MakeReplyFunc()` to reply later
- **Load Balancing**: NATS queue subscriptions automatically distribute load across service instances
- **Goroutine Control**: `poolSize` option for bounded concurrency; `multiGoroutine` per-service toggle

### Options Hierarchy

- **ServerOptions**: namespace, errorHandler, recoverHandler, encoder, middleware, poolSize
- **ServiceOptions**: id, timeout (default 5s), middleware, multiGoroutine
- **ClientOptions**: namespace, encoder, middleware
- **CallOptions**: header, id (target specific instance)

## Examples

The `example/` directory contains 12 progressive examples demonstrating each feature:
0. Basic demo
1. Request-response
2. Publish pattern
3. Headers
4. Namespace
5. Service ID
6. Error handling
7. Recovery
8. Middleware
9. Load balancing
10. Delayed reply
11. Custom encoders
12. Single-goroutine mode
