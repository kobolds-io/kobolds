# kobolds

**kobolds is currently under heavy development - use at your own risk**

## Description

`kobolds` is a modern, high performing messaging broker for communication between different machines across different networks and machines. This means it's easy to scale, easy to implement, robust and reliable enough to support modern workloads.

## Quickstart

### Prerequisites

- `zig` 0.13.0

### Get Started

Build the cli and run it

```bash
# build and fetch all deps
zig build --fetch

# run the cli
./zig-out/bin/kobolds
```

Start a node locally

```bash
# run node with the default configuration
./zig-out/bin/kobolds node listen
```

benchmark the node

```bash
./zig-out/bin/kobolds node bench
```

## Background

It draws inspiration from many sources namely; `nats`, `mqtt`, `nanomsg` and especially `ros` and brings many of the conveiniences of the modern cloud environment. `kobolds` means to offer several convenient features like load balancing, key value store, message/worker queues, multi transport & encoding as well as providing clients in multiple languages.

## Goals

- **reliably** communicate between multiple systems with a single protocol.
- distribute work to multiple machines across multiple network boundaries
- provide a clear model for how to build scalable and data intensive systems

**Core patterns**

| Pattern           | Description                                                               |
| ----------------- | ------------------------------------------------------------------------- |
| Request/Reply     | send a transactional request to an advertised service and receive a reply |
| Publish/Subscribe | publish a message to `n` subscribers                                      |
| Bridge            | send/receive messages between nodes                                       |

**Non Core patterns**

| Pattern | Description                                      |
| ------- | ------------------------------------------------ |
| Queue   | create a queue that can be accessed by `n` nodes |
| Store   | manage key value pairs                           |
| Pair    | send/receive messages to and from specific nodes |

## Roadmap

### Version 0.1.x

| complete | name                                                                                    | priority |
| -------- | --------------------------------------------------------------------------------------- | -------- |
| ❌       | [create kobolds communication protocol](https://github.com/kobolds-io/kobolds/issues/3) | high     |
| ✔️       | [create basic message parser](https://github.com/kobolds-io/kobolds/issues/4)           | high     |
| ❌       | [create basic node](https://github.com/kobolds-io/kobolds/issues/5)                     | high     |
| ❌       | [create basic node](https://github.com/kobolds-io/kobolds/issues/6)                     | high     |
| ❌       | [create basic cli](https://github.com/kobolds-io/kobolds/issues/7)                      | high     |
| ❌       | [create single node publish/subscribe](https://github.com/kobolds-io/kobolds/issues/8)  | high     |
| ❌       | [create single node request/reply](https://github.com/kobolds-io/kobolds/issues/9)      | high     |

### Version 0.2.x

| complete | name                               | priority |
| -------- | ---------------------------------- | -------- |
| ❌       | node to node keep alive            | medium   |
| ❌       | node info                          | high     |
| ❌       | node info                          | high     |
| ❌       | single node service load balancing | low      |
| ❌       | authentication/authorization       | high     |

### Version 0.3.x

| complete | name                              | priority |
| -------- | --------------------------------- | -------- |
| ❌       | node to node communication        | high     |
| ❌       | node to node keep alive           | medium   |
| ❌       | multi node publish/subscribe      | high     |
| ❌       | multi node request/reply          | high     |
| ❌       | multi node service load balancing | low      |

### Version 0.4.x

| complete | name                          | priority |
| -------- | ----------------------------- | -------- |
| ❌       | queues                        | medium   |
| ❌       | support for `json` encodings  | medium   |
| ❌       | public web page               | low      |
| ❌       | node to node `tls` encryption | high     |
| ❌       | node to node `tls` encryption | high     |

### Version 0.5.x

| complete | name                           | priority |
| -------- | ------------------------------ | -------- |
| ❌       | QoS implementation             | low      |
| ❌       | `icp` transport support        | low      |
| ❌       | typescript node implementation | medium   |
| ❌       | static type enforcement        | medium   |

### Version 0.6.x

| complete | name             | priority |
| -------- | ---------------- | -------- |
| ❌       | demo application | high     |

### Version 1.0.0

| complete | name                      | priority |
| -------- | ------------------------- | -------- |
| ❌       | full protol definition    | high     |
| ❌       | deprecation documentation | high     |
| ❌       | website                   | high     |

## Zig

`zig` is low level like `c`. It also can compile to some very important industrial cpu architectures, namely `x86`, `arm` and `risc-v`. With the rise of mobile robots like AMRs (autonomous mobile robots), drones, and other IoT devices, it is important for the footprint of the software to also be very minimal and use very little power. Using a higher level language like `go` or `nodejs` just starts to not make sense. `zig` is also very simple, so picking it up and contributing to this project would be a breeze compared to `rust` or `c++`.

`zig` is also fairly early in development and many of the core libraries that many other languages implement like async functions do not exist yet. `zig` has excellent interop with `c` which can fill many of the gaps posed by these limitations.
