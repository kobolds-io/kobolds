# Harpy

**Harpy is currently under heavy development - use at your own risk**

## Description

`harpy` is a modern, high performing messaging system for communication between different machines across different networks. This means it's easy to scale, easy to apply and robust and reliable enough to support modern workloads.

## Quickstart

### Prerequisites

- `zig` 0.13.0

### Get Started

Build the cli and run it

```bash
# build and fetch all deps
zig build --fetch

# run the cli
./zig-out/bin/harpy
```

Start a cluster locally

```bash
# run cluster with the default configuration
./zig-out/bin/harpy cluster run
```

subscribe to a topic

```bash
# subscribe to any messages published to the `/hello` topic
./zig-out/bin/harpy node sub /hello
```

publish to a topic

```bash
# publish a message the the `/hello` topic
./zig-out/bin/harpy node pub /hello "this is my cool message"
```

## Background

It draws inspiration from many sources namely; `nats`, `mqtt`, `nanomsg` and especially `ros` and brings many of the conveiniences of the modern cloud environment. `harpy` means to offer several convenient features like load balancing, key value store, message/worker queues, multi transport & encoding as well as providing clients in multiple languages.

## Goals

- **reliably** communicate between multiple systems with a single protocol.
- distribute work to multiple machines across multiple network boundaries
- provide a clear model for how to build scalable and data intensive systems

**Core patterns**

| Pattern           | Description                                                               |
| ----------------- | ------------------------------------------------------------------------- |
| Request/Reply     | send a transactional request to an advertised service and receive a reply |
| Publish/Subscribe | publish a message to `n` subscribers                                      |
| Bridge            | send/receive messages between clusters                                    |

**Non Core patterns**

| Pattern | Description                                      |
| ------- | ------------------------------------------------ |
| Queue   | create a queue that can be accessed by `n` nodes |
| Store   | manage key value pairs                           |
| Pair    | send/receive messages to and from specific nodes |

## Roadmap

### Version 0.1.x

| complete | name                                                                                        | priority |
| -------- | ------------------------------------------------------------------------------------------- | -------- |
| ❌       | [create harpy communication protocol](https://github.com/butterworks-io/harpy/issues/3)     | high     |
| ❌       | [create basic message parser](https://github.com/butterworks-io/harpy/issues/4)             | high     |
| ❌       | [create basic cluster](https://github.com/butterworks-io/harpy/issues/5)                    | high     |
| ❌       | [create basic node](https://github.com/butterworks-io/harpy/issues/6)                       | high     |
| ❌       | [create basic cli](https://github.com/butterworks-io/harpy/issues/7)                        | high     |
| ❌       | [create single cluster publish/subscribe](https://github.com/butterworks-io/harpy/issues/8) | high     |
| ❌       | [create single cluster request/reply](https://github.com/butterworks-io/harpy/issues/9)     | high     |

### Version 0.2.x

| complete | name                                  | priority |
| -------- | ------------------------------------- | -------- |
| ❌       | node to cluster keep alive            | medium   |
| ❌       | cluster info                          | high     |
| ❌       | node info                             | high     |
| ❌       | single cluster service load balancing | low      |
| ❌       | authentication/authorization          | high     |

### Version 0.3.x

| complete | name                                 | priority |
| -------- | ------------------------------------ | -------- |
| ❌       | cluster to cluster communication     | high     |
| ❌       | cluster to cluster keep alive        | medium   |
| ❌       | multi cluster publish/subscribe      | high     |
| ❌       | multi cluster request/reply          | high     |
| ❌       | multi cluster service load balancing | low      |

### Version 0.4.x

| complete | name                                | priority |
| -------- | ----------------------------------- | -------- |
| ❌       | queues                              | medium   |
| ❌       | support for `json` encodings        | medium   |
| ❌       | public web page                     | low      |
| ❌       | node to cluster `tls` encryption    | high     |
| ❌       | cluster to cluster `tls` encryption | high     |

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
