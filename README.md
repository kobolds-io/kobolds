# Harpy

**Harpy is currently under development - use at your own risk**

## Description

The world of robotics and industrial communication is wrought with danger. If a system does something it wasn't supposed to, people and machinery can face dire consequences. Harpy means to provide a framework of communication to reliably transfer data from one machine to another over multiple transports.

Harpy is meant as a modern replacement for `ROS1` and an alternative to `ROS2`. It draws inspiration from many sources namely; `nats`, `mqtt`, `nanomsg` and `ros` and brings many of the conveiniences of the modern cloud environment. Harpy means to offer several convenient features like load balancing, key value store, message/worker queues, multi transport & encoding as well as providing clients in multiple languages.

## Why Zig?

`zig` is low level like `c`. It also can compile to some very important industrial cpu architectures, namely `x86`, `arm` and `risc-v`. With the rise of mobile robots like AMRs (autonomous mobile robots), drones, and other mobile robots, it is important for the footprint of the software to also be very minimal and use very little power. Using a higher level language like `go` or `nodejs` just starts to not make sense. `zig` is also very simple, so picking it up and contributing to this project would be a breeze vs trying to do it in `rust` or `c++`.

In short, `zig` is my current favorite alternative to writing this in `c`, `rust` or `go`.

## Goals

- 1. I can reliably communicate between multiple systems with a single protocol.
- 2. I can distribute work to multiple machines across multiple network boundaries

**Core patterns**

| Pattern           | Description                                                                    |
| ----------------- | ------------------------------------------------------------------------------ |
| Request/Reply     | a client sends a transaction to be acknowledged and returned by another client |
| Publish/Subscribe | `n` message producers sending messages to `x` consumers without a buffer       |

**Non Core patterns**

| Pattern       | Description                                                                         |
| ------------- | ----------------------------------------------------------------------------------- |
| Message Queue | `n` producers enqueue messages and `x` consumers dequeue messages at their own pace |
| Data Store    | ability to CRUD a key/value from multiple clients                                   |
| Stream        | send a stream of bytes over a persistent connection                                 |

# Description

This issue is to describe large features to be implemented throughout the life of the project. If you'd like to propose a new item to be added to the roadmap, create an issue and tag a core maintainer.

## Version 0.1.x

| complete | name                          | priority |
| -------- | ----------------------------- | -------- |
| ❌       | basic zig cli                 | high     |
| ❌       | basic zig client              | high     |
| ❌       | basic zig node                | high     |
| ❌       | Kobold Message Protocol       | high     |
| ❌       | single node Publish/Subscribe | high     |
| ❌       | single node Request/Reply     | high     |
| ❌       | service topic load balancing  | high     |
| ❌       | Public Web Page               | medium   |

## Version 0.2.x

| complete | name                         | priority |
| -------- | ---------------------------- | -------- |
| ❌       | client info                  | high     |
| ❌       | node info                    | high     |
| ❌       | node to node communication   | high     |
| ❌       | multi node Publish/Subscribe | high     |
| ❌       | multi node Request/Reply     | high     |
| ❌       | implementation examples      | medium   |
| ❌       | performance optimization     | medium   |
| ❌       | authentication/authorization | high     |
| ❌       | `tls` encryption             | high     |
| ❌       | end to end encryption        | high     |

## Version 0.3.x

| complete | name                                 | priority |
| -------- | ------------------------------------ | -------- |
| ❌       | single node message queues           | medium   |
| ❌       | multi node message queues            | medium   |
| ❌       | official communication specification | high     |
| ❌       | message record                       | low      |
| ❌       | multi transport support              | low      |
| ❌       | multi language clients               | medium   |
| ❌       | message routing optimization         | high     |
| ❌       | static type enforcement              | medium   |

## Version 1.0.0

TBD
