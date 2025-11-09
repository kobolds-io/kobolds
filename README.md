# Overview

Kobolds is a high-performance, multi-paradigm message broker designed for modern applications that need publish/subscribe, request/reply, queueing, streaming, and key/value storage—all in a single, efficient system. Kobolds is built to handle high-throughput, low-latency workloads, with optional persistence, message ordering, and delivery guarantees, making it suitable for systems where speed and reliability are critical.

Kobolds is a from scratch implementation of a message broker with minimal dependencies which allows it to shed many legacy decisions and fully own the security model.

> Warning: Kobolds is under heavy development and should NOT be used in production. If you would like to receive updates for kobolds, click here <-----------TODO add a link to a sign up sheet

## Getting started

This project is built using `zig 0.15.1`. It currently only supports `linux` based systems with `io_uring` support for async io. We will add support for more operating systems like macos and windows at a later time.

1. Clone the repo and enter the directory

   ```bash
   git clone git@github.com:kobolds-io/kobolds.git

   cd kobolds
   ```

2. run the test suite

   ```bash
   zig build test
   ```

3. run the cli

   ```bash
   # zig build run -- <command>
   zig build run -- help
   ```

## Message Patterns Supported

| Pattern                   | Implemented |
| ------------------------- | ----------- |
| Publish/Subscribe         | no          |
| Request/Reply             | no          |
| Queues                    | no          |
| Streams                   | no          |
| Key Value Storage         | no          |
| Direct message forwarding | no          |

## FAQ

1. [What is kobolds?](https://github.com/kobolds-io/kobolds/issues/1)
1. [Kobolds Protocol](https://github.com/kobolds-io/kobolds/issues/2)
