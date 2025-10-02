# kobolds

**kobolds is currently under heavy development - use at your own risk**

## Description

`kobolds` is a modern, high performing messaging broker for communication between different machines across different networks and machines. This means it's easy to scale, easy to implement, robust and reliable enough to support modern workloads.

## Quickstart

### Prerequisites

- `zig` 0.15.1

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

| Pattern | Description |
| --- | --- |
| Publish/Subscribe | publish a message to `n` subscribers |
| Request/Reply | send a transactional request to an advertised service and receive a reply |
| Worker Queue | create a queue that can be accessed by `n` nodes |
| Key value store | manage key value pairs |

## Roadmap

### Version 0.1.x

| complete | name | priority |
| --- | --- | --- |
| ✔️ | [create kobolds communication protocol](https://github.com/kobolds-io/kobolds/issues/3) | high |
| ✔️ | [create basic message parser](https://github.com/kobolds-io/kobolds/issues/4) | high |
| ✔️ | [create basic node](https://github.com/kobolds-io/kobolds/issues/6) | high |
| ✔️ | [create basic cli](https://github.com/kobolds-io/kobolds/issues/7) | high |
| ✔️ | [create single node publish/subscribe](https://github.com/kobolds-io/kobolds/issues/8) | high |
| ✔️ | [create single node request/reply](https://github.com/kobolds-io/kobolds/issues/9) | high |
| ✔️ | single node service load balancing | low |

### Version 0.2.x

| complete | name                            | priority |
| -------- | ------------------------------- | -------- |
| ❌       | node to node message forwarding | high     |
| ❌       | reserved services and topics    | medium   |
| ❌       | node to node keep alive         | medium   |
| ❌       | client to node keep alive       | medium   |
| ❌       | node info                       | high     |
| ❌       | authentication/authorization    | high     |
| ❌       | memory usage reduction          | low      |

### Version 0.3.x

| complete | name                              | priority |
| -------- | --------------------------------- | -------- |
| ❌       | multi node publish/subscribe      | high     |
| ❌       | multi node request/reply          | high     |
| ❌       | demo application                  | high     |
| ❌       | multi node service load balancing | low      |

### Version 0.4.x

| complete | name                                   | priority |
| -------- | -------------------------------------- | -------- |
| ❌       | worker queues                          | high     |
| ❌       | node to node `tls` encryption          | high     |
| ❌       | public web page                        | medium   |
| ❌       | support for `json` or `cbor` encodings | low      |
| ❌       | connection rate limiting               | medium   |

### Version 0.5.x

| complete | name                             | priority |
| -------- | -------------------------------- | -------- |
| ❌       | QoS implementation               | low      |
| ❌       | `icp` transport support          | low      |
| ❌       | typescript client implementation | medium   |
| ❌       | client sdk implementation        | medium   |
| ❌       | static type enforcement          | medium   |

### Version 1.0.0

| complete | name                      | priority |
| -------- | ------------------------- | -------- |
| ❌       | full protol definition    | high     |
| ❌       | deprecation documentation | high     |
| ❌       | public website            | high     |
| ❌       | company formed            | high     |

## Zig

`zig` is low level like `c`. It also can compile to some very important industrial cpu architectures, namely `x86`, `arm` and `risc-v`. With the rise of mobile robots like AMRs (autonomous mobile robots), drones, and other IoT devices, it is important for the footprint of the software to also be very minimal and use very little power. Using a higher level language like `go` or `nodejs` just starts to not make sense. `zig` is also very simple, so picking it up and contributing to this project would be a breeze compared to `rust` or `c++`.

`zig` is also fairly early in development and many of the core libraries that many other languages implement like async functions do not exist yet. `zig` has excellent interop with `c` which can fill many of the gaps posed by these limitations.

## Benchmarks

```plaintext
--------------------------------------------------------
  Operating System: linux x86_64
  CPU:              12th Gen Intel(R) Core(TM) i7-12700H
  CPU Cores:        14
  Total Memory:     31.021GiB
--------------------------------------------------------

|---------------------|
| Checksum Benchmarks |
|---------------------|
benchmark              runs     total time     time/run (avg ± σ)    (min ... max)                p75        p99        p995
-----------------------------------------------------------------------------------------------------------------------------
xxhash32 checksum 16 b 65535    2.694ms        41ns ± 41ns           (36ns ... 7.051us)           41ns       67ns       68ns
xxhash32 checksum 128  65535    4.335ms        66ns ± 172ns          (55ns ... 39.871us)          67ns       70ns       89ns
xxhash32 checksum mess 65535    125.034ms      1.907us ± 236ns       (1.886us ... 43.665us)       1.903us    1.94us     2.09us
xxhash64 checksum 16 b 65535    2.363ms        36ns ± 33ns           (32ns ... 8.588us)           37ns       38ns       58ns
xxhash64 checksum 128  65535    2.742ms        41ns ± 19ns           (38ns ... 3.832us)           42ns       52ns       52ns
xxhash64 checksum mess 65535    21.54ms        328ns ± 203ns         (301ns ... 34.082us)         310ns      849ns      969ns
crc32 checksum 16 byte 65535    2.372ms        36ns ± 21ns           (32ns ... 5.478us)           37ns       58ns       59ns
crc32 checksum 128 byt 65535    2.352ms        35ns ± 37ns           (32ns ... 6.996us)           36ns       58ns       59ns
crc32 checksum message 65535    2.339ms        35ns ± 12ns           (32ns ... 2.962us)           36ns       58ns       59ns

|----------------------------|
| Checksum Verify Benchmarks |
|----------------------------|
benchmark              runs     total time     time/run (avg ± σ)    (min ... max)                p75        p99        p995
-----------------------------------------------------------------------------------------------------------------------------
xxhash32 verify 16 byt 65535    2.746ms        41ns ± 25ns           (37ns ... 4.202us)           44ns       48ns       67ns
xxhash32 verify 128 by 65535    4.001ms        61ns ± 48ns           (55ns ... 10.485us)          64ns       90ns       94ns
xxhash32 verify messag 65535    125.045ms      1.908us ± 182ns       (1.886us ... 30.272us)       1.903us    1.939us    2.088us
xxhash64 verify 16 byt 65535    2.371ms        36ns ± 27ns           (32ns ... 4.974us)           37ns       38ns       58ns
xxhash64 verify 128 by 65535    2.766ms        42ns ± 20ns           (38ns ... 4.595us)           43ns       50ns       52ns
xxhash64 verify messag 65535    20.491ms       312ns ± 64ns          (303ns ... 5.806us)          310ns      424ns      484ns
crc32 verify 16 bytes  65535    2.395ms        36ns ± 139ns          (32ns ... 35.478us)          36ns       58ns       59ns
crc32 verify 128 bytes 65535    28.15ms        429ns ± 84ns          (422ns ... 13.821us)         429ns      441ns      443ns
crc32 verify message b 65535    1.969s         30.056us ± 1.336us    (26.198us ... 147.521us)     29.879us   33.724us   36.512us

|---------------------|
| Message Benchmarks |
|---------------------|
benchmark              runs     total time     time/run (avg ± σ)    (min ... max)                p75        p99        p995
-----------------------------------------------------------------------------------------------------------------------------
serialize large        65535    88.31ms        1.347us ± 223ns       (1.317us ... 28.101us)       1.329us    1.827us    2.08us
deserialize large      65535    43.791ms       668ns ± 124ns         (641ns ... 14.854us)         652ns      1.02us     1.19us
serialize small        65535    44.023ms       671ns ± 105ns         (660ns ... 20.442us)         669ns      701ns      772ns
deserialize small      65535    13.325ms       203ns ± 50ns          (180ns ... 4.878us)          206ns      237ns      239ns

|--------------------|
| Message Benchmarks |
|--------------------|
benchmark              runs     total time     time/run (avg ± σ)    (min ... max)                p75        p99        p995
-----------------------------------------------------------------------------------------------------------------------------
encode large           65535    51.785ms       790ns ± 133ns         (765ns ... 16.996us)         786ns      970ns      1.067us
decode large           65535    36.532ms       557ns ± 69ns          (543ns ... 5.258us)          556ns      584ns      602ns
encode small           65535    10.346ms       157ns ± 87ns          (153ns ... 20.998us)         157ns      183ns      185ns
decode small           65535    7.913ms        120ns ± 53ns          (116ns ... 10.777us)         120ns      145ns      153ns

|--------------------|
| Parser2 Benchmarks |
|--------------------|
benchmark              runs     total time     time/run (avg ± σ)    (min ... max)                p75        p99        p995
-----------------------------------------------------------------------------------------------------------------------------
parse 14 bytes         1000     496.408us      496ns ± 263ns         (469ns ... 8.258us)          483ns      552ns      579ns
parse 8206 bytes       1000     1.233ms        1.233us ± 124ns       (1.115us ... 4.956us)        1.249us    1.306us    1.338us
parse 16412 bytes      1000     3.164ms        3.164us ± 176ns       (3.003us ... 6.803us)        3.18us     3.31us     3.427us
parse 24618 bytes      1000     4.68ms         4.68us ± 368ns        (4.453us ... 10.94us)        4.648us    5.655us    6.168us
parse 24618 bytes      1000     4.648ms        4.648us ± 985ns       (4.329us ... 35.036us)       4.632us    4.721us    7.261us

|-------------------|
| Parser Benchmarks |
|-------------------|
benchmark              runs     total time     time/run (avg ± σ)    (min ... max)                p75        p99        p995
-----------------------------------------------------------------------------------------------------------------------------
parse 128 bytes        1000     199.038us      199ns ± 56ns          (188ns ... 1.796us)          196ns      248ns      277ns
parse 8284 bytes       1000     1.476s         1.476ms ± 27.149us    (1.436ms ... 1.73ms)         1.473ms    1.659ms    1.7ms
parse 16568 bytes      1000     4.595s         4.595ms ± 59.657us    (4.575ms ... 5.197ms)        4.583ms    4.87ms     4.988ms
parse 24852 bytes      1000     9.356s         9.356ms ± 83.541us    (9.32ms ... 9.994ms)         9.332ms    9.703ms    9.743ms
parse 24852 bytes      1000     9.354s         9.354ms ± 75.958us    (9.32ms ... 9.739ms)         9.331ms    9.636ms    9.65ms
```
