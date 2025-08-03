# kobolds

**kobolds is currently under heavy development - use at your own risk**

## Description

`kobolds` is a modern, high performing messaging broker for communication between different machines across different networks and machines. This means it's easy to scale, easy to implement, robust and reliable enough to support modern workloads.

## Quickstart

### Prerequisites

- `zig` 0.14.0

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
  CPU:              13th Gen Intel(R) Core(TM) i9-13900K
  CPU Cores:        24
  Total Memory:     23.298GiB
--------------------------------------------------------

|---------------------|
| checksum Benchmarks |
|---------------------|
benchmark              runs     total time     time/run (avg ± σ)     (min ... max)                p75        p99        p995
-----------------------------------------------------------------------------------------------------------------------------
xxhash32 checksum 16 b 65535    1.691ms        25ns ± 1.031us         (18ns ... 260.378us)         21ns       22ns       23ns
xxhash32 checksum 128  65535    2.53ms         38ns ± 272ns           (31ns ... 65.559us)          37ns       38ns       38ns
xxhash32 checksum mess 65535    83.509ms       1.274us ± 387ns        (1.245us ... 36.937us)       1.258us    1.284us    1.808us
xxhash64 checksum 16 b 65535    803.08us       12ns ± 30ns            (11ns ... 6.67us)            12ns       13ns       13ns
xxhash64 checksum 128  65535    1.093ms        16ns ± 29ns            (15ns ... 6.283us)           17ns       18ns       18ns
xxhash64 checksum mess 65535    8.127ms        124ns ± 94ns           (120ns ... 10.284us)         123ns      125ns      135ns
crc32 checksum 16 byte 65535    806.179us      12ns ± 23ns            (11ns ... 5.535us)           12ns       13ns       13ns
crc32 checksum 128 byt 65535    803.89us       12ns ± 29ns            (11ns ... 7.206us)           12ns       13ns       13ns
crc32 checksum message 65535    793.341us      12ns ± 5ns             (11ns ... 786ns)             12ns       13ns       13ns

|-------------------|
| verify Benchmarks |
|-------------------|
benchmark              runs     total time     time/run (avg ± σ)     (min ... max)                p75        p99        p995
-----------------------------------------------------------------------------------------------------------------------------
xxhash32 verify 16 byt 65535    1.352ms        20ns ± 53ns            (18ns ... 13.356us)          21ns       21ns       22ns
xxhash32 verify 128 by 65535    2.509ms        38ns ± 98ns            (35ns ... 11.193us)          37ns       38ns       38ns
xxhash32 verify messag 65535    83.814ms       1.278us ± 498ns        (1.244us ... 37.41us)        1.258us    1.266us    1.33us
xxhash64 verify 16 byt 65535    800.444us      12ns ± 29ns            (11ns ... 6.068us)           12ns       13ns       13ns
xxhash64 verify 128 by 65535    1.1ms          16ns ± 50ns            (15ns ... 10.43us)           17ns       18ns       18ns
xxhash64 verify messag 65535    8.349ms        127ns ± 227ns          (120ns ... 36.744us)         123ns      168ns      169ns
crc32 verify 16 bytes  65535    806.846us      12ns ± 45ns            (11ns ... 10.216us)          12ns       13ns       13ns
crc32 verify 128 bytes 65535    11.6ms         177ns ± 143ns          (171ns ... 12.236us)         174ns      176ns      176ns
crc32 verify message b 65535    798.7ms        12.187us ± 7.169us     (11.899us ... 1.663ms)       11.934us   19.019us   22.093us

|--------------------|
| Message Benchmarks |
|--------------------|
benchmark              runs     total time     time/run (avg ± σ)     (min ... max)                p75        p99        p995
-----------------------------------------------------------------------------------------------------------------------------
encode                 65535    15.452ms       235ns ± 176ns          (226ns ... 17.01us)          230ns      317ns      330ns
decode                 65535    13.889ms       211ns ± 252ns          (202ns ... 33.846us)         205ns      281ns      284ns
compress gzip          65535    1.583s         24.169us ± 36.025us    (22.978us ... 8.999ms)       23.682us   33.11us    37.737us
decompress gzip        65535    1.757s         26.81us ± 11.572us     (25.909us ... 2.7ms)         26.3us     37.419us   42.609us

|-------------------|
| Parser Benchmarks |
|-------------------|
benchmark              runs     total time     time/run (avg ± σ)     (min ... max)                p75        p99        p995
-----------------------------------------------------------------------------------------------------------------------------
parse 128 bytes        65535    3.513ms        53ns ± 49ns            (49ns ... 10.225us)          55ns       65ns       67ns
parse 8320 bytes       65535    21.597ms       329ns ± 264ns          (318ns ... 30.684us)         323ns      356ns      440ns
parse 16640 bytes      65535    63.169ms       963ns ± 464ns          (880ns ... 69.977us)         966ns      1.035us    1.272us
parse 24960 bytes      65535    95.644ms       1.459us ± 435ns        (1.384us ... 35.798us)       1.444us    1.577us    2.141us
parse 249600 bytes     65535    3.535s         53.944us ± 22.645us    (51.889us ... 4.56ms)        52.804us   73.087us   82.991us
```
