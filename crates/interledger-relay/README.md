## Setup
### Prerequisites

Install rustup:

    $ sudo pacman -S rustup

Install a Rust toolchain (check the `Dockerfile` for the exact version used):

    $ rustup install stable
    $ rustup default stable

Get the code:

    $ git clone git@github.com:coilhq/interledger-relay.git
    $ cd interledger-relay/

### Building

Run the tests to make sure everything is working:

    $ cargo test

If they all pass, build the release executable:

    $ cargo build --release

By default, the executable is at `target/release/ilprelay`.

## Configuration
### Next Hop
#### Static

Configure the connector with a static ILP address.

##### Example

```json
"root": {
  "type": "Static",
  "address": "example.connector.address",
  "asset_scale": 9,
  "asset_code": "XRP"
},
```

#### Dynamic

On startup, the connector will query its ILP address from the specified parent connector via ILDCP. If the ILDCP request fails, the connector will exit.

##### Example

```json
"root": {
  "type": "Dynamic",
  "parent_endpoint": "http://example.com/ilp",
  "parent_auth": "SECRET",
  "name": "my_connector_name"
},
```

### Partition Field

- `"Destination"` (default): When partitioning by `Destination`, packets of a STREAM connection follow a single route (unless that route is marked as unavailable).
- `"ExecutionCondition"`: When partitioning by `ExecutionCondition`, packets of a STREAM connection are split over multiple routes. This is probably only useful for testing.

##### Example

```json
"routing_partition": "Destination",
```

### Route Configuration
#### Partitioning

Partitioning divides traffic to a single target prefix over multiple sub-routes. Prepare packets are sent to a pseudorandom (but deterministic) sub-route -- see "Partition Field".

If it is not explicitly set, the `partition` is set to `1.0` for every sub-route. When there are multiple sub-routes for a single target prefix, explicit `partition`s should be used to avoid confusion.

The `partition`s of all routes _within a shared prefix_ are summed to compute the `total_partitions`. Each sub-route receives `partition / total_partitions * 100` percent of outgoing ILP Prepare packets.

##### Example

```json
"test.prefix.": [
  { // Route A
    "next_hop": { … },
    "partition": 0.25
  },
  { // Route B
    "next_hop": { … },
    "partition": 0.75
  }
],
```

In this example, `total_partitions = 0.25 + 0.75 = 1.0`. Route A receives `0.25 / 1.0 * 100 = 25%` of the traffic, and Route B receives 75%.

#### Failover

When `failover` is configured on a sub-route, the connector will track "failures". If a sub-route fails frequently enough to meet the configured threshold, it is temporarily marked unavailable.

A _failure_ in this context is either:
- An HTTP connection error.
- A HTTP `5xx` status code.

While a sub-route is _unavailable_:
- Prepare packets are sent on alternate sub-routes. During this time, the sub-route is excluded from the `total_partitions` calculation. A sub-route becomes available again once its `fail_duration` has expired.
- The connector will continue to respond to incoming ILP Prepares from an "unavailable" remote.

Fields (all required):
- `window_size`: positive integer.
- `fail_ratio`: float, between `0.0` and `1.0` (inclusive).
- `fail_duration`
  - `secs`: positive integer
  - `nanos`: positive integer

A route is marked as unavailable for `fail_duration` when `fail_ratio <= number of failures per window / window_size`.

##### Example

```json
"test.prefix.": [
  { // Route A
    "next_hop": { … },
    "failover": {
      "window_size": 50,
      "fail_ratio": 0.2,
      "fail_duration": { "secs": 30, "nanos": 0 }
    },
    "partition": 0.25
  },
  { // Route B
    "next_hop": { … },
    "failover": {
      "window_size": 50,
      "fail_ratio": 0.2,
      "fail_duration": { "secs": 30, "nanos": 0 }
    },
    "partition": 0.75
  }
],
```

## Example

```
RUST_LOG='info' \
RELAY_BIND='127.0.0.1:3001' \
RELAY_CONFIG='{
  "root": {
    "type": "Static",
    "address": "private.moneyd",
    "asset_scale": 9,
    "asset_code": "XRP"
  },
  "relatives": [
    {
      "type": "Child",
      "account": "child_1",
      "auth": ["child_1_secret"],
      "suffix": "child1"
    },
    {
      "type": "Parent",
      "account": "parent",
      "auth": ["parent_secret"]
    }
  ],
  "routes": [
    "private.moneyd.child1.": [{
      "next_hop": {
        "type": "Bilateral",
        "endpoint": "http://127.0.0.1:3000",
        "auth": "secret_bilateral"
      }
    }],
    "private.moneyd.": [{
      "next_hop": {
        "type": "Multilateral",
        "endpoint_prefix": "http://127.0.0.1:",
        "endpoint_suffix": "",
        "auth": "secret_multilateral"
      }
    }]
  ],
  "debug_service": {
    "log_prepare": false,
    "log_fulfill": false,
    "log_reject": true
  }
}' ilprelay
```
