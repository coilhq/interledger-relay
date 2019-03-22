## Setup
### Prerequisites

Install rustup:

    $ sudo pacman -S rustup

Install a Rust toolchain:

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

The executable can be found at `target/release/ilprelay`.

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
  "peers": [
    {
      "type": "Child",
      "auth": ["child_1_secret"],
      "suffix": "child1"
    },
    {
      "type": "Parent",
      "auth": ["parent_secret"]
    }
  ],
  "routes": [
    {
      "target_prefix": "private.moneyd.child1.",
      "next_hop": {
        "type": "Bilateral",
        "endpoint": "http://127.0.0.1:3000",
        "auth": "secret_bilateral"
      }
    },
    {
      "target_prefix": "private.moneyd.",
      "next_hop": {
        "type": "Multilateral",
        "endpoint_prefix": "http://127.0.0.1:",
        "endpoint_suffix": "",
        "auth": "secret_multilateral"
      }
    }
  ]
}' ilprelay
```

## Config

### Next Hop
#### Static

    {
      "type": "Static",
      "address": "example.connector.address",
      "asset_scale": 9,
      "asset_code": "XRP"
    }

#### Dynamic

    {
      "type": "Dynamic",
      "parent_endpoint": "http://example.com/ilp",
      "parent_auth": "SECRET",
      "name": "my_connector_name"
    }
