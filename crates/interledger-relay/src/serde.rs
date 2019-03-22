use hyper::Uri;
use serde::de::{self, Deserialize, Deserializer};

pub fn deserialize_uri<'de, D>(deserializer: D) -> Result<Uri, D::Error>
where
    D: Deserializer<'de>,
{
    <&str>::deserialize(deserializer)?
        .parse::<Uri>()
        .map_err(de::Error::custom)
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use crate::AuthToken;
    use crate::app::{Config, ConnectorRoot, PeerConfig};
    use crate::testing::ROUTES;
    use super::*;

    #[test]
    fn test_deserialize_uri() {
        #[derive(Debug, PartialEq, Deserialize)]
        struct UriData(
            #[serde(deserialize_with = "deserialize_uri")]
            Uri,
        );

        assert_eq!(
            serde_json::from_str::<UriData>(r#"
                "http://example.com/foo"
            "#).unwrap(),
            UriData(Uri::from_static("http://example.com/foo")),
        );
        assert!(serde_json::from_str::<UriData>("\"not a uri\"").is_err());
        assert!(serde_json::from_str::<UriData>("1234").is_err());
    }

    #[test]
    fn test_deserialize_connector_builder() {
        let config = serde_json::from_str::<Config>(r#"
        { "root":
          { "type": "Static"
          , "address": "test.relay"
          , "asset_scale": 9
          , "asset_code": "XRP"
          }
        , "peers":
          [ { "type": "Child"
            , "auth": ["child_secret"]
            , "suffix": "child"
            }
          , { "type": "Parent"
            , "auth": ["parent_secret"]
            }
          ]
        , "routes":
          [ { "target_prefix": "test.alice."
            , "next_hop":
              { "type": "Bilateral"
              , "endpoint": "http://127.0.0.1:3001/alice"
              , "auth": "alice_auth"
              }
            }
          , { "target_prefix": "test.relay."
            , "next_hop":
              { "type": "Multilateral"
              , "endpoint_prefix": "http://127.0.0.1:3001/bob/"
              , "endpoint_suffix": "/ilp"
              , "auth": "bob_auth"
              }
            }
          ]
        }"#).expect("valid json");

        assert_eq!(
            config,
            Config {
                root: ConnectorRoot::Static {
                    address: ilp::Address::new(b"test.relay"),
                    asset_scale: 9,
                    asset_code: "XRP".to_owned(),
                },
                peers: vec![
                    PeerConfig::Child {
                        auth: vec![AuthToken::new("child_secret")],
                        suffix: "child".to_owned(),
                    },
                    PeerConfig::Parent {
                        auth: vec![AuthToken::new("parent_secret")],
                    },
                ],
                routes: ROUTES[0..=1].to_vec(),
            },
        );
    }
}
