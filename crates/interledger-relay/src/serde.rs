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
    use std::sync::Arc;
    use std::time;

    use serde::Deserialize;

    use crate::{AuthToken, BigQueryConfig, BigQueryServiceConfig, DebugServiceOptions, RoutingPartition, RoutingTableData};
    use crate::app::{Config, ConnectorRoot, RelationConfig};
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
        , "relatives":
          [ { "type": "Child"
            , "account": "child_account"
            , "auth": ["child_secret"]
            , "suffix": "child"
            }
          , { "type": "Parent"
            , "account": "parent_account"
            , "auth": ["parent_secret"]
            }
          ]
        , "routes":
          { "test.alice.":
            [ { "next_hop":
                { "type": "Bilateral"
                , "endpoint": "http://127.0.0.1:3001/alice"
                , "auth": "alice_auth"
                }
              , "account": "alice"
              }
            ]
          , "test.relay.":
            [ { "next_hop":
                { "type": "Multilateral"
                , "endpoint_prefix": "http://127.0.0.1:3001/bob/"
                , "endpoint_suffix": "/ilp"
                , "auth": "bob_auth"
                }
              , "account": "bob"
              }
            ]
          , "":
            [ { "next_hop":
                { "type": "Bilateral"
                , "endpoint": "http://127.0.0.1:3001/default"
                , "auth": "default_auth"
                }
              , "account": "default"
              }
            ]
          }
        , "debug_service":
            { "log_prepare": false
            , "log_fulfill": false
            , "log_reject": true
            }
        , "big_query_service":
            { "queue_count": 5
            , "flush_interval": { "secs": 123, "nanos": 0 }
            , "project_id": "PROJECT_ID"
            , "dataset_id": "DATASET_ID"
            , "table_id": "TABLE_ID"
            }
        , "pre_stop_path": "/pre_stop"
        , "routing_partition": "ExecutionCondition"
        }"#).expect("valid json");

        assert_eq!(
            config,
            Config {
                root: ConnectorRoot::Static {
                    address: ilp::Address::new(b"test.relay"),
                    asset_scale: 9,
                    asset_code: "XRP".to_owned(),
                },
                relatives: vec![
                    RelationConfig::Child {
                        account: Arc::new("child_account".to_owned()),
                        auth: vec![AuthToken::new("child_secret")],
                        suffix: "child".to_owned(),
                    },
                    RelationConfig::Parent {
                        account: Arc::new("parent_account".to_owned()),
                        auth: vec![AuthToken::new("parent_secret")],
                    },
                ],
                routes: RoutingTableData(ROUTES.to_vec()),
                debug_service: DebugServiceOptions {
                    log_prepare: false,
                    log_fulfill: false,
                    log_reject: true,
                },
                big_query_service: Some(BigQueryServiceConfig {
                    queue_count: 5,
                    batch_capacity: 500,
                    flush_interval: time::Duration::from_secs(123),
                    big_query: BigQueryConfig {
                        origin: "https://bigquery.googleapis.com".to_owned(),
                        project_id: "PROJECT_ID".to_owned(),
                        dataset_id: "DATASET_ID".to_owned(),
                        table_id: "TABLE_ID".to_owned(),
                        service_account_key_file: None,
                    },
                }),
                pre_stop_path: Some("/pre_stop".to_owned()),
                routing_partition: RoutingPartition::ExecutionCondition,
            },
        );
    }
}
