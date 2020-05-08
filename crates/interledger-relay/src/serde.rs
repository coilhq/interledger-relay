use hyper::Uri;
use serde::de::{self, Deserialize, Deserializer};

use crate::{RoutingTable, StaticRoute};

pub fn deserialize_uri<'de, D>(deserializer: D) -> Result<Uri, D::Error>
where
    D: Deserializer<'de>,
{
    <&str>::deserialize(deserializer)?
        .parse::<Uri>()
        .map_err(de::Error::custom)
}

impl<'de> Deserialize<'de> for RoutingTable {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct TableData(Vec<StaticRoute>);

        let table_data = TableData::deserialize(deserializer)?;
        Ok(RoutingTable::new(table_data.0))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::Deserialize;

    use crate::{AuthToken, BigQueryConfig, BigQueryServiceConfig, DebugServiceOptions};
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
        , "debug_service":
            { "log_prepare": false
            , "log_fulfill": false
            , "log_reject": true
            }
        , "big_query_service":
            { "queue_count": 5
            , "project_id": "PROJECT_ID"
            , "dataset_id": "DATASET_ID"
            , "table_id": "TABLE_ID"
            }
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
                routes: ROUTES[0..=1].to_vec(),
                debug_service: DebugServiceOptions {
                    log_prepare: false,
                    log_fulfill: false,
                    log_reject: true,
                },
                big_query_service: Some(BigQueryServiceConfig {
                    queue_count: 5,
                    batch_capacity: 500,
                    big_query: BigQueryConfig {
                        origin: "https://bigquery.googleapis.com".to_owned(),
                        project_id: "PROJECT_ID".to_owned(),
                        dataset_id: "DATASET_ID".to_owned(),
                        table_id: "TABLE_ID".to_owned(),
                        service_account_key_file: None,
                    },
                }),
            },
        );
    }
}
