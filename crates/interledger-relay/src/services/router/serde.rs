use std::collections::HashMap;

use bytes::Bytes;
use serde::de::{Deserialize, Deserializer};

use super::{NextHop, RouteFailover, StaticRoute};

#[derive(Clone, Debug, PartialEq)]
pub struct RoutingTableData(pub Vec<StaticRoute>);

#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
struct RouteMap(HashMap<String, Vec<RouteData>>);

#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RouteData {
    pub next_hop: NextHop,
    pub failover: Option<RouteFailover>,
    #[serde(default = "default_partition")]
    pub partition: f64,
}

fn default_partition() -> f64 { 1.0 }

impl<'de> Deserialize<'de> for RoutingTableData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut routes_by_prefix = RouteMap::deserialize(deserializer)?.0;
        let mut prefixes = routes_by_prefix
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        // Order prefixes from longest-to-shortest so that long prefixes don't
        // get masked by short ones.
        prefixes.sort_unstable_by_key(|prefix| usize::MAX - prefix.len());

        let mut routes = Vec::new();
        for prefix in prefixes {
            let route_datas = routes_by_prefix
                .remove(&prefix)
                .unwrap();
            let prefix = Bytes::from(prefix);
            for route_data in route_datas {
                routes.push(StaticRoute {
                    target_prefix: prefix.clone(),
                    next_hop: route_data.next_hop,
                    failover: route_data.failover,
                    partition: route_data.partition,
                });
            }
        }
        Ok(RoutingTableData(routes))
    }
}

impl Into<Vec<StaticRoute>> for RoutingTableData {
    #[inline]
    fn into(self) -> Vec<StaticRoute> {
        self.0
    }
}
