use bytes::Bytes;

use super::{DynamicRoute, RoutingPartition, StaticRoute};

// TODO validate target prefixes
// TODO lint route order: check for unreachable; verify trailing "."

/// A simple static routing table.
///
/// Resolution is first-to-last, so the catch-all route (if any) should be the
/// last item.
#[derive(Debug)]
pub struct RoutingTable {
    partition_by: RoutingPartition,
    groups: Vec<RouteGroup>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RoutingError {
    NoRoute,
    /// One or more routes to the destination exist in the table, but all are
    /// unhealthy.
    NoHealthyRoute,
}

/// A set of routes that share a target prefix.
#[derive(Debug)]
struct RouteGroup {
    target_prefix: Bytes,
    routes: Vec<DynamicRoute>,
}

/// Uniquely identify a route within a `RoutingTable`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct RouteIndex {
    /// Index within `RoutingTable.groups`.
    pub(crate) group_index: usize,
    /// Index within `RouteGroup.routes`.
    pub(crate) route_index: usize,
}

impl RoutingTable {
    pub fn new(routes: Vec<StaticRoute>, partition_by: RoutingPartition) -> Self {
        let mut groups = Vec::<RouteGroup>::new();
        for route in routes {
            let existing_group = groups
                .iter_mut()
                .find(|group| group.target_prefix == route.target_prefix);
            if let Some(group) = existing_group {
                group.routes.push(DynamicRoute::new(route));
            } else {
                groups.push(RouteGroup {
                    target_prefix: route.target_prefix.clone(),
                    routes: vec![DynamicRoute::new(route)],
                });
            }
        }
        RoutingTable { groups, partition_by }
    }

    /// Return the first matching, healthy route (and its index).
    ///
    /// If a route with prefix `"foo.bar."` matches (even if it is unhealthy),
    /// then all subsequent matches must have the same prefix (this is used for
    /// fallback routes).
    pub(crate) fn resolve<'a>(&'a self, prepare: &'a ilp::Prepare)
        -> Result<(RouteIndex, &'a DynamicRoute), RoutingError>
    {
        let (group_index, group) = self
            .resolve_group(prepare.destination())
            .ok_or(RoutingError::NoRoute)?;
        let mut available_routes = group.routes
            .iter()
            .enumerate()
            .filter(|(_i, route)| route.is_available())
            .peekable();
        // Recompute the total partitions every `resolve` so that it only includes
        // available routes.
        let total_partitions = available_routes
            .clone()
            .map(|(_i, route)| route.config.partition)
            .sum::<f64>();

        let mut position = if group.routes.len() > 1 {
            self.partition_by.find(prepare)
        } else {
            // Don't bother to compute the hash unnecessarily.
            0.0
        };

        while let Some((route_index, route)) = available_routes.next() {
            let fraction = route.config.partition / total_partitions;
            if position <= fraction || available_routes.peek().is_none() {
                // The last matching available route is always used as a catch-all.
                return Ok((RouteIndex { group_index, route_index }, route));
            }
            // Shift `position` down so that it fits in the upcoming partitions.
            position -= fraction;
        }

        Err(RoutingError::NoHealthyRoute)
    }

    fn resolve_group<'a>(&'a self, destination: ilp::Addr<'a>)
        -> Option<(usize, &'a RouteGroup)>
    {
        self.groups
            .iter()
            .enumerate()
            .find(|(_index, group)| {
                destination.as_ref().starts_with(&group.target_prefix)
            })
    }

    pub(crate) fn update(&self, index: RouteIndex, is_success: bool) {
        self.groups[index.group_index]
            .routes[index.route_index]
            .update(is_success)
    }
}

#[cfg(test)]
impl RouteIndex {
    pub const fn new(group_index: usize, route_index: usize) -> Self {
        RouteIndex { group_index, route_index }
    }
}

#[cfg(test)]
impl std::ops::Index<RouteIndex> for RoutingTable {
    type Output = DynamicRoute;
    fn index(&self, index: RouteIndex) -> &Self::Output {
        &self.groups[index.group_index].routes[index.route_index]
    }
}

#[cfg(test)]
impl std::ops::Index<(usize, usize)> for RoutingTable {
    type Output = DynamicRoute;
    fn index(&self, (group_index, route_index): (usize, usize)) -> &Self::Output {
        &self.groups[group_index].routes[route_index]
    }
}

#[cfg(test)]
mod test_routing_table {
    use std::time;

    use bytes::Bytes;
    use lazy_static::lazy_static;

    use crate::NextHop;
    use crate::services::RouteStatus;
    use crate::testing::ROUTES;
    use super::*;

    lazy_static! {
        static ref HOP_0: NextHop = ROUTES[0].next_hop.clone();
        static ref HOP_1: NextHop = ROUTES[1].next_hop.clone();
        static ref HOP_2: NextHop = ROUTES[2].next_hop.clone();
    }

    #[test]
    fn test_resolve() {
        let table = RoutingTable::new(vec![
            StaticRoute::new(Bytes::from("test.one"), HOP_0.clone()),
            StaticRoute::new(Bytes::from("test.two"), HOP_1.clone()),
            StaticRoute::new(Bytes::from("test."), HOP_2.clone()),
        ], RoutingPartition::default());

        let tests = &[
            // Exact match.
            ("test.one", Ok(RouteIndex::new(0, 0))),
            // Prefix match.
            ("test.one.alice", Ok(RouteIndex::new(0, 0))),
            ("test.two.bob", Ok(RouteIndex::new(1, 0))),
            ("test.three", Ok(RouteIndex::new(2, 0))),
            // Dot separator isn't necessary.
            ("test.two__", Ok(RouteIndex::new(1, 0))),
            // Unhealthy
            // No matching prefix.
            ("example.test.one", Err(RoutingError::NoRoute)),
            ("g.alice", Err(RoutingError::NoRoute)),
        ];

        for (addr, index) in tests {
            let addr = addr.as_bytes();
            let expect = index.map(|index| (index, &table[index]));
            assert_eq!(table.resolve(&make_prepare(addr)), expect);
        }
    }

    #[test]
    fn test_resolve_unhealthy() {
        let table = RoutingTable::new(vec![
            StaticRoute::new(Bytes::from("test.one"), HOP_0.clone()),
            StaticRoute::new_with_partition(Bytes::from("test.one"), HOP_2.clone(), 0.0),
        ], RoutingPartition::default());
        assert_eq!(
            table.resolve(&make_prepare(b"test.one.a")),
            Ok((RouteIndex::new(0, 0), &table[(0, 0)])),
        );

        *table[(0, 0)].status.write().unwrap() = RouteStatus::Unhealthy {
            until: time::Instant::now() + time::Duration::from_secs(1),
        };
        assert_eq!(
            table.resolve(&make_prepare(b"test.one.a")),
            Ok((RouteIndex::new(0, 1), &table[(0, 1)])),
        );

        *table[(0, 1)].status.write().unwrap() = RouteStatus::Unhealthy {
            until: time::Instant::now() + time::Duration::from_secs(1),
        };
        assert_eq!(
            table.resolve(&make_prepare(b"test.one.a")),
            Err(RoutingError::NoHealthyRoute),
        );
    }

    #[test]
    fn test_resolve_catch_all() {
        let table = RoutingTable::new(vec![
            StaticRoute::new(Bytes::from("test.one"), HOP_0.clone()),
            StaticRoute::new(Bytes::from("test.two"), HOP_1.clone()),
            StaticRoute::new(Bytes::from(""), HOP_2.clone()),
        ], RoutingPartition::default());
        assert_eq!(
            table.resolve(&make_prepare(b"example.test.one")),
            Ok((RouteIndex::new(2, 0), &table[(2, 0)])),
        );
    }

    #[test]
    fn test_resolve_partition() {
        let table = RoutingTable::new(vec![
            StaticRoute::new_with_partition(Bytes::from("test.one."), HOP_0.clone(), 0.50),
            StaticRoute::new_with_partition(Bytes::from("test.one."), HOP_1.clone(), 0.25),
            StaticRoute::new_with_partition(Bytes::from("test.one."), HOP_1.clone(), 0.25),
        ], RoutingPartition::Destination);

        let mut counts = [0_i32; 3];
        for i in 0..10_000 {
            let (index, _route) =
                table.resolve(&make_prepare(&alice(i))).unwrap();
            counts[index.route_index] += 1;
        }
        // Ensure that the partitions are (mostly) balanced.
        assert!((counts[0] - 5_000).abs() < 100);
        assert!((counts[1] - 2_500).abs() < 100);
        assert!((counts[2] - 2_500).abs() < 100);

        // When the first route is down, all traffic is routed to the remaining route.
        *table[(0, 0)].status.write().unwrap() = RouteStatus::Unhealthy {
            until: time::Instant::now() + time::Duration::from_secs(1),
        };

        let mut counts = [0_i32; 3];
        for i in 0..10_000 {
            let (index, _route) =
                table.resolve(&make_prepare(&alice(i))).unwrap();
            counts[index.route_index] += 1;
        }
        assert_eq!(counts[0], 0);
        assert!((counts[1] - 5_000).abs() < 100);
        assert!((counts[2] - 5_000).abs() < 100);
    }

    fn make_prepare(address: &[u8]) -> ilp::Prepare {
        ilp::PrepareBuilder {
            amount: 123,
            expires_at: std::time::SystemTime::now()
                + std::time::Duration::from_secs(20),
            execution_condition: b"\
                \x11\x7b\x43\x4f\x1a\x54\xe9\x04\x4f\x4f\x54\x92\x3b\x2c\xff\x9e\
                \x4a\x6d\x42\x0a\xe2\x81\xd5\x02\x5d\x7b\xb0\x40\xc4\xb4\xc0\x4a\
            ",
            destination: ilp::Addr::try_from(address).unwrap(),
            data: b"prepare data",
        }.build()
    }

    fn alice(n: usize) -> Vec<u8> {
        format!("test.one.alice.{}", n).into_bytes()
    }
}
