use super::{DynamicRoute, StaticRoute};

// TODO validate target prefixes
// TODO lint route order: check for unreachable; verify trailing "."

/// A simple static routing table.
///
/// Resolution is first-to-last, so the catch-all route (if any) should be the
/// last item.
#[derive(Debug, PartialEq)]
pub struct RoutingTable(Vec<DynamicRoute>);

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RoutingError {
    NoRoute,
    /// One or more routes to the destination exist in the table, but all are
    /// unhealthy.
    NoHeathyRoute,
}

impl RoutingTable {
    #[inline]
    pub fn new(routes: Vec<StaticRoute>) -> Self {
        let routes = routes
            .into_iter()
            .map(DynamicRoute::new)
            .collect::<Vec<_>>();
        RoutingTable(routes)
    }

    /// Return the first matching, healthy route (and its index).
    ///
    /// If a route with prefix `"foo.bar."` matches (even if it is unhealthy),
    /// then all subsequent matches must have the same prefix (this is used for
    /// fallback routes).
    pub fn resolve<'a>(&'a self, destination: ilp::Addr<'a>)
        -> Result<(usize, &'a DynamicRoute), RoutingError>
    {
        let mut route_exists = false;
        self.resolve_static(destination)
            .find(|(_i, route)| {
                route_exists = true;
                route.is_available()
            })
            .ok_or_else(|| if route_exists {
                RoutingError::NoHeathyRoute
            } else {
                RoutingError::NoRoute
            })
    }

    fn resolve_static<'a>(&'a self, destination: ilp::Addr<'a>)
        -> impl Iterator<Item = (usize, &DynamicRoute)> + 'a
    {
        let mut first_hit = None;
        self.0
            .iter()
            .enumerate()
            .filter(move |(_index, route)| {
                let target_prefix = &route.config.target_prefix;
                match first_hit {
                    Some(exact_prefix) => target_prefix == exact_prefix,
                    None => {
                        let matches =
                            destination.as_ref().starts_with(target_prefix);
                        if matches { first_hit = Some(target_prefix); }
                        matches
                    },
                }
            })
    }

    pub(crate) fn update(&mut self, index: usize, is_success: bool) {
        self.0[index].update(is_success)
    }
}

impl AsRef<[DynamicRoute]> for RoutingTable {
    fn as_ref(&self) -> &[DynamicRoute] {
        &self.0
    }
}

impl Default for RoutingTable {
    fn default() -> Self {
        RoutingTable::new(Vec::new())
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
        ]);
        let routes = &table.0;

        let tests = &[
            // Exact match.
            ("test.one", Ok((0, &routes[0]))),
            // Prefix match.
            ("test.one.alice", Ok((0, &routes[0]))),
            ("test.two.bob", Ok((1, &routes[1]))),
            ("test.three", Ok((2, &routes[2]))),
            // Dot separator isn't necessary.
            ("test.two__", Ok((1, &routes[1]))),
            // Unhealthy
            // No matching prefix.
            ("example.test.one", Err(RoutingError::NoRoute)),
            ("g.alice", Err(RoutingError::NoRoute)),
        ];

        for (addr, resolve) in tests {
            let addr = addr.as_bytes();
            assert_eq!(table.resolve(ilp::Addr::new(addr)), *resolve);
        }
    }

    #[test]
    fn test_resolve_unhealthy() {
        let mut table = RoutingTable::new(vec![
            StaticRoute::new(Bytes::from("test.one"), HOP_0.clone()),
            StaticRoute::new(Bytes::from("test.one"), HOP_1.clone()),
        ]);

        table.0[0].status = RouteStatus::Unhealthy {
            until: time::Instant::now() + time::Duration::from_secs(1),
        };
        assert_eq!(
            table.resolve(ilp::Addr::new(b"test.one.alice")),
            Ok((1, &table.as_ref()[1])),
        );

        table.0[1].status = RouteStatus::Unhealthy {
            until: time::Instant::now() + time::Duration::from_secs(1),
        };
        assert_eq!(
            table.resolve(ilp::Addr::new(b"test.one.alice")),
            Err(RoutingError::NoHeathyRoute),
        );
    }

    #[test]
    fn test_resolve_catch_all() {
        let table = RoutingTable::new(vec![
            StaticRoute::new(Bytes::from("test.one"), HOP_0.clone()),
            StaticRoute::new(Bytes::from("test.two"), HOP_1.clone()),
            StaticRoute::new(Bytes::from(""), HOP_2.clone()),
        ]);
        assert_eq!(
            table.resolve(ilp::Addr::new(b"example.test.one")),
            Ok((2, &table.0[2])),
        );
    }
}
