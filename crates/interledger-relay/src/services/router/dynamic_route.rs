use std::sync;
use std::time;

use log::{info, warn};

use super::StaticRoute;

const MAX_WINDOW_DURATION: time::Duration =
    time::Duration::from_secs(5 * 60);

/// A dynamic route's availability changes according to the health of its endpoint.
#[derive(Debug)]
pub struct DynamicRoute {
    pub config: StaticRoute,
    /// The whole routing table is in a `RwLock`, but wrapping each `status` in
    /// an independent lock ensures that e.g. routing table lookups don't interfere
    /// with health updates.
    pub status: sync::RwLock<RouteStatus>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RouteStatus {
    Infallible,
    Healthy {
        remaining: usize,
        failures: usize,
        updated_at: time::Instant,
    },
    Unhealthy {
        // TODO use exponential backoff? or maybe exp backoff of window_size
        until: time::Instant,
    },
}

impl DynamicRoute {
    pub fn new(config: StaticRoute) -> Self {
        let status = sync::RwLock::new(match &config.failover {
            None => RouteStatus::Infallible,
            Some(failover) => RouteStatus::Healthy {
                remaining: failover.window_size,
                failures: 0,
                updated_at: time::Instant::now(),
            },
        });
        DynamicRoute { config, status }
    }

    pub fn with_status(config: StaticRoute, status: RouteStatus) -> Self {
        DynamicRoute {
            config,
            status: sync::RwLock::new(status),
        }
    }

    pub fn is_available(&self) -> bool {
        match *self.status.read().unwrap() {
            RouteStatus::Infallible => true,
            RouteStatus::Healthy { .. } => true,
            RouteStatus::Unhealthy { until } => until < time::Instant::now(),
        }
    }

    pub fn update(&self, is_success: bool) {
        self.update_with_now(is_success, time::Instant::now());
    }

    fn update_with_now(&self, is_success: bool, now: time::Instant) {
        let fails = (!is_success) as usize;
        if *self.status.read().unwrap() == RouteStatus::Infallible {
            return;
        }

        let mut status = self.status.write().unwrap();
        match &mut *status {
            RouteStatus::Infallible => {},
            RouteStatus::Healthy { remaining, failures, updated_at } => {
                let failover = self.config.failover.as_ref().unwrap();
                if now - *updated_at > MAX_WINDOW_DURATION {
                    *remaining = failover.window_size;
                    *failures = 0;
                }

                *remaining -= 1;
                *failures += fails;
                *updated_at = now;
                let fail_ratio = *failures as f64 / failover.window_size as f64;
                if failover.fail_ratio <= fail_ratio {
                    // Test the `fail_ratio` even before `remaining` is `0`, so
                    // that bad routes fail early.
                    let until = now + failover.fail_duration;
                    *status = RouteStatus::Unhealthy { until };
                    warn!(
                        "marking route unhealthy: target_prefix={:?} next_hop={:?} until={:?}",
                        self.config.target_prefix,
                        self.config.next_hop,
                        until,
                    );
                } else if *remaining == 0 {
                    *remaining = failover.window_size;
                    *failures = 0;
                }
            },
            RouteStatus::Unhealthy { until } => {
                if now < *until { return; }
                let failover = self.config.failover.as_ref().unwrap();
                info!(
                    "marking route healthy: target_prefix={:?} next_hop={:?}",
                    self.config.target_prefix,
                    self.config.next_hop,
                );
                *status = RouteStatus::Healthy {
                    remaining: failover.window_size - fails,
                    failures: fails,
                    updated_at: now,
                };
            },
        }
    }
}

#[cfg(test)]
impl PartialEq for DynamicRoute {
    fn eq(&self, other: &DynamicRoute) -> bool {
        self.config == other.config
            && *self.status.read().unwrap() == *other.status.read().unwrap()
    }
}

#[cfg(test)]
mod test_dynamic_route {
    use bytes::Bytes;
    use lazy_static::lazy_static;

    use crate::RouteFailover;
    use crate::testing;
    use super::*;

    const SECOND: time::Duration = time::Duration::from_secs(1);

    lazy_static! {
        static ref ROUTE: StaticRoute = StaticRoute {
            target_prefix: Bytes::from("test.alice"),
            next_hop: testing::ROUTES[0].next_hop.clone(),
            failover: Some(RouteFailover {
                window_size: 20,
                fail_ratio: 0.06,
                fail_duration: 2 * SECOND,
            }),
        };
    }

    #[test]
    fn test_is_available() {
        let now = time::Instant::now();
        let unhealthy_past = DynamicRoute::with_status(
            ROUTE.clone(),
            RouteStatus::Unhealthy { until: now - SECOND },
        );
        let unhealthy_future = DynamicRoute::with_status(
            ROUTE.clone(),
            RouteStatus::Unhealthy { until: now + SECOND },
        );
        assert_eq!(unhealthy_past.is_available(), true);
        assert_eq!(unhealthy_future.is_available(), false);
    }

    #[test]
    fn test_update() {
        struct Test {
            success: bool,
            before: RouteStatus,
            after: RouteStatus,
        }

        let now = time::Instant::now();
        let tests = &[
            // infallible → infallible
            Test {
                success: false,
                before: RouteStatus::Infallible,
                after: RouteStatus::Infallible,
            },
            // unhealthy → unhealthy
            Test {
                success: false,
                before: RouteStatus::Unhealthy { until: now + 5 * SECOND },
                after: RouteStatus::Unhealthy { until: now + 5 * SECOND },
            },
            // unhealthy → healthy
            Test {
                success: false,
                before: RouteStatus::Unhealthy { until: now - 5 * SECOND },
                after: RouteStatus::Healthy {
                    remaining: 19,
                    failures: 1,
                    updated_at: now,
                },
            },
            // healthy → unhealthy
            Test {
                success: false,
                before: RouteStatus::Healthy {
                    remaining: 1,
                    failures: 2,
                    updated_at: now,
                },
                after: RouteStatus::Unhealthy { until: now + 2 * SECOND },
            },
            // healthy → unhealthy (shortcut)
            Test {
                success: false,
                before: RouteStatus::Healthy {
                    remaining: 10,
                    failures: 2,
                    updated_at: now,
                },
                after: RouteStatus::Unhealthy { until: now + 2 * SECOND },
            },

            // healthy → healthy (reset; window)
            Test {
                success: false,
                before: RouteStatus::Healthy {
                    remaining: 1,
                    failures: 0,
                    updated_at: now,
                },
                after: RouteStatus::Healthy {
                    remaining: 20,
                    failures: 0,
                    updated_at: now,
                },
            },
            // healthy → healthy (reset; time)
            Test {
                success: false,
                before: RouteStatus::Healthy {
                    remaining: 15,
                    failures: 0,
                    updated_at: now - MAX_WINDOW_DURATION - SECOND,
                },
                after: RouteStatus::Healthy {
                    remaining: 19,
                    failures: 1,
                    updated_at: now,
                },
            },

            // healthy → healthy (success)
            Test {
                success: true,
                before: RouteStatus::Healthy {
                    remaining: 15,
                    failures: 1,
                    updated_at: now - SECOND,
                },
                after: RouteStatus::Healthy {
                    remaining: 14,
                    failures: 1,
                    updated_at: now,
                },
            },
            // healthy → healthy (reset; success)
            Test {
                success: true,
                before: RouteStatus::Healthy {
                    remaining: 1,
                    failures: 1,
                    updated_at: now - SECOND,
                },
                after: RouteStatus::Healthy {
                    remaining: 20,
                    failures: 0,
                    updated_at: now,
                },
            },
        ];

        for (i, test) in tests.iter().enumerate() {
            let route =
                DynamicRoute::with_status(ROUTE.clone(), test.before.clone());
            route.update_with_now(test.success, now);
            let route_after =
                DynamicRoute::with_status(ROUTE.clone(), test.after.clone());
            assert_eq!(route, route_after, "index={:?}", i);
        }
    }
}
