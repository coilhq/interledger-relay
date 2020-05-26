#[derive(Clone, Copy, Debug, PartialEq, serde::Deserialize)]
pub enum RoutingPartition {
    /// When partitioning by `Destination`, packets of a STREAM connection
    /// follow a single route (unless that route is marked as unavailable).
    Destination,
    /// When partitioning by `ExecutionCondition`, packets of a STREAM connection
    /// are split over multiple routes.
    ExecutionCondition,
}

impl RoutingPartition {
    pub(super) fn find(self, prepare: &ilp::Prepare) -> f64 {
        let destination = prepare.destination();
        hash(match self {
            Self::Destination => destination.as_ref(),
            Self::ExecutionCondition => prepare.execution_condition(),
        })
    }
}

impl Default for RoutingPartition {
    fn default() -> Self {
        RoutingPartition::Destination
    }
}

/// Returns a number in the range `[0.0,1.0]`.
fn hash(data: &[u8]) -> f64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hasher.write(data);
    hasher.finish() as f64 / std::u64::MAX as f64
}

#[cfg(test)]
mod test_routing_partition {
    use crate::testing;
    use super::*;

    #[test]
    fn test_find() {
        assert_eq!(
            RoutingPartition::Destination.find(&testing::PREPARE),
            hash(testing::PREPARE.destination().as_ref()),
        );
        assert_eq!(
            RoutingPartition::ExecutionCondition.find(&testing::PREPARE),
            hash(testing::PREPARE.execution_condition().as_ref()),
        );
    }

    #[test]
    fn test_hash() {
        for i in 0..10_000 {
            let bytes = format!("{}", i);
            let result = hash(bytes.as_bytes());
            // Ensure that the hashing is deterministic.
            assert_eq!(result, hash(bytes.as_bytes()));
            assert_ne!(result, hash(format!("{}", i + 1).as_bytes()));

            // Ensure that the result lies in the correct range.
            assert!(0.0 <= result);
            assert!(result <= 1.0);
        }
    }
}
