//! # interledger-packet
//!
//! Interledger packet serialization/deserialization.
//!
//! # References
//!
//!   * <https://github.com/interledger/rfcs/blob/master/0027-interledger-protocol-4/0027-interledger-protocol-4.md#packet-format>
//!   * <https://github.com/interledger/rfcs/blob/master/asn1/InterledgerProtocol.asn>
//!

mod address;
mod error;
mod errors;
#[cfg(test)]
mod fixtures;
pub mod ildcp;
pub mod oer;
mod packet;

pub use self::address::{Addr, Address, AddressError};
pub use self::error::{ErrorClass, ErrorCode};
pub use self::errors::ParseError;

pub use self::packet::MaxPacketAmountDetails;
pub use self::packet::{Fulfill, Packet, PacketType, Prepare, Reject};
pub use self::packet::{FulfillBuilder, PrepareBuilder, RejectBuilder};
