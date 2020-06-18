use std::borrow::Borrow;
use std::sync::Arc;

use crate::services;
use super::Relation;

pub trait Request: Into<ilp::Prepare> + Borrow<ilp::Prepare> {}
impl Request for ilp::Prepare {}
impl Request for RequestWithHeaders {}
impl Request for RequestFromPeer {}

pub trait RequestWithPeerName: Request {
    /// The value of the `ILP-Peer-Name` header.
    fn peer_name(&self) -> Option<&[u8]>;
}

pub trait RequestWithFrom: Request {
    fn from_account(&self) -> &Arc<String>;
    fn from_relation(&self) -> Relation;
    fn from_address(&self) -> ilp::Addr;
}

#[derive(Clone, Debug, PartialEq)]
pub struct RequestWithHeaders {
    pub(crate) prepare: ilp::Prepare,
    pub(crate) headers: hyper::HeaderMap,
}

impl RequestWithHeaders {
    #[cfg(test)]
    pub fn new(prepare: ilp::Prepare, headers: hyper::HeaderMap) -> Self {
        RequestWithHeaders { prepare, headers }
    }

    pub fn header<K>(&self, header_name: K) -> Option<&[u8]>
    where
        K: hyper::header::AsHeaderName,
    {
        self.headers.get(header_name)
            .map(|header| header.as_ref())
    }
}

impl Into<ilp::Prepare> for RequestWithHeaders {
    fn into(self) -> ilp::Prepare {
        self.prepare
    }
}

impl Borrow<ilp::Prepare> for RequestWithHeaders {
    fn borrow(&self) -> &ilp::Prepare {
        &self.prepare
    }
}

impl RequestWithPeerName for RequestWithHeaders {
    fn peer_name(&self) -> Option<&[u8]> {
        static PEER_NAME: &str = "ILP-Peer-Name";
        // TODO I think this copies the name into a HeaderName every call, which isn't ideal
        self.headers
            .get(PEER_NAME)
            .map(|header| header.as_ref())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RequestFromPeer {
    pub(crate) base: RequestWithHeaders,
    pub(crate) from_account: Arc<String>,
    pub(crate) from_relation: Relation,
    pub(crate) from_address: ilp::Address,
}

impl Into<ilp::Prepare> for RequestFromPeer {
    fn into(self) -> ilp::Prepare {
        self.base.into()
    }
}

impl Borrow<ilp::Prepare> for RequestFromPeer {
    fn borrow(&self) -> &ilp::Prepare {
        self.base.borrow()
    }
}

impl RequestWithPeerName for RequestFromPeer {
    fn peer_name(&self) -> Option<&[u8]> {
        self.base.peer_name()
    }
}

impl RequestWithFrom for RequestFromPeer {
    fn from_account(&self) -> &Arc<String> {
        &self.from_account
    }

    fn from_relation(&self) -> Relation {
        self.from_relation
    }

    fn from_address(&self) -> ilp::Addr {
        self.from_address.as_addr()
    }
}

#[derive(Debug)]
pub(crate) struct ResponseWithRoute {
    pub(crate) packet: ResponsePacket,
    /// The route which forwarded (outgoing) this response's corresponding
    /// ILP-Prepare.
    pub(crate) route: Option<services::RouteIndex>,
}

type ResponsePacket = Result<ilp::Fulfill, ilp::Reject>;

impl From<ResponsePacket> for ResponseWithRoute {
    fn from(packet: ResponsePacket) -> Self {
        ResponseWithRoute {
            packet,
            route: None,
        }
    }
}
