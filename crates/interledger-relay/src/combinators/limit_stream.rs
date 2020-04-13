use std::error::Error;
use std::fmt;
use std::pin::Pin;

use futures::prelude::*;
use futures::task::{Context, Poll};

/// A stream combinator which returns a maximum number of bytes before failing.
#[derive(Debug)]
pub struct LimitStream<S> {
    remaining: usize,
    stream: S,
}

impl<S> LimitStream<S> {
    #[inline]
    pub fn new(max_bytes: usize, stream: S) -> Self {
        LimitStream {
            remaining: max_bytes,
            stream,
        }
    }
}

impl<S: futures::stream::TryStream> Stream for LimitStream<S>
where
    S: std::marker::Unpin + futures::stream::TryStream,
    S::Ok: AsRef<[u8]>,
    S::Error: Error,
{
    type Item = Result<S::Ok, LimitStreamError<S::Error>>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context)
        -> Poll<Option<Self::Item>>
    {
        let poll = Pin::new(&mut self.stream).try_poll_next(context);
        if let Poll::Ready(Some(Ok(chunk))) = &poll {
            let chunk_size = chunk.as_ref().len();
            match self.remaining.checked_sub(chunk_size) {
                Some(remaining) => self.remaining = remaining,
                None => {
                    self.remaining = 0;
                    return Poll::Ready(Some(Err(LimitStreamError::LimitExceeded)));
                },
            }
        }

        poll.map(|item| item.map(|chunk_result| {
            chunk_result.map_err(LimitStreamError::from)
        }))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.remaining))
    }
}

#[derive(Debug, PartialEq)]
pub enum LimitStreamError<E> {
    LimitExceeded,
    StreamError(E),
}

impl<E: Error + 'static> Error for LimitStreamError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self {
            LimitStreamError::LimitExceeded => None,
            LimitStreamError::StreamError(error) => Some(error),
        }
    }
}

impl<E: Error> From<E> for LimitStreamError<E> {
    fn from(error: E) -> Self {
        LimitStreamError::StreamError(error)
    }
}

impl<E: Error> fmt::Display for LimitStreamError<E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LimitStreamError::LimitExceeded => f.write_str("LimitExceeded"),
            LimitStreamError::StreamError(error) => {
                write!(f, "StreamError({})", error)
            },
        }
    }
}

#[cfg(test)]
mod test_limit_stream {
    use bytes::{Bytes, BytesMut};

    use super::*;

    #[test]
    fn test_stream() {
        const SIZE: usize = 256;
        let buffer = Bytes::from(&[0x00; SIZE][..]);

        // Buffer size is below limit.
        assert_eq!(
            collect_limited_stream(buffer.clone(), SIZE + 1).unwrap(),
            buffer,
        );
        // Buffer size is equal to the limit.
        assert_eq!(
            collect_limited_stream(buffer.clone(), SIZE).unwrap(),
            buffer,
        );
        // Buffer size is above the limit.
        assert!({
            collect_limited_stream(buffer.clone(), SIZE - 1)
                .unwrap_err()
                .is_limit_exceeded()
        });
    }

    fn collect_limited_stream(buffer: Bytes, limit: usize)
        -> Result<Bytes, LimitStreamError<hyper::Error>>
    {
        use futures::executor::block_on;
        let body = hyper::Body::from(buffer);
        let mut stream = LimitStream::new(limit, body);
        let mut output = BytesMut::new();
        while let Some(chunk) = block_on(stream.try_next())? {
            output.extend(chunk);
        }
        Ok(output.freeze())
    }

    impl<E: Error> LimitStreamError<E> {
        fn is_limit_exceeded(&self) -> bool {
            match self {
                LimitStreamError::LimitExceeded => true,
                _ => false,
            }
        }
    }
}
