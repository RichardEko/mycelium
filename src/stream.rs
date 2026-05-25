use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};

/// A TCP stream that is optionally wrapped in TLS.
///
/// Both variants are `Unpin`; the `AsyncRead`/`AsyncWrite` impls forward to
/// the inner type without boxing or vtable indirection.
pub(crate) enum GossipStream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    TlsServer(tokio_rustls::server::TlsStream<TcpStream>),
    #[cfg(feature = "tls")]
    TlsClient(tokio_rustls::client::TlsStream<TcpStream>),
}

impl AsyncRead for GossipStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            GossipStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            GossipStream::TlsServer(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            GossipStream::TlsClient(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for GossipStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            GossipStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            GossipStream::TlsServer(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            GossipStream::TlsClient(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            GossipStream::Plain(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            GossipStream::TlsServer(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            GossipStream::TlsClient(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            GossipStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            GossipStream::TlsServer(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            GossipStream::TlsClient(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl Unpin for GossipStream {}
