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
pub enum GossipStream {
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

impl GossipStream {
    /// The peer's **CA-validated** Ed25519 identity key, harvested from its cert after the
    /// handshake (identity-auth Phase 1b, `docs/design/identity-authentication.md`).
    ///
    /// `Some` only for an **outbound** TLS connection (the client side), where we dialed a known
    /// `NodeId` so the key can be correlated to it; `None` for plaintext or the inbound (server)
    /// side (whose peer `NodeId` isn't yet known — the cert SAN carries only the IP). rustls has
    /// already validated the cert against the cluster CA before this runs, so the DER is a
    /// well-formed CA-issued Ed25519 cert and the length-checked SPKI scan is safe.
    #[cfg(feature = "tls")]
    pub fn peer_ed25519_key(&self) -> Option<[u8; 32]> {
        match self {
            GossipStream::TlsClient(s) => {
                let (_, conn) = s.get_ref();
                let cert = conn.peer_certificates()?.first()?;
                crate::tls::ed25519_key_from_cert_der(cert.as_ref())
            }
            _ => None,
        }
    }

    #[cfg(not(feature = "tls"))]
    pub fn peer_ed25519_key(&self) -> Option<[u8; 32]> {
        None
    }
}
