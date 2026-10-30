use std::error::Error;
use std::net::SocketAddr;

use bytes::BytesMut;
use proxy_protocol::version2::{ProxyAddresses, ProxyCommand, ProxyTransportProtocol};
use proxy_protocol::EncodeError;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::net;

/// Read and validate an incoming HAProxy PROXY protocol v2 header.
pub async fn read_proxy_v2(inbound: &mut TcpStream) -> io::Result<(SocketAddr, SocketAddr)> {
    const SIG: [u8; 12] = [13,10,13,10,0,13,10,81,85,73,84,10];
    let mut h=[0u8;16]; inbound.read_exact(&mut h).await?;
    if h[..12]!=SIG || h[12]!=0x21 { return Err(io::Error::new(io::ErrorKind::InvalidData,"invalid PROXY v2 signature/command")); }
    let family=h[13]; let len=u16::from_be_bytes([h[14],h[15]]) as usize;
    if len>4096 { return Err(io::Error::new(io::ErrorKind::InvalidData,"PROXY v2 payload too large")); }
    let mut p=vec![0u8;len]; inbound.read_exact(&mut p).await?;
    match family {
        0x11 if len>=12 => { let s=std::net::Ipv4Addr::new(p[0],p[1],p[2],p[3]); let d=std::net::Ipv4Addr::new(p[4],p[5],p[6],p[7]); Ok((SocketAddr::new(s.into(),u16::from_be_bytes([p[8],p[9]])),SocketAddr::new(d.into(),u16::from_be_bytes([p[10],p[11]])))) }
        0x21 if len>=36 => { let s=std::net::Ipv6Addr::from(<[u8;16]>::try_from(&p[0..16]).unwrap()); let d=std::net::Ipv6Addr::from(<[u8;16]>::try_from(&p[16..32]).unwrap()); Ok((SocketAddr::new(s.into(),u16::from_be_bytes([p[32],p[33]])),SocketAddr::new(d.into(),u16::from_be_bytes([p[34],p[35]])))) }
        _ => Err(io::Error::new(io::ErrorKind::InvalidData,"unsupported PROXY v2 family/protocol or truncated address")),
    }
}

/// Build an outbound PROXY v2 header using the original ingress addresses.
pub fn stream_proxy_header_addresses(peer:SocketAddr,local:SocketAddr)->Result<BytesMut,EncodeError>{
 let header=proxy_protocol::ProxyHeader::Version2{command:ProxyCommand::Proxy,transport_protocol:ProxyTransportProtocol::Stream,addresses:match(peer,local){(SocketAddr::V4(source),SocketAddr::V4(destination))=>ProxyAddresses::Ipv4{source,destination},(SocketAddr::V6(source),SocketAddr::V6(destination))=>ProxyAddresses::Ipv6{source,destination},_=>unreachable!("PROXY v2 source/destination families must match")}}; proxy_protocol::encode(header)
}

/// Proxy the inbound stream to a target address.
pub async fn proxy(
    inbound: TcpStream,
    proxy_header: ProxyHeader,
    addr_target: SocketAddr,
) -> Result<(), Box<dyn Error>> {
    let peer=inbound.peer_addr()?; let local=inbound.local_addr()?;
    proxy_with_queue_addresses(inbound,proxy_header,addr_target,&[],peer,local).await
}

/// Proxy the inbound stream to a target address.
///
/// Send the queue to the target server before proxying.
pub async fn proxy_with_queue(
    inbound: TcpStream,
    proxy_header: ProxyHeader,
    addr_target: SocketAddr,
    queue: &[u8],
) -> Result<(), Box<dyn Error>> {
    let peer=inbound.peer_addr()?; let local=inbound.local_addr()?;
    proxy_with_queue_addresses(inbound,proxy_header,addr_target,queue,peer,local).await
}

/// Proxy using explicit original client and destination addresses.
pub async fn proxy_with_queue_addresses(inbound:TcpStream,proxy_header:ProxyHeader,addr_target:SocketAddr,queue:&[u8],peer:SocketAddr,local:SocketAddr)->Result<(),Box<dyn Error>>{
    // Set up connection to server
    // TODO: on connect fail, ping server and redirect to serve_status if offline
    let mut outbound = TcpStream::connect(addr_target).await?;

    // Add proxy header
    match proxy_header {
        ProxyHeader::None => {}
        ProxyHeader::Local => {
            let header = local_proxy_header()?;
            outbound.write_all(&header).await?;
        }
        ProxyHeader::Proxy => {
            let header = stream_proxy_header_addresses(peer,local)?;
            outbound.write_all(&header).await?;
        }
    }

    // Start proxy on both streams
    proxy_inbound_outbound_with_queue(inbound, outbound, &[], queue).await
}

/// Proxy the inbound stream to a target address.
///
/// Send the queue to the target server before proxying.
// TODO: find better name for this
pub async fn proxy_inbound_outbound_with_queue(
    mut inbound: TcpStream,
    mut outbound: TcpStream,
    inbound_queue: &[u8],
    outbound_queue: &[u8],
) -> Result<(), Box<dyn Error>> {
    let (mut ri, mut wi) = inbound.split();
    let (mut ro, mut wo) = outbound.split();

    // Forward queued bytes to client once writable
    if !inbound_queue.is_empty() {
        wi.writable().await?;
        trace!(target: "lazymc", "Relaying {} queued bytes to client", inbound_queue.len());
        wi.write_all(inbound_queue).await?;
    }

    // Forward queued bytes to server once writable
    if !outbound_queue.is_empty() {
        wo.writable().await?;
        trace!(target: "lazymc", "Relaying {} queued bytes to server", outbound_queue.len());
        wo.write_all(outbound_queue).await?;
    }

    let client_to_server = async {
        io::copy(&mut ri, &mut wo).await?;
        wo.shutdown().await
    };
    let server_to_client = async {
        io::copy(&mut ro, &mut wi).await?;
        wi.shutdown().await
    };

    tokio::try_join!(client_to_server, server_to_client)?;

    // Gracefully close connection if not done already
    net::close_tcp_stream(inbound).await?;

    Ok(())
}

/// Proxy header.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ProxyHeader {
    /// Do not add proxy header.
    None,

    /// Header for locally initiated connection.
    #[allow(unused)]
    Local,

    /// Header for proxied connection.
    Proxy,
}

impl ProxyHeader {
    /// Changes to `None` if `false` if given.
    ///
    /// `None` stays `None`.
    pub fn not_none(self, not_none: bool) -> Self {
        if not_none {
            self
        } else {
            Self::None
        }
    }
}

/// Get the proxy header for a locally initiated connection.
///
/// This header may be sent over the outbound stream to signal client information.
pub fn local_proxy_header() -> Result<BytesMut, EncodeError> {
    // Build proxy header
    let header = proxy_protocol::ProxyHeader::Version2 {
        command: ProxyCommand::Local,
        transport_protocol: ProxyTransportProtocol::Stream,
        addresses: ProxyAddresses::Unspec,
    };

    proxy_protocol::encode(header)
}

/// Get the proxy header for the given inbound stream.
///
/// This header may be sent over the outbound stream to signal client information.
pub fn stream_proxy_header(inbound: &TcpStream) -> Result<BytesMut, EncodeError> {
    // Get peer and local address
    let peer = inbound
        .peer_addr()
        .expect("Peer address not known for TCP stream");
    let local = inbound
        .local_addr()
        .expect("Local address not known for TCP stream");

    // Build proxy header
    let header = proxy_protocol::ProxyHeader::Version2 {
        command: ProxyCommand::Proxy,
        transport_protocol: ProxyTransportProtocol::Stream,
        addresses: match (peer, local) {
            (SocketAddr::V4(source), SocketAddr::V4(destination)) => ProxyAddresses::Ipv4 {
                source,
                destination,
            },
            (SocketAddr::V6(source), SocketAddr::V6(destination)) => ProxyAddresses::Ipv6 {
                source,
                destination,
            },
            (_, _) => unreachable!(),
        },
    };

    proxy_protocol::encode(header)
}
