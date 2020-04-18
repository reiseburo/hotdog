/**
 * This module handles the necessary configuration to serve over TLS
 */

use async_std::{
    io,
    io::BufReader,
    net::{TcpStream, TcpListener, ToSocketAddrs},
    prelude::*,
    sync::Arc,
    task,
};
use async_tls::TlsAcceptor;
use crate::read_logs;
use crate::settings::*;
use dipstick::*;
use log::*;
use rustls::internal::pemfile::{certs, rsa_private_keys};
use rustls::{Certificate, NoClientAuth, PrivateKey, ServerConfig};
use std::path::Path;


/// Load the passed certificates file
fn load_certs(path: &Path) -> io::Result<Vec<Certificate>> {
    debug!("Loading TLS certs from: {}", path.display());
    certs(&mut std::io::BufReader::new(std::fs::File::open(path)?))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid cert"))
}

/// Load the passed keys file
fn load_keys(path: &Path) -> io::Result<Vec<PrivateKey>> {
    debug!("Loading TLS keys from: {}", path.display());
    rsa_private_keys(&mut std::io::BufReader::new(std::fs::File::open(path)?))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid key"))
}

/// Configure the server using rusttls
/// See https://docs.rs/rustls/0.16.0/rustls/struct.ServerConfig.html for details
///
/// A TLS server needs a certificate and a fitting private key
fn load_tls_config(settings: &Settings) -> io::Result<ServerConfig> {
    match &settings.global.listen.tls {
        TlsType::CertAndKey { cert, key } => {
            let certs = load_certs(cert.as_path())?;
            let mut keys = load_keys(key.as_path())?;

            // we don't use client authentication
            let mut config = ServerConfig::new(NoClientAuth::new());
            config
                // set this server to use one cert together with the loaded private key
                .set_single_cert(certs, keys.remove(0))
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

            Ok(config)
        },
        _ => {
            panic!("Attempted to load a TLS configuration despite TLS not being enabled");
        }
    }
}

pub async fn accept_loop(addr: impl ToSocketAddrs,
    settings: Arc<Settings>,
    metrics: Arc<LockingOutput>) -> Result<()> {

    let config = load_tls_config(&settings)?;

    // We create one TLSAcceptor around a shared configuration.
    // Cloning the acceptor will not clone the configuration.
    let acceptor = TlsAcceptor::from(Arc::new(config));

    let listener = TcpListener::bind(&addr).await?;
    let mut incoming = listener.incoming();

    while let Some(stream) = incoming.next().await {
        // We use one acceptor per connection, so
        // we need to clone the current one.
        let acceptor = acceptor.clone();
        let mut stream = stream?;
        let settings = settings.clone();
        let metrics = metrics.clone();

        task::spawn(async move {
            handle_connection(&acceptor, &mut stream, settings, metrics).await;
        });
    }
    Ok(())
}

/// The connection handling function.
async fn handle_connection(acceptor: &TlsAcceptor,
    tcp_stream: &mut TcpStream,
    settings: Arc<Settings>,
    metrics: Arc<LockingOutput>) -> io::Result<()> {

    let peer_addr = tcp_stream.peer_addr()?;
    debug!("Accepted connection from: {}", peer_addr);

    // Calling `acceptor.accept` will start the TLS handshake
    let handshake = acceptor.accept(tcp_stream);
    // The handshake is a future we can await to get an encrypted
    // stream back.
    let tls_stream = handshake.await?;
    let reader = BufReader::new(tls_stream);

    read_logs(reader, settings, metrics).await;
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_certs() {
        let cert_path = Path::new("./contrib/cert.pem");
        if let Ok(certs) = load_certs(&cert_path) {
            assert_eq!(1, certs.len());
        }
        else {
            assert!(false);
        }
    }

    #[test]
    fn test_load_keys() {
        let key_path = Path::new("./contrib/cert-key.pem");
        if let Ok(keys) = load_keys(&key_path) {
            assert_eq!(1, keys.len());
        }
        else {
            assert!(false);
        }
    }
}
