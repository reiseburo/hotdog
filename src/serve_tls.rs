use crate::serve::*;
use crate::settings::*;
/**
 * This module handles the necessary configuration to serve over TLS
 */
use async_std::{io, io::BufReader, net::TcpStream, prelude::*, sync::Arc, task};
use async_tls::TlsAcceptor;
use log::*;
use rustls::internal::pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use rustls::{
    AllowAnyAnonymousOrAuthenticatedClient, Certificate, NoClientAuth, PrivateKey, RootCertStore,
    ServerConfig,
};
use std::path::Path;

use crate::connection::*;
use crate::serve::*;

/**
 * TlsServer is a syslog-over-TLS implementation, which will allow for receiving logs over a TLS
 * encrypted channel.
 *
 * Currently client authentication is not supported
 */
pub struct TlsServer {
    acceptor: TlsAcceptor,
}

impl TlsServer {
    pub fn new(state: &ServerState) -> Self {
        let config =
            load_tls_config(state).expect("Failed to generate the TLS ServerConfig properly");
        let acceptor = TlsAcceptor::from(Arc::new(config));
        TlsServer { acceptor }
    }
}

impl Server for TlsServer {
    fn bootstrap(&mut self, _state: &ServerState) -> Result<(), ServerError> {
        Ok(())
    }

    fn handle_connection(
        &self,
        stream: TcpStream,
        connection: Connection,
    ) -> Result<(), std::io::Error> {
        debug!("Accepting from: {}", stream.peer_addr()?);

        // Calling `acceptor.accept` will start the TLS handshake
        let handshake = self.acceptor.accept(stream);

        task::spawn(async move {
            // The handshake is a future we can await to get an encrypted
            // stream back.
            if let Ok(tls_stream) = handshake.await {
                let reader = BufReader::new(tls_stream);
                connection.read_logs(reader).await;
            } else {
                error!("Unable to establish a TLS Stream for client!");
            }
        });
        Ok(())
    }
}

/**
 * Generate the default ServerConfig needed for rustls to work properly in server mode
 */
fn load_tls_config(state: &ServerState) -> io::Result<ServerConfig> {
    match &state.settings.global.listen.tls {
        TlsType::CertAndKey { cert, key, ca } => {
            let certs = load_certs(cert.as_path())?;
            let mut keys = load_keys(key.as_path())?;

            if keys.is_empty() {
                panic!("TLS key could not be properly loaded! This is fatal!");
            }

            let verifier = if ca.is_some() {
                let ca_path = ca.as_ref().unwrap();
                let mut store = RootCertStore::empty();
                if let Err(e) = store.add_pem_file(&mut std::io::BufReader::new(
                    std::fs::File::open(ca_path.as_path())?,
                )) {
                    error!("Failed to add the CA properly, certificate verification may not work as expected: {:?}", e);
                }
                AllowAnyAnonymousOrAuthenticatedClient::new(store)
            } else {
                NoClientAuth::new()
            };

            // we don't use client authentication
            let mut config = ServerConfig::new(verifier);
            config
                // set this server to use one cert together with the loaded private key
                .set_single_cert(certs, keys.remove(0))
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

            Ok(config)
        }
        _ => {
            panic!("Attempted to load a TLS configuration despite TLS not being enabled");
        }
    }
}

/// Load the passed certificates file
fn load_certs(path: &Path) -> io::Result<Vec<Certificate>> {
    debug!("Loading TLS certs from: {}", path.display());
    certs(&mut std::io::BufReader::new(std::fs::File::open(path)?))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid cert"))
}

/**
 * Loads the keys file passed in, whether it is an RSA or PKCS8 formatted key
 */
fn load_keys(path: &Path) -> io::Result<Vec<PrivateKey>> {
    debug!("Loading TLS keys from: {}", path.display());

    let result = rsa_private_keys(&mut std::io::BufReader::new(std::fs::File::open(path)?))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid key"));

    if let Ok(keys) = result {
        if keys.is_empty() {
            debug!("Failed to load key as RSA, trying PKCS8");
            return pkcs8_private_keys(&mut std::io::BufReader::new(std::fs::File::open(path)?))
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid key"));
        }
        return Ok(keys);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_certs() {
        let cert_path = Path::new("./contrib/cert.pem");
        if let Ok(certs) = load_certs(&cert_path) {
            assert_eq!(1, certs.len());
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_load_keys_rsa() {
        let key_path = Path::new("./contrib/cert-key.pem");
        if let Ok(keys) = load_keys(&key_path) {
            assert_eq!(1, keys.len());
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_load_keys_pkcs8() {
        let key_path = Path::new("./contrib/pkcs8-key.pem");
        if let Ok(keys) = load_keys(&key_path) {
            assert_eq!(1, keys.len());
        } else {
            assert!(false);
        }
    }
}
