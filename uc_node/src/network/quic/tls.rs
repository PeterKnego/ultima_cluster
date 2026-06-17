//! Self-signed cert generation and rustls client/server configs.
//!
//! M2 only supports `TlsConfig::SelfSigned`: generates a fresh cert at first
//! start, writes `tls.crt` + `tls.key` to data_dir, and uses an accept-anything
//! client verifier so peers can connect without a shared CA.

use std::path::Path;
use std::sync::Arc;
use std::sync::OnceLock;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, ServerConfig};

use super::super::NetworkError;

/// rustls 0.23 requires a process-wide default crypto provider. Install the
/// ring-backed provider exactly once, lazily, the first time we build a config.
fn ensure_crypto_provider() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        // Ignore the result: if another thread/library already installed a
        // provider, that's fine — we just need *a* provider available.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Generates a fresh self-signed cert + key. Returns (cert_pem, key_pem).
/// The cert's CN is "ultima_cluster" and SAN includes the app_id.
pub fn generate_self_signed(app_id: &str) -> Result<(String, String), NetworkError> {
    let mut params = rcgen::CertificateParams::new(vec![
        app_id.to_string(),
        "ultima_cluster".to_string(),
        "localhost".to_string(),
    ])
    .map_err(|e| NetworkError::Cert(format!("params: {e}")))?;
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "ultima_cluster");

    let key_pair =
        rcgen::KeyPair::generate().map_err(|e| NetworkError::Cert(format!("keygen: {e}")))?;
    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| NetworkError::Cert(format!("sign: {e}")))?;

    Ok((cert.pem(), key_pair.serialize_pem()))
}

/// Open or initialize cert/key files under `data_dir`.
/// Returns the DER-decoded cert and key.
pub fn load_or_init(
    data_dir: &Path,
    app_id: &str,
) -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>), NetworkError> {
    let cert_path = data_dir.join("tls.crt");
    let key_path = data_dir.join("tls.key");

    if !cert_path.exists() || !key_path.exists() {
        let (cert_pem, key_pem) = generate_self_signed(app_id)?;
        std::fs::write(&cert_path, &cert_pem)?;
        std::fs::write(&key_path, &key_pem)?;
    }

    let cert_pem = std::fs::read(&cert_path)?;
    let key_pem = std::fs::read(&key_path)?;

    let mut cert_reader = cert_pem.as_slice();
    let cert = rustls_pemfile::certs(&mut cert_reader)
        .next()
        .ok_or_else(|| NetworkError::Cert("no cert in tls.crt".into()))?
        .map_err(|e| NetworkError::Cert(format!("parse cert: {e}")))?;

    let mut key_reader = key_pem.as_slice();
    let key = rustls_pemfile::pkcs8_private_keys(&mut key_reader)
        .next()
        .ok_or_else(|| NetworkError::Cert("no pkcs8 key in tls.key".into()))?
        .map_err(|e| NetworkError::Cert(format!("parse key: {e}")))?;

    Ok((cert, PrivateKeyDer::Pkcs8(key)))
}

/// Build a rustls `ServerConfig` that presents our self-signed cert.
/// M2 uses no client auth — trust the QUIC handshake's encryption only.
pub fn build_server_config(
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
) -> Result<Arc<ServerConfig>, NetworkError> {
    ensure_crypto_provider();
    let cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .map_err(|e| NetworkError::Tls(format!("server config: {e}")))?;
    Ok(Arc::new(cfg))
}

/// Build a rustls `ClientConfig` that accepts any cert (we're using
/// self-signed certs that peers won't have in their trust store).
/// M5 production polish replaces this with a real CA path.
pub fn build_client_config() -> Result<Arc<ClientConfig>, NetworkError> {
    use rustls::DigitallySignedStruct;
    use rustls::SignatureScheme;
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::ServerName;

    ensure_crypto_provider();

    #[derive(Debug)]
    struct AcceptAnything;
    impl ServerCertVerifier for AcceptAnything {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: rustls::pki_types::UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }
        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            vec![
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::RSA_PKCS1_SHA384,
                SignatureScheme::RSA_PKCS1_SHA512,
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::ECDSA_NISTP384_SHA384,
                SignatureScheme::ECDSA_NISTP521_SHA512,
                SignatureScheme::ED25519,
                SignatureScheme::RSA_PSS_SHA256,
                SignatureScheme::RSA_PSS_SHA384,
                SignatureScheme::RSA_PSS_SHA512,
            ]
        }
    }

    let cfg = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnything))
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn generate_self_signed_succeeds() {
        let (cert, key) = generate_self_signed("test-app").expect("gen");
        assert!(cert.contains("BEGIN CERTIFICATE"));
        assert!(key.contains("BEGIN PRIVATE KEY"));
    }

    #[test]
    fn load_or_init_creates_files_first_time() {
        let dir = TempDir::new().unwrap();
        let (_cert, _key) = load_or_init(dir.path(), "test-app").expect("init");
        assert!(dir.path().join("tls.crt").exists());
        assert!(dir.path().join("tls.key").exists());
    }

    #[test]
    fn load_or_init_idempotent_on_second_call() {
        let dir = TempDir::new().unwrap();
        let (cert1_der, _key1_der) = load_or_init(dir.path(), "test-app").expect("init 1");
        let cert1_bytes = std::fs::read(dir.path().join("tls.crt")).unwrap();

        let (cert2_der, _key2_der) = load_or_init(dir.path(), "test-app").expect("init 2");
        let cert2_bytes = std::fs::read(dir.path().join("tls.crt")).unwrap();

        // Second call returns the same cert (didn't regenerate).
        assert_eq!(cert1_bytes, cert2_bytes);
        assert_eq!(cert1_der.as_ref(), cert2_der.as_ref());
    }
}
