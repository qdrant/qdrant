use tonic::transport::{Certificate, ClientTlsConfig, Identity, Endpoint};

use super::transport_channel_pool::ClientMutualTlsCertificates;

pub trait WithClientMutualTlsCertificates {
    fn with_client_mutual_tls_certificates(self, security_config_maybe: &Option<ClientMutualTlsCertificates>) -> Self;
}

impl WithClientMutualTlsCertificates for Endpoint {
    fn with_client_mutual_tls_certificates(self, client_mutual_tls_certificates: &Option<ClientMutualTlsCertificates>) -> Self {
        match client_mutual_tls_certificates {
            Some(certs) => {
                let identity = Identity::from_pem(&certs.client_certificate, &certs.client_certificate_key);
                let server_ca = Certificate::from_pem(&certs.server_ca_certificate);
                self
                    .tls_config(
                        ClientTlsConfig::new()
                            .identity(identity)
                            .ca_certificate(server_ca)
                    )
                    .expect("Failed to load certificates for client!")
            },
            None => self
        }
    }
}