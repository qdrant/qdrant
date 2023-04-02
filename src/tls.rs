use crate::settings::P2pSecurityConfig;
use api::grpc::transport_channel_pool::ClientMutualTlsCertificates;
use tonic::transport::{ServerTlsConfig, Identity, Server, Certificate};



pub trait WithSecurity {
    fn with_security_config(self, security_config_maybe: Option<P2pSecurityConfig>) -> Self;
}

impl WithSecurity for Server {
    fn with_security_config(self, security_config_maybe: Option<P2pSecurityConfig>) -> Server {
        if let Some(security_config) = security_config_maybe {
            self.tls_config(create_server_tls_config(security_config)).expect("P2P TLS configuration failed")
        } else {
            self
        }
    }
}

pub fn to_client_certificates(
    security_config_maybe: &Option<P2pSecurityConfig>
) -> Option<ClientMutualTlsCertificates> {
    if let Some(cfg) = security_config_maybe {
        Some(ClientMutualTlsCertificates {
            client_certificate: read_certificate(&cfg.client_certificate_path),
            client_certificate_key: read_certificate(&cfg.client_certificate_key_path),
            server_ca_certificate: read_certificate(&cfg.ca_certificate_path)
        })
    } else {
        None
    }
}

fn create_server_tls_config(security_config: P2pSecurityConfig) -> ServerTlsConfig {
    let identity = Identity::from_pem(
        &read_certificate(&security_config.server_certificate_path), 
        &read_certificate(&security_config.server_certificate_key_path)
    );
    let client_ca = Certificate::from_pem(&read_certificate(&security_config.ca_certificate_path));

    ServerTlsConfig::new().identity(identity).client_ca_root(client_ca)
}

fn read_certificate(path: &String) -> String {
    std::fs::read_to_string(path)
        .expect("Cannot read certificate file!")
}
