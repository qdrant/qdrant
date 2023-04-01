use crate::settings::P2pSecurityConfig;
use tonic::transport::{ServerTlsConfig, Identity, Server};

fn create_server_tls_config(security_config: P2pSecurityConfig) -> ServerTlsConfig {
    let cert = std::fs::read_to_string(security_config.ssl_certificate_path)
        .expect("Cannot read certificate file!");
    let key = std::fs::read_to_string(security_config.ssl_certificate_key_path)
        .expect("Cannot read certificate key fiel!");
    ServerTlsConfig::new().identity(Identity::from_pem(&cert, &key))
}

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
