use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};

fn check_port_available(port: u16) -> bool {
    let ipv4 = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
    TcpListener::bind(ipv4).is_ok()
}

pub fn validate_port_available(port: u16, server_name: &str) -> anyhow::Result<()> {
    if check_port_available(port) {
        Ok(())
    } else {
        let message = format!(
            "Could not start {} server because the port {} is already in use",
            server_name, port
        );
        log::error!("{}", message);
        Err(anyhow::anyhow!(message))
    }
}
