use std::fs::File;
use std::io::Read;
use std::net::{SocketAddr, ToSocketAddrs};
use uuid::Uuid;
use parser::Parser;
use parser::toml::Parser as tParser;
use toml::DecodeError;
use raft::ServerId;

#[derive(Debug,Deserialize,Clone)]
/// Contains the configuration of config.toml
pub struct Config {
    pub server: ServerConfig,
    pub peers: Vec<PeerConfig>,
    pub logs: Vec<LogConfig>,
    pub dynamic_peer: Option<DynamicPeer>,
    pub security: SecurityConfig,
}

#[derive(Debug,Deserialize,Clone)]
/// The specific server configuration
pub struct ServerConfig {
    pub node_id: u64,
    pub node_address: String,
    pub community_string: String,
    pub binding_addr: String,
}

#[derive(Debug,Deserialize,Clone)]
/// Configuration for dynamic peer adding
pub struct DynamicPeer {
    pub node_id: u64,
    pub node_address: String,
}

#[derive(Debug,Deserialize,Clone)]
/// Configuration for a single peer
pub struct PeerConfig {
    pub node_id: u64,
    pub node_address: String,
}

#[derive(Debug,Deserialize,Clone)]
/// Configuration for a single log
pub struct LogConfig {
    pub path: String,
    pub lid: String,
}

#[derive(Debug,Deserialize,Clone)]
/// Configuration for Client-Security
pub struct SecurityConfig {
    pub username: String,
    pub password: String,
}

impl Config {
    pub fn init(file: &str) -> Result<Self, DecodeError> {
        let mut config_file = File::open(file)
            .expect(&format!("Unable to read the config {}", file));

        let mut config = String::new();
        config_file.read_to_string(&mut config).expect("Unable to read the config");

        tParser::parse(&config)
    }

    pub fn get_node_addr(&self) -> SocketAddr {
        let server: Vec<SocketAddr> = self.server.node_address.to_socket_addrs().unwrap().collect();
        server[0]
    }

    pub fn get_binding_addr(&self) -> SocketAddr {
        self.server.binding_addr.parse().expect("Binding address is invalid")
    }

    pub fn get_dynamic_peering(&self) -> Option<(ServerId, SocketAddr)> {
        match self.dynamic_peer {
            Some(ref dpeer) => {
                let id = dpeer.node_id;
                let addr = &dpeer.node_address;

                let server: Vec<SocketAddr> = addr.to_socket_addrs().unwrap().collect();

                Some((ServerId::from(id), server[0]))
            }
            None => None,
        }
    }

    pub fn get_peers_id(&self) -> Vec<u64> {
        self.peers.iter().map(|x| x.node_id).collect()
    }

    pub fn get_nodes(&self) -> (Vec<u64>, Vec<SocketAddr>) {
        let mut node_ids: Vec<u64> = Vec::new();
        let mut node_addresses: Vec<SocketAddr> = Vec::new();

        for peer in self.peers.clone() {
            node_ids.push(peer.node_id);

            let addr: Vec<SocketAddr> = peer.node_address.to_socket_addrs().unwrap().collect();

            node_addresses.push(addr[0]);
        }

        (node_ids, node_addresses)
    }
}

impl PeerConfig {
    pub fn get_node_addr(&self) -> SocketAddr {
        let addr: Vec<SocketAddr> = self.node_address
            .to_socket_addrs()
            .unwrap()
            .collect();

        addr[0]
    }
}

impl LogConfig {
    pub fn get_log_id(&self) -> Uuid {
        self.lid.parse().expect("LogId of a log is invalid")
    }
}
