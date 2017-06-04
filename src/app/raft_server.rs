#[allow(dead_code)]

use app::config::Config;
use raft::Server;
use raft::LogId;
use raft::TransactionId;
use raft::state_machine::StateMachine;
use raft::EventLoop;
use raft::persistent_log::Log;

use consensus::DocumentStateMachine;
use app::document::*;
use consensus::DocLog;
use raft::auth::Auth;
use raft::ServerId;

use raft::auth::hasher::sha256::Sha256Hasher;
use raft::auth::credentials::Credentials;
use raft::auth::credentials::PlainCredentials;
use raft::auth::multi::{MultiAuth, MultiAuthBuilder};

use std::collections::{HashSet, HashMap};
use std::net::SocketAddr;
use raft::TimeoutConfiguration;

pub type DB_CREDENTIAL = PlainCredentials;
pub type DB_HASHER = Sha256Hasher;

type L = DocLog;
type M = DocumentStateMachine;
type A = MultiAuth<DB_CREDENTIAL>;

pub struct ServerManager
{
    server: Server<L, M, A>,
    event_loop: EventLoop<Server<L, M, A>>
}

impl ServerManager
{
    pub fn configure(config: Config) -> Self
    {
        let peers = Self::setup_peers(&config);
        let logs = Self::setup_logs(&config);
        let auth = Self::setup_auth(&config);

        let server_addr = config.get_node_addr();

        let (mut server, mut event_loop) = Server::new(ServerId::from(config.server.node_id),
                                                       server_addr,
                                                       peers.clone(),
                                                       logs,
                                                       auth.clone(),
                                                       TimeoutConfiguration::default(),
                                                       129 as usize)
                .unwrap();


        Self::setup_dynamic_peering(&config, &peers, &mut server, &mut event_loop);

        {
            let states = server.log_manager.get_states();
            let state_machines = server.log_manager.get_state_machines();
            let peers = server.log_manager.get_peers();

            //init(config.get_binding_addr(), node_addr, states, state_machines,peers, auth);
        }


        server.init(&mut event_loop);

        ServerManager{
            server,
            event_loop
        }
    }

    pub fn run(&mut self) {
        self.event_loop.run(&mut self.server).unwrap();
    }

    fn setup_logs(config: &Config) -> Vec<(LogId, DocLog, DocumentStateMachine)> {
        config
            .logs
            .iter()
            .map(|l| {
                use std::path::Path;

                let mut state_machine = DocumentStateMachine::new(&Path::new(&l.path));
                {
                    let snapshot_map = state_machine.get_snapshot_map().unwrap_or_default();
                    state_machine.restore_snapshot(snapshot_map).unwrap();
                }

                let logid = LogId::from(&l.lid)
                    .expect(&format!("The logid given was invalid {:?}", l.lid));
                let log = DocLog::new(&Path::new(&l.path), LogId::from(&l.lid).unwrap());

                println!("Init {:?}", l.lid);

                (logid, log, state_machine)

            })
            .collect()
    }

    fn setup_peers(config: &Config) -> HashMap<ServerId, SocketAddr> {
        let (node_ids, node_addresses) = config.get_nodes();

        node_ids
            .iter()
            .zip(node_addresses.iter())
            .map(|(&id, addr)| (ServerId::from(id), *addr))
            .collect::<HashMap<_, _>>()
    }

    fn setup_auth(config: &Config) -> MultiAuth<DB_CREDENTIAL> {
        let mut builder = MultiAuth::<DB_CREDENTIAL>::build()
            .with_community_string(&config.server.community_string);

        let creds: Vec<(String, String)> = config
            .clone()
            .credentials
            .into_iter()
            .map(|cr| (cr.username, cr.password))
            .collect();

        for &(ref username, ref password) in creds.iter() {
            builder = builder.add_user::<DB_HASHER>(&username, &password);
        }

        let auth = builder.finalize();

        auth
    }

    fn setup_dynamic_peering(config: &Config,
                                      peers: &HashMap<ServerId, SocketAddr>,
                                      server: &mut Server<L, M, A>,
                                      event_loop: &mut EventLoop<Server<L, M, A>>)
    {
        if peers.is_empty() {
            match config.get_dynamic_peering() {
                Some((peer_id, peer_addr)) => {
                    server
                        .peering_request(event_loop, ServerId::from(peer_id), peer_addr)
                        .unwrap();
                }
                None => panic!("No peers or dynamic peering defined"),
            }
        }
    }
}
