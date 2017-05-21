#![feature(plugin)]
#![feature(custom_derive)]
#![feature(drop_types_in_const)]

extern crate raft;

extern crate log;
extern crate env_logger;

#[macro_use]
extern crate iron;
extern crate router;
extern crate params;
extern crate bodyparser;
extern crate iron_sessionstorage;

extern crate docopt;
extern crate bincode;
extern crate rustc_serialize;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate uuid;
extern crate toml;
extern crate base64;

#[macro_use]
extern crate lazy_static;

pub mod document;
//pub mod http_handler;
pub mod handler;
pub mod config;
pub mod doclog;
mod types;
mod statemachine;
mod parser;
mod login;

use std::net::SocketAddr;
use docopt::Docopt;

use raft::ServerId;
use std::collections::{HashSet, HashMap};

use std::fs::File;
use std::io::Read;

use uuid::Uuid;

use raft::Server;
use raft::LogId;
use raft::TransactionId;
use raft::state_machine::StateMachine;
use raft::EventLoop;
use raft::persistent_log::Log;

use statemachine::DocumentStateMachine;
use document::*;
use config::*;
use handler::Handler;
use doclog::DocLog;

use raft::auth::Auth;
use raft::auth::hasher::sha256::Sha256Hasher;
use raft::auth::credentials::Credentials;
use raft::auth::credentials::PlainCredentials;
use raft::auth::multi::{MultiAuth, MultiAuthBuilder};

use raft::TimeoutConfiguration;

//use http_handler::*;

type DB_CREDENTIAL = PlainCredentials;
type DB_HASHER = Sha256Hasher;

static USAGE: &'static str = "
A replicated document database.

Commands:

    get     Return document

    put     Set document

    server  Start server

Usage:
    document get <doc-id> <lid> <node-address> <username> <password>
    document put <doc-id> <lid> <node-address> <filepath> <username> <password>
    document post <lid> <node-address> <filepath> <username> <password>
    document remove <doc-id> <lid> <node-address> <username> <password>
    document server  <config-path>
    document begintrans <lid> <node-address> <username> <password>
    document commit <lid> <node-address> <username> <password> <transid>
    document rollback <lid> <node-address> <username> <password> <transid>
    document transpost <lid> <node-address> <filepath> <username> <password> <transid>
    document transremove <lid> <node-address> <doc-id> <username> <password> <transid>
    document transput <lid> <node-address> <doc-id> <filepath> <username> <password> <transid>
";

#[derive(Debug,RustcDecodable,Clone)]
struct Args {
    cmd_server: bool,
    cmd_get: bool,
    cmd_post: bool,
    cmd_remove: bool,
    cmd_put: bool,
    cmd_begintrans: bool,
    cmd_commit: bool,
    cmd_rollback: bool,
    cmd_transpost: bool,
    cmd_transremove: bool,
    cmd_transput: bool,
    arg_id: Option<u64>,
    arg_doc_id: Option<String>,
    arg_node_id: Vec<u64>,
    arg_node_address: Option<String>,
    arg_filepath: String,
    arg_config_path: Option<String>,
    arg_addr: Option<String>,
    arg_password: Option<String>,
    arg_username: Option<String>,
    arg_transid: Option<String>,
    arg_lid: Option<String>,
}

impl Args {
    pub fn get_doc_id(&self) -> Uuid {
        let doc_id = self.arg_doc_id.clone().unwrap();
        Uuid::parse_str(&doc_id).expect(&format!("{} is not a valid id", doc_id))
    }

    pub fn get_node_addr(&self) -> SocketAddr {
        self.arg_node_address
            .clone()
            .unwrap()
            .parse()
            .expect("Given IP is not valid")
    }

    pub fn get_lid(&self) -> LogId {
        LogId::from(&self.arg_lid.clone().unwrap()).expect("Given LogId is not valid")
    }

    pub fn get_trans_id(&self) -> TransactionId {
        let tid = self.arg_transid.clone().unwrap();
        TransactionId::from(&tid).expect(&format!("{} is not a valid transaction id", tid))
    }

    pub fn get_credentials(&self) -> DB_CREDENTIAL {
        use raft::auth::hasher::Hasher;

        let username = self.arg_username.clone().unwrap();
        let password = self.arg_password.clone().unwrap();

        DB_CREDENTIAL::new::<DB_HASHER>(&username, &password)
    }
}

fn main() {
    env_logger::init().unwrap();

    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.decode())
        .unwrap_or_else(|e| e.exit());

    if args.cmd_server {
        server(&args);
    } else {
        let mut handler = new_handler(&args);

        if args.cmd_get {
            let id = args.get_doc_id();

            let document = handler.get(id).unwrap();

            println!("{:?}", document);
        } else if args.cmd_post {
            let tid = TransactionId::new();

            let buffer = get_bytes(&args.arg_filepath);
            let document = Document::new(buffer);

            let id = handler.post(document, tid).unwrap();

            println!("{}", id);
        } else if args.cmd_remove {
            let id = args.get_doc_id();
            let tid = TransactionId::new();

            match handler.remove(id, tid) {
                Ok(()) => println!("ok"),
                Err(err) => panic!(err),
            }
        } else if args.cmd_put {
            let id = args.get_doc_id();
            let tid = TransactionId::new();

            let buffer = get_bytes(&args.arg_filepath);

            handler.put(id, buffer, tid).unwrap();

            println!("ok");
        } else if args.cmd_begintrans {
            let tid = TransactionId::new();
            let res = handler.begin_transaction(tid);

            println!("{}", res.unwrap());
        } else if args.cmd_commit {
            let tid = args.get_trans_id();

            let res = handler.commit_transaction(tid);
            println!("{}", res.unwrap());
        } else if args.cmd_rollback {
            let tid = args.get_trans_id();

            let res = handler.rollback_transaction(tid);

            println!("{}", res.unwrap());
        } else if args.cmd_transpost {
            let tid = args.get_trans_id();

            let buffer = get_bytes(&args.arg_filepath);

            let document = Document::new(buffer);

            let id = handler.post(document, tid).unwrap();

            println!("{}", id);
        } else if args.cmd_transremove {
            let id = args.get_doc_id();
            let tid = args.get_trans_id();

            let res = handler.remove(id, tid).unwrap();

            println!("ok");
        } else if args.cmd_transput {
            let id = args.get_doc_id();
            let tid = args.get_trans_id();

            let buffer = get_bytes(&args.arg_filepath);

            handler.put(id, buffer, tid).unwrap();

            println!("ok");
        }
    }
}

fn new_handler(args: &Args) -> Handler<DB_CREDENTIAL> {
    let credentials = args.get_credentials();
    let lid = args.get_lid();
    let mut node_addr = HashSet::new();
    node_addr.insert(args.get_node_addr());

    Handler::new(node_addr, lid, credentials)
}

fn get_bytes(filepath: &str) -> Vec<u8> {
    let mut fhandler = File::open(&filepath)
        .expect(&format!("Unable to open the file{}", filepath));
    let mut buffer: Vec<u8> = Vec::new();

    fhandler
        .read_to_end(&mut buffer)
        .expect(&format!("Unable read the file to end {}", filepath));

    buffer
}

fn server(args: &Args) {
    let arg_config_path = args.clone().arg_config_path.unwrap();
    let config_path = arg_config_path.as_str();
    let config = Config::init(&config_path).expect("Config is invalid");
    let server_addr = config.get_node_addr();
    let node_addr = match server_addr {
        SocketAddr::V4(v) => v,
        _ => panic!("The node address given must be IPv4"),
    };

    let peers = setup_peers(&config);
    let logs = setup_logs(&config);
    let auth = setup_auth(&config);

    let (mut server, mut event_loop) = Server::new(ServerId::from(config.server.node_id),
                                                   server_addr,
                                                   peers.clone(),
                                                   logs,
                                                   auth.clone(),
                                                   TimeoutConfiguration::default(),
                                                   129 as usize)
            .unwrap();


    setup_dynamic_peering(&config, &peers, &mut server, &mut event_loop);

    {
        let states = server.log_manager.get_states();
        let state_machines = server.log_manager.get_state_machines();
        let peers = server.log_manager.get_peers();

        //init(config.get_binding_addr(), node_addr, states, state_machines,peers, auth);
    }

    server.init(&mut event_loop);

    event_loop.run(&mut server).unwrap();
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

fn setup_dynamic_peering<L, M, A>(config: &Config,
                                  peers: &HashMap<ServerId, SocketAddr>,
                                  server: &mut Server<L, M, A>,
                                  event_loop: &mut EventLoop<Server<L, M, A>>)
    where L: Log,
          M: StateMachine,
          A: Auth
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
