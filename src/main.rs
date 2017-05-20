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

use statemachine::DocumentStateMachine;
use document::*;
use config::*;
use handler::Handler;
use doclog::DocLog;

use raft::auth::hasher::sha256::Sha256Hasher;
use raft::auth::credentials::Credentials;
use raft::auth::credentials::SingleCredentials;
use raft::auth::multi::{MultiAuth, MultiAuthBuilder};

use raft::TimeoutConfiguration;

//use http_handler::*;

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
        self.arg_node_address.clone().unwrap().parse().expect("Given IP is not valid")
    }

    pub fn get_lid(&self) -> LogId {
        LogId::from(&self.arg_lid.clone().unwrap()).expect("Given LogId is not valid")
    }

    pub fn get_trans_id(&self) -> TransactionId{
        let tid = self.arg_transid.clone().unwrap();
        TransactionId::from(&tid).expect(&format!("{} is not a valid transaction id", tid))
    }

    pub fn get_credentials(&self) -> SingleCredentials{
        use raft::auth::hasher::Hasher;

        let username = self.arg_username.clone().unwrap();
        let password = self.arg_password.clone().unwrap();

        SingleCredentials::new(&username, &Sha256Hasher::hash(&password))
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

        let mut handler = {
            let credentials = args.get_credentials();
            let lid = args.get_lid();
            let mut node_addr = HashSet::new();
            node_addr.insert(args.get_node_addr());

            Handler::new(node_addr, lid, credentials)
        };

        if args.cmd_get {
            let id = args.get_doc_id();

            let document = handler.get(id).unwrap();

            println!("{:?}", document);
        } else if args.cmd_post {
            let filepath = &args.arg_filepath;
            let tid = TransactionId::new();

            let mut fhandler = File::open(&filepath).expect(&format!("Unable to open the file{}", filepath));
            let mut buffer: Vec<u8> = Vec::new();

            fhandler.read_to_end(&mut buffer).expect(&format!("Unable read the file to end {}", filepath));

            let document = Document::new(buffer);

            let id = handler.post(document, tid).unwrap();

            println!("{}",id);
        } else if args.cmd_remove {
            let id = args.get_doc_id();
            let tid = TransactionId::new();

            match handler.remove(id, tid){
                Ok(()) => println!("ok"),
                Err(err) => panic!(err)
            }
        } else if args.cmd_put {
            let id = args.get_doc_id();
            let tid = TransactionId::new();

            let filepath = &args.arg_filepath;

            let mut fhandler = File::open(filepath).expect(&format!("Unable to open the file{}", filepath));
            let mut buffer: Vec<u8> = Vec::new();

            fhandler.read_to_end(&mut buffer).expect(&format!("Unable read the file to end {}", filepath));

            match handler.put(id, buffer, tid) {
                Ok(()) => println!("ok"),
                Err(err) => panic!(err)
            }
        } else if args.cmd_begintrans {
            let tid = TransactionId::new();
            let res = handler.begin_transaction(tid);

            println!("{}", res.unwrap());
        } else if args.cmd_commit{
            let tid = args.get_trans_id();

            let res = handler.commit_transaction(tid);
            println!("{}", res.unwrap());
        } else if args.cmd_rollback {
            let tid = args.get_trans_id();

            let res = handler.rollback_transaction(tid);

            println!("{}", res.unwrap());
        } else if args.cmd_transpost {
            let filepath = &args.arg_filepath;
            let tid = args.get_trans_id();

            let mut fhandler = File::open(&filepath).expect(&format!("Unable to open the file{}", filepath));
            let mut buffer: Vec<u8> = Vec::new();

            fhandler.read_to_end(&mut buffer).expect(&format!("Unable read the file to end {}", filepath));

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

            let filepath = &args.arg_filepath;

            let mut fhandler = File::open(filepath).expect(&format!("Unable to open the file{}", filepath));
            let mut buffer: Vec<u8> = Vec::new();

            fhandler.read_to_end(&mut buffer).expect(&format!("Unable read the file to end {}", filepath));

            match handler.put(id, buffer,tid) {
                Ok(()) => println!("ok"),
                Err(err) => panic!(err)
            }
        }
    }
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

    let (node_ids, node_addresses) = config.get_nodes();

    let peers = node_ids.iter()
        .zip(node_addresses.iter())
        .map(|(&id, addr)| (ServerId::from(id), *addr))
        .collect::<HashMap<_, _>>();

    let mut logs: Vec<(LogId, DocLog, DocumentStateMachine)> = Vec::new();

    for l in &config.logs {
        use std::path::Path;

        let mut state_machine = DocumentStateMachine::new(&Path::new(&l.path));
        {
            let snap_map = state_machine.get_snapshot_map().unwrap_or_default();
        //    let snap_log = state_machine.get_snapshot_log().unwrap_or_default();

            state_machine.restore_snapshot(snap_map);
        }
        let logid = LogId::from(&l.lid).expect(&format!("The logid given was invalid {:?}", l.lid));
        let log = DocLog::new(&Path::new(&l.path), LogId::from(&l.lid).unwrap());
        logs.push((logid, log, state_machine));
        println!("Init {:?}", l.lid);
    }

    let mut builder = MultiAuth::<SingleCredentials>::build()
        .with_community_string(&config.server.community_string);

    for &(ref username, ref password) in config.security.get_credentials().iter() {
        builder = builder
            .add_user_hashed(username, password);
    }


    let auth = builder.finalize();

    let (mut server, mut event_loop) = Server::new(ServerId::from(config.server.node_id),
                                                   server_addr,
                                                   peers.clone(),
                                                   logs,
                                                   auth.clone(),
                                                   TimeoutConfiguration::default(),
                                                    129 as usize)
        .unwrap();

    {
        if peers.is_empty() {
            match config.get_dynamic_peering() {
                Some((peer_id, peer_addr)) => {
                    server.peering_request(&mut event_loop, ServerId::from(peer_id), peer_addr).unwrap();
                }
                None => panic!("No peers or dynamic peering defined"),
            }
        }
    }
    {
        let states = server.log_manager.get_states();
        let state_machines = server.log_manager.get_state_machines();
        let peers = server.log_manager.get_peers();

        //init(config.get_binding_addr(), node_addr, states, state_machines,peers, auth);
    }

    server.init(&mut event_loop);

    event_loop.run(&mut server).unwrap();
}
