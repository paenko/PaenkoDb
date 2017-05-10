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
pub mod http_handler;
pub mod handler;
pub mod config;
pub mod doclog;
mod statemachine;
mod parser;
mod login;

use std::net::SocketAddr;
use docopt::Docopt;

use raft::ServerId;
use std::collections::HashMap;

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

use raft::auth::sha256::Sha256Auth;
use raft::auth::credentials::SingleCredentials;

use http_handler::*;

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
}

fn main() {

    env_logger::init().unwrap();

    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.decode())
        .unwrap_or_else(|e| e.exit());

    if args.cmd_server {
        server(&args);
    } else {
        let username = args.arg_username.clone().unwrap();
        let password = args.arg_password.clone().unwrap();
        let lid = args.get_lid();
        let node_addr = args.get_node_addr();

        if args.cmd_get {
            let id = args.get_doc_id();

            get(&node_addr, id, &username, &password, lid);

        } else if args.cmd_post {

            post(&node_addr,
                 &args.arg_filepath,
                 &username,
                 &password,
                 TransactionId::new(),
                 lid);
        } else if args.cmd_remove {
            let id = args.get_doc_id();

            remove(&node_addr, id, &username, &password, TransactionId::new(), lid);
        } else if args.cmd_put {
            let id = args.get_doc_id();

            put(&node_addr,
                id,
                &args.arg_filepath,
                &username,
                &password,
                TransactionId::new(),
                lid);
        } else if args.cmd_begintrans {
            let res =
                Handler::begin_transaction(&node_addr, &username, &password, TransactionId::new(), lid);

            println!("{}", res.unwrap());
        } else if args.cmd_commit{
            let tid = args.get_trans_id();
            let res = Handler::commit_transaction(&node_addr, &username, &password, lid, tid);
            println!("{}", res.unwrap());
        } else if args.cmd_rollback {
            let tid = args.get_trans_id();
            let res = Handler::rollback_transaction(&node_addr, &username, &password, lid,tid);

            println!("{}", res.unwrap());
        } else if args.cmd_transpost {
            let tid = args.get_trans_id();

            post(&node_addr,
                 &args.arg_filepath,
                 &username,
                 &password,
                 tid,
                 lid);

        } else if args.cmd_transremove {
            let id = args.get_doc_id();
            let tid = args.get_trans_id();

            remove(&node_addr, id, &username, &password, tid, lid);
        } else if args.cmd_transput {
            let id = args.get_doc_id();
            let tid = args.get_trans_id();

            put(&node_addr,
                id,
                &args.arg_filepath,
                &username,
                &password,
                tid,
                lid);
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
        let mut state_machine = DocumentStateMachine::new(&l.path);
        {
            let snap_map = state_machine.get_snapshot_map().unwrap_or_default();
            let snap_log = state_machine.get_snapshot_log().unwrap_or_default();

            state_machine.restore_snapshot(snap_map, snap_log);
        }
        let logid = LogId::from(&l.lid).expect(&format!("The logid given was invalid {:?}", l.lid));
        let log = DocLog::new(&l.path, LogId::from(&l.lid).unwrap());
        logs.push((logid, log, state_machine));
        println!("Init {:?}", l.lid);
    }

    let credentials = SingleCredentials::new(config.security.username.clone(), config.security.password.clone());
    let auth = Sha256Auth::new(credentials);

    let (mut server, mut event_loop) = Server::new(ServerId::from(config.server.node_id),
                                                   server_addr,
                                                   &peers,
                                                   config.server.community_string.to_string(),
                                                   auth.clone(),
                                                   logs)
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

        init(config.get_binding_addr(), node_addr, states, state_machines,peers, auth);
    }

    server.init(&mut event_loop);

    event_loop.run(&mut server).unwrap();
}

fn get(addr: &SocketAddr, doc_id: Uuid, username: &str, password: &str, lid: LogId) {
    let document = Handler::get(addr, &username, &password, doc_id, lid);
    println!("{:?}", document);
}

fn post(addr: &SocketAddr,
        filepath: &str,
        username: &str,
        password: &str,
        session: TransactionId,
        lid: LogId) {

    let mut handler = File::open(&filepath).expect(&format!("Unable to open the file{}", filepath));
    let mut buffer: Vec<u8> = Vec::new();

    handler.read_to_end(&mut buffer).expect(&format!("Unable read the file to end {}", filepath));

    let document = Document {
        id: Uuid::new_v4(),
        payload: buffer,
        version: 1,
    };

    let id = match Handler::post(addr, &username, &password, document, session, lid) {
        Ok(id) => id,
        Err(err) => panic!(err),
    };

    println!("{}", id);
}

fn put(addr: &SocketAddr,
       doc_id: Uuid,
       filepath: &str,
       username: &str,
       password: &str,
       session: TransactionId,
       lid: LogId) {

    let mut handler = File::open(filepath).expect(&format!("Unable to open the file{}", filepath));
    let mut buffer: Vec<u8> = Vec::new();

    handler.read_to_end(&mut buffer).expect(&format!("Unable read the file to end {}", filepath));

    match Handler::put(addr, &username, &password, doc_id, buffer, session, lid) {
        Ok(()) => {

        }
        Err(err) => panic!(err),
    }
}

fn remove(addr: &SocketAddr,
          doc_id: Uuid,
          username: &str,
          password: &str,
          session: TransactionId,
          lid: LogId) {
    match Handler::remove(addr, &username, &password, doc_id, session, lid) {
        Ok(()) => println!("Ok"),
        Err(err) => panic!(err),
    }
}
