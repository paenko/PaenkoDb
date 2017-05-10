#![allow(non_camel_case_types)]

use iron::status;
use router::Router;
use iron::prelude::*;
use bodyparser;

use iron_sessionstorage::traits::*;
use iron_sessionstorage::SessionStorage;
use iron_sessionstorage::backends::SignedCookieBackend;

use uuid::Uuid;
use std::net::{SocketAddr,  SocketAddrV4};

use document::*;
use handler::Handler;
use statemachine::DocumentStateMachine;

use std::thread::spawn;

use raft::LogId;
use raft::ServerId;
use raft::TransactionId;
use raft::state::{LeaderState, CandidateState, FollowerState};
use raft::auth::Auth;
use raft::auth::sha256::Sha256Auth;
use raft::auth::credentials::SingleCredentials;
use raft::StateInformation;

use login::Login;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use rustc_serialize::base64::{ToBase64, FromBase64, STANDARD};
use serde_json;
use serde_json::to_string as to_json;


#[derive(Deserialize,Serialize)]
struct http_Response {
    payload: String,
    version: usize,
}

#[derive(Clone,Copy)]
struct Context {
    node_addr: SocketAddrV4,
}

pub fn init(binding_addr: SocketAddr,
            node_addr: SocketAddrV4,
            states: HashMap<LogId,StateInformation>,
            state_machines: HashMap<LogId, Arc<RwLock<DocumentStateMachine>>>,
            peers: Arc<RwLock<HashMap<ServerId, SocketAddr>>>,
            auth: Sha256Auth<SingleCredentials>) {
    let mut router = Router::new();

    let states = Arc::new(states);
    let state_machines = Arc::new(state_machines);
    let context = Context { node_addr: node_addr };
    let auth = Arc::new(auth);

    router.get("/auth/login",
               move |request: &mut Request| http_display_login(request),
               "get_login");

    {
        router.post("/auth/login",
                    move |request: &mut Request| http_login(request, auth.clone()),
                    "login");
    }
    router.post("/auth/logout",
                move |request: &mut Request| http_logout(request),
                "logout");

    router.get("/document/:lid/:id",
               move |request: &mut Request| http_get(request, &context),
               "get_document");
    router.post("/document/:lid",
                move |request: &mut Request| http_post(request, &context),
                "post_document");
    router.post("/document/:lid/transaction/:session",
                move |request: &mut Request| http_trans_post(request, &context),
                "post_trans_document");
    router.delete("/document/:lid/:id",
                  move |request: &mut Request| http_delete(request, &context),
                  "delete_document");
    router.delete("/document/:lid/:id/transaction/:session",
                  move |request: &mut Request| http_trans_delete(request, &context),
                  "delete_trans_document");
    router.put("/document/:lid/document/:id",
               move |request: &mut Request| http_put(request, &context),
               "put_document");
    router.put("/document/:lid/transaction/:session/document/:id",
               move |request: &mut Request| http_trans_put(request, &context),
               "put_trans_document");
    router.post("/transaction/begin/:lid",
                move |request: &mut Request| http_begin_transaction(request, &context),
                "begin_transaction");

    router.post("/transaction/commit/:lid/:session",
                move |request: &mut Request| http_commit_transaction(request, &context),
                "commit_transaction");

    router.post("/transaction/rollback/:lid/:session",
                move |request: &mut Request| http_rollback_transaction(request, &context),
                "rollback_transaction");

    {
        router.get("/meta/log/:lid/documents",
                   move |request: &mut Request| {
                       http_get_documents(request, &context, state_machines.clone())
                   },
                   "get_document_keys");

    }
    {
        let states = states.clone();
        router.get("/meta/logs",
                   move |request: &mut Request| http_logs(request, &context, states.clone()),
                   "meta_logs");
    }
    {
        let states = states.clone();
        router.get("/meta/:lid/state/leader",
                   move |request: &mut Request| {
                       http_meta_state_leader(request, &context, states.clone())
                   },
                   "meta_state_leader");
    }
    {
        let states = states.clone();
        router.get("/meta/:lid/state/candidate",
                   move |request: &mut Request| {
                       http_meta_state_candidate(request, &context, states.clone())
                   },
                   "meta_state_candidate");
    }
    {
        router.get("/meta/:lid/state/follower",
                   move |request: &mut Request| {
                       http_meta_state_follower(request, &context, states.clone())
                   },
                   "meta_state_follower");
    }
    {
        router.get("/meta/peers",
                   move |request: &mut Request| {
                       http_meta_peers(request, &context, peers.clone())
                   },
                   "meta_peers");
    }

    // TODO implement user & password
    fn http_get_documents(req: &mut Request,
                          _: &Context,
                          state_machines: Arc<HashMap<LogId, Arc<RwLock<DocumentStateMachine>>>>)
                          -> IronResult<Response> {
        let raw_lid = iexpect!(req.extensions.get::<Router>().unwrap().find("lid"),
                               (status::BadRequest, "No lid found"));
        let lid = itry!(LogId::from(raw_lid),
                        (status::BadRequest, "LogId is invalid"));

        let state_machine = iexpect!(state_machines.get(&lid),
                                     (status::BadRequest, "No log found"))
            .read()
            .unwrap();

        let documents = state_machine.get_documents();

        Ok(Response::with((status::Ok,
                           format!("{:?}",
                                   documents.iter()
                                       .map(|d| d.simple().to_string())
                                       .collect::<Vec<_>>()))))
    }

    // TODO implement user & password
    fn http_logs(_: &mut Request,
                 _: &Context,
                 state: Arc<HashMap<LogId,
                                    (Arc<RwLock<LeaderState>>,
                                     Arc<RwLock<CandidateState>>,
                                     Arc<RwLock<FollowerState>>)>>)
                 -> IronResult<Response> {
        let keys = state.keys();

        let mut logs = String::new();

        for k in keys {
            logs.push('\n');
            logs.push_str(&format!("{}", k));
        }

        Ok(Response::with((status::Ok, format!("{}", &logs))))
    }

    fn http_meta_state_leader(req: &mut Request,
                              _: &Context,
                              state: Arc<HashMap<LogId,
                                                 (Arc<RwLock<LeaderState>>,
                                                  Arc<RwLock<CandidateState>>,
                                                  Arc<RwLock<FollowerState>>)>>)
                              -> IronResult<Response> {

        let raw_lid = iexpect!(req.extensions.get::<Router>().unwrap().find("lid"),
                               (status::BadRequest, "No lid found"));
        let lid = itry!(LogId::from(raw_lid),
                        (status::BadRequest, "LogId is invalid"));

        let lock = state.get(&lid).unwrap().0.read().expect("Could not lock state");

        let ref lock = *lock;

        let json = to_json(&lock.clone()).expect("Cannot encode json");

        Ok(Response::with((status::Ok, format!("{}", json))))
    }

    fn http_meta_state_candidate(req: &mut Request,
                                 _: &Context,
                                 state: Arc<HashMap<LogId,
                                                    (Arc<RwLock<LeaderState>>,
                                                     Arc<RwLock<CandidateState>>,
                                                     Arc<RwLock<FollowerState>>)>>)
                                 -> IronResult<Response> {

        let raw_lid = iexpect!(req.extensions.get::<Router>().unwrap().find("lid"),
                               (status::BadRequest, "Cannot find logid"));
        let lid = itry!(LogId::from(raw_lid), (status::BadRequest, "Invalid logid"));
        let lock = state.get(&lid).unwrap().1.read().expect("Could not lock state");

        Ok(Response::with((status::Ok, format!("{}", to_json(&*lock).unwrap()))))
    }

    fn http_meta_state_follower(req: &mut Request,
                                _: &Context,
                                state: Arc<HashMap<LogId,
                                                   (Arc<RwLock<LeaderState>>,
                                                    Arc<RwLock<CandidateState>>,
                                                    Arc<RwLock<FollowerState>>)>>)
                                -> IronResult<Response> {
        let raw_lid = iexpect!(req.extensions.get::<Router>().unwrap().find("lid"),
                               (status::BadRequest, "Cannot find logid"));
        let lid = itry!(LogId::from(raw_lid), (status::BadRequest, "Invalid logid"));
        let lock = state.get(&lid).unwrap().2.read().expect("Could not lock state");

        Ok(Response::with((status::Ok, format!("{}", to_json(&*lock).unwrap()))))
    }

    fn http_meta_peers(_: &mut Request,
                       _: &Context,
                       peers: Arc<RwLock<HashMap<ServerId, SocketAddr>>>)
                       -> IronResult<Response> {
        let lock = peers.read().unwrap();

        Ok(Response::with((status::Ok, format!("{}", to_json(&*lock).unwrap()))))
    }

    spawn(move || {
        let my_secret = b"verysecret".to_vec();
        let mut ch = Chain::new(router);
        ch.link_around(SessionStorage::new(SignedCookieBackend::new(my_secret)));
        Iron::new(ch).http(binding_addr).unwrap();
    });

    // TODO change to generic
    fn http_login(req: &mut Request,
                  auth: Arc<Sha256Auth<SingleCredentials>>)
                  -> IronResult<Response> {
        let username = {
            let ref body = req.get::<bodyparser::Json>().unwrap().unwrap();
            let ref p = iexpect!(body.find("username"));

            p.as_str().unwrap().to_string()
        };

        let password = {
            let ref body = req.get::<bodyparser::Json>().unwrap().unwrap();
            let ref p = iexpect!(body.find("password"));

            p.as_str().unwrap().to_string()
        };

        let hash_password = auth.hash(&password);

        match auth.find(&username, &hash_password) { 
            true => {
                let login = Login::new(username, hash_password);
                try!(req.session().set(login));

                Ok(Response::with(status::Ok))
            }
            false => Ok(Response::with((status::Unauthorized))),
        }
    }

    fn http_logout(req: &mut Request) -> IronResult<Response> {
        try!(req.session().clear());
        Ok(Response::with(status::Ok))
    }

    fn http_display_login(req: &mut Request) -> IronResult<Response> {
        let session = try!(req.session().get::<Login>());

        match session {
            Some(account) => {
                Ok(Response::with((status::Ok, format!("Logged as {}", account.username))))
            }
            None => Ok(Response::with((status::Unauthorized, "Not logged"))),
        }

    }

    // TODO implement user & password
    fn http_get(req: &mut Request, context: &Context) -> IronResult<Response> {
        let session = iexpect!(try!(req.session().get::<Login>()));

        let ref username = session.username;
        let ref password = session.hashed_password;

        let ref id = iexpect!(req.extensions
            .get::<Router>()
            .unwrap()
            .find("id"));

        let ref lid = iexpect!(req.extensions.get::<Router>().unwrap().find("lid"),
                               (status::BadRequest, "Cannot find logid"));

        match Handler::get(&SocketAddr::V4(context.node_addr),
                           &username,
                           &password,
                           Uuid::parse_str(*id).unwrap(),
                           LogId::from(*lid).unwrap()) {
            Ok(document) => {
                let http_doc = http_Response {
                    version: document.version,
                    payload: document.payload.as_slice().to_base64(STANDARD),
                };

                let encoded = itry!(to_json(&http_doc), "Cannot encode document to json");

                Ok(Response::with((status::Ok, encoded)))
            }
            Err(ref error) => {
                Ok(Response::with((status::InternalServerError, format!("{}", error))))
            }
        }
    }

    fn http_post(req: &mut Request, context: &Context) -> IronResult<Response> {
        let payload = {
            let ref body = req.get::<bodyparser::Json>().unwrap().unwrap();

            let p = iexpect!(body.find("payload"),
                             (status::BadRequest, "No payload was in the body defined"));

            let str_payload = match *p {
                serde_json::Value::String(ref load) => load,
                _ => panic!("Unexpected payload type"),
            };

            str_payload.from_base64().expect("Payload is not base64")
        };

        let session = iexpect!(try!(req.session().get::<Login>()),
                                   (status::BadRequest, "No session! Please login"));

        let ref username = session.username;
        let ref password = session.hashed_password;

        let ref lid = iexpect!(req.extensions.get::<Router>().unwrap().find("lid"));

        let id = Uuid::new_v4();

        let document = Document {
            id: id,
            payload: payload,
            version: 1,
        };

        let session = TransactionId::new();

        match Handler::post(&SocketAddr::V4(context.node_addr),
                            &username,
                            &password,
                            document,
                            session,
                            LogId::from(lid).unwrap()) {
            Ok(id) => Ok(Response::with((status::Ok, format!("{}", id)))),
            Err(_) => {
                Ok(Response::with((status::BadRequest,
                                   "An error occured when posting new document")))
            }
        }
    }

    fn http_trans_post(req: &mut Request, context: &Context) -> IronResult<Response> {
        let payload = {
            let ref body = req.get::<bodyparser::Json>().unwrap().unwrap();

            let p = iexpect!(body.find("payload"));

            let str_payload = match *p {
                serde_json::Value::String(ref load) => load,
                _ => panic!("Unexpected payload type"),
            };

            str_payload.from_base64().expect("Payload is not base64")
        };

        let ref session = iexpect!(try!(req.session().get::<Login>()));

        let ref username = session.username;
        let ref password = session.hashed_password;

        // TODO do not panic
        let session: TransactionId = req.extensions
            .get::<Router>()
            .unwrap()
            .find("session")
            .expect("Cannot find session")
            .parse()
            .expect("Failed to parse session id");

        let ref lid = iexpect!(req.extensions.get::<Router>().unwrap().find("lid"),
                               (status::BadRequest, "Cannot find lid"));

        let id = Uuid::new_v4();

        let document = Document {
            id: id,
            payload: payload,
            version: 1,
        };

        match Handler::post(&SocketAddr::V4(context.node_addr),
                            &username,
                            &password,
                            document,
                            session,
                            LogId::from(lid).unwrap()) {
            Ok(id) => Ok(Response::with((status::Ok, format!("{}", id)))),
            Err(_) => {
                Ok(Response::with((status::InternalServerError,
                                   "An error occured when posting new document")))
            }
        }

    }

    fn http_delete(req: &mut Request, context: &Context) -> IronResult<Response> {
        let ref session = iexpect!(try!(req.session().get::<Login>()));

        let ref username = session.username;
        let ref password = session.hashed_password;

        let session = TransactionId::new();

        let ref doc_id = iexpect!(req.extensions
            .get::<Router>()
            .unwrap()
            .find("id"));

        let ref lid = iexpect!(req.extensions.get::<Router>().unwrap().find("lid"));


        let res = match Handler::remove(&SocketAddr::V4(context.node_addr),
                                        &username,
                                        &password,
                                        Uuid::parse_str(*doc_id).unwrap(),
                                        session,
                                        LogId::from(lid).unwrap()) {
            Ok(()) => Response::with((status::Ok, "Ok")),
            Err(_) => {
                Response::with((status::InternalServerError,
                                "An error occured when removing document"))
            }
        };

        Ok(res)
    }

    fn http_trans_delete(req: &mut Request, context: &Context) -> IronResult<Response> {
        let ref session = iexpect!(try!(req.session().get::<Login>()));

        let ref username = session.username;
        let ref password = session.hashed_password;

        // TODO do not panic
        let ref session: TransactionId = req.extensions
            .get::<Router>()
            .unwrap()
            .find("session")
            .expect("Cannot find session")
            .parse()
            .expect("Failed to parse session id");


        let ref doc_id = iexpect!(req.extensions
            .get::<Router>()
            .unwrap()
            .find("id"));

        let ref lid = iexpect!(req.extensions.get::<Router>().unwrap().find("lid"));

        let res = match Handler::remove(&SocketAddr::V4(context.node_addr),
                                        &username,
                                        &password,
                                        Uuid::parse_str(*doc_id).unwrap(),
                                        *session,
                                        LogId::from(lid).unwrap()) {
            Ok(()) => Response::with((status::Ok, "Ok")),
            Err(_) => {
                Response::with((status::InternalServerError,
                                "An error occured when removing document"))
            }
        };

        Ok(res)
    }

    fn http_put(req: &mut Request, context: &Context) -> IronResult<Response> {
        let ref session = iexpect!(try!(req.session().get::<Login>()));

        let ref username = session.username;
        let ref password = session.hashed_password;

        let payload = {
            let ref body = req.get::<bodyparser::Json>().unwrap().unwrap();

            let p = iexpect!(body.find("payload"));

            // TODO should not panic
            let str_payload = match *p {
                serde_json::Value::String(ref load) => load,
                _ => panic!("Unexpected payload type"),
            };

            itry!(str_payload.from_base64(),
                  (status::BadRequest, "Payload is not base64"))
        };

        let ref id = iexpect!(req.extensions.get::<Router>().unwrap().find("id"),
                              (status::BadRequest, "Cannot find id"));
        let ref lid = iexpect!(req.extensions.get::<Router>().unwrap().find("lid"),
                               (status::BadRequest, "Cannot find logid"));

        let session = TransactionId::new();

        let bytes = itry!(payload.from_base64(),
                          (status::BadRequest, "Payload is not base64"));


        let res = match Handler::put(&SocketAddr::V4(context.node_addr),
                                     &username,
                                     &password,
                                     Uuid::parse_str(&id).unwrap(),
                                     bytes,
                                     session,
                                     LogId::from(lid).unwrap()) {
            Ok(()) => Response::with((status::Ok, "Ok")),
            Err(_) => {
                Response::with((status::InternalServerError,
                                "An error occured when updating document"))
            }
        };
        Ok(res)

    }

    fn http_trans_put(req: &mut Request, context: &Context) -> IronResult<Response> {
        let ref session = iexpect!(try!(req.session().get::<Login>()));

        let ref username = session.username;
        let ref password = session.hashed_password;

        let payload = {
            let ref body = req.get::<bodyparser::Json>().unwrap().unwrap();

            let p = iexpect!(body.find("payload"));

            let str_payload = match *p {
                serde_json::Value::String(ref load) => load,
                _ => panic!("Unexpected payload type"),
            };

            itry!(str_payload.from_base64(),
                  (status::BadRequest, "Payload is not base64"))
        };

        // TODO do not panic
        let session: TransactionId = req.extensions
            .get::<Router>()
            .unwrap()
            .find("session")
            .expect("Cannot find session")
            .parse()
            .expect("Failed to parse session id");

        let ref id = iexpect!(req.extensions.get::<Router>().unwrap().find("id"));
        let ref lid = iexpect!(req.extensions.get::<Router>().unwrap().find("lid"));

        let res = match Handler::put(&SocketAddr::V4(context.node_addr),
                                     &username,
                                     &password,
                                     Uuid::parse_str(&id).unwrap(),
                                     payload,
                                     session,
                                     LogId::from(lid).unwrap()) {
            Ok(()) => Response::with((status::Ok, "Ok")),
            Err(_) => {
                Response::with((status::InternalServerError,
                                "An error occured when updating document"))
            }
        };
        Ok(res)

    }

    fn http_begin_transaction(req: &mut Request, context: &Context) -> IronResult<Response> {
        let ref session = iexpect!(try!(req.session().get::<Login>()));

        let ref username = session.username;
        let ref password = session.hashed_password;

        let ref lid = iexpect!(req.extensions.get::<Router>().unwrap().find("lid"));

        match Handler::begin_transaction(&SocketAddr::V4(context.node_addr),
                                         &username,
                                         &password,
                                         TransactionId::new(),
                                         LogId::from(lid).unwrap()) {
            Ok(session) => Ok(Response::with((status::Ok, session))),
            Err(_) => Ok(Response::with((status::InternalServerError, "Something went wrong :("))),
        }
    }

    fn http_commit_transaction(req: &mut Request, context: &Context) -> IronResult<Response> {
        let session = iexpect!(try!(req.session().get::<Login>()));

        let ref username = session.username;
        let ref password = session.hashed_password;

        // TODO do not panic
        let ref session: TransactionId = req.extensions
            .get::<Router>()
            .unwrap()
            .find("session")
            .expect("Cannot find session")
            .parse()
            .expect("Failed to parse session id");

        let ref lid = iexpect!(req.extensions.get::<Router>().unwrap().find("lid"),
                               (status::BadRequest, "Cannot find logid"));

        match Handler::commit_transaction(&SocketAddr::V4(context.node_addr),
                                          &username,
                                          &password,
                                          LogId::from(lid).unwrap(),
                                          *session) {
            Ok(res) => Ok(Response::with((status::Ok, res))),
            Err(_) => Ok(Response::with((status::InternalServerError, "Something went wrong :("))),
        }
    }

    fn http_rollback_transaction(req: &mut Request, context: &Context) -> IronResult<Response> {
        let session = iexpect!(try!(req.session().get::<Login>()));

        let ref username = session.username;
        let ref password = session.hashed_password;

        // TODO do not panic
        let ref session: TransactionId = req.extensions
            .get::<Router>()
            .unwrap()
            .find("session")
            .expect("Cannot find session")
            .parse()
            .expect("Failed to parse session id");

        let ref lid = iexpect!(req.extensions.get::<Router>().unwrap().find("lid"),
                               (status::BadRequest, "Cannot find logid"));

        match Handler::rollback_transaction(&SocketAddr::V4(context.node_addr),
                                            &username,
                                            &password,
                                            LogId::from(lid).unwrap(),
                                            *session) {
            Ok(res) => Ok(Response::with((status::Ok, res))),
            Err(_) => Ok(Response::with((status::InternalServerError, "Something went wrong :("))),
        }
    }
}
