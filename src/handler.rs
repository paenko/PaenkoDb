use document::*;
use std::net::SocketAddr;
use uuid::Uuid;
use raft::Result;
use raft::Client;
use raft::LogId;
use raft::TransactionId;
use raft::Error as RError;
use raft::RaftError;

use bincode::serde::serialize as encode;
use bincode::serde::deserialize as decode;
use bincode::SizeLimit;
use std::collections::HashSet;
use std::str::from_utf8;
use std::collections::VecDeque;

use raft::auth::credentials::Credentials;
use raft::auth::credentials::SingleCredentials;
use raft::auth::simple::SimpleAuth;
use types::Message;
use document::DocumentId;

pub struct Handler<C: Credentials>
    where C: Credentials{
    peers: HashSet<SocketAddr>,
    lid: LogId,
    client: Client<C>,
}

impl<C> Handler<C>
where C: Credentials {
    pub fn new(peers: HashSet<SocketAddr>, lid: LogId, credentials: C) -> Self{
        let client = Client::new(peers.clone(), credentials, lid);

        Handler{
            peers,
            lid,
            client: client,
        }
    }

    pub fn get(&mut self, id: DocumentId) -> Result<Document>{
        let payload = encode(&Message::Get(id), SizeLimit::Infinite).unwrap();

        let response = match self.client.query(payload.as_slice()) {
            Ok(res) => res,
            Err(err) => return Err(err),
        };

        let document: Document = decode(response.as_slice()).unwrap();

        Ok(document)
    }

    pub fn post(&mut self, document: Document, session: TransactionId) -> Result<DocumentId>{

        let payload = encode(&Message::Post(document), SizeLimit::Infinite).unwrap();

        let response = match self.client.propose(session, payload.as_slice()) {
            Ok(res) => res,
            Err(err) => return Err(err),
        };

        let did: DocumentId = decode(response.as_slice()).unwrap();

        Ok(did)
    }

    pub fn put(&mut self, did: DocumentId, new_payload: Vec<u8>, session: TransactionId) -> Result<()>{
        let document = try!(self.get(did)); //Fetch old document

        let payload = encode(&Message::Put(did, document, new_payload), SizeLimit::Infinite).unwrap();

        match self.client.propose(session, payload.as_slice()) {
            Ok(res) => res,
            Err(err) => return Err(err),
        };

        Ok(())
    }

    pub fn remove(&mut self, did: DocumentId, session:TransactionId) -> Result<()>{
        let document = self.get(did)?;

        let payload = encode(&Message::Remove(document), SizeLimit::Infinite).unwrap();

        match self.client.propose(session, payload.as_slice()) {
            Ok(res) => res,
            Err(err) => return Err(err),
        };

        Ok(())
    }

    pub fn begin_transaction(&mut self, session: TransactionId) -> Result<TransactionId>{
        match self.client.begin_transaction(session) {
            Ok(res) => {
                let uid = Uuid::from_bytes(res.as_slice()).unwrap().to_string();
                let tid = TransactionId::from(&uid).expect("Not valid TransactionId");

                Ok(tid)
            }
            Err(err) => return Err(err),
        }
    }

    pub fn commit_transaction(&mut self, session: TransactionId) -> Result<TransactionId> {
        match self.client.end_transaction(session) {
            Ok(res) => Ok(TransactionId::from(&from_utf8(res.as_slice()).unwrap().to_string()).expect("Not valid TransactionId")),
            Err(err) => return Err(err),
        }
    }

    pub fn rollback_transaction(&mut self, session: TransactionId) -> Result<TransactionId> {
        match self.client.rollback_transaction(session) {
            Ok(res) => Ok(TransactionId::from(&from_utf8(res.as_slice()).unwrap().to_string()).expect("Not valid TransactionId")),
            Err(err) => return Err(err),
        }
    }
}
