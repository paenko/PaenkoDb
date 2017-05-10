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

use raft::auth::credentials::SingleCredentials;
use raft::auth::simple::SimpleAuth;

#[derive(Debug,Serialize,Deserialize)]
pub enum Message {
    Get(Uuid),
    Post(Document),
    Remove(Uuid),
    Put(Uuid, Vec<u8>),
}

pub struct Handler;

impl Handler {
    /// Returns a HashSet with a single Peer entry
    fn to_hashset(addr: SocketAddr) -> HashSet<SocketAddr> {
        let mut hashset: HashSet<SocketAddr> = HashSet::new();
        hashset.insert(addr);
        hashset
    }

    /// Creates a template client
    fn new_client(addr: &SocketAddr, username: &str, password: &str, lid: LogId) -> Client {
        Client::new::<SimpleAuth<SingleCredentials>>(Self::to_hashset(*addr),
                                                     username.to_string(),
                                                     password.to_string(),
                                                     lid)
    }

    /// Gets a document
    /// 
    /// # Arguments
    /// * `addr` - The `SocketAddr` of the node
    /// * `username` - The `username` of the user
    /// * `plain_password` - The `password` of the user in plain text
    /// * `id` - The `DocumentId` which will be requested
    /// * `lid` - The `LogId` in which the document should be
    pub fn get(addr: &SocketAddr,
               username: &str,
               plain_password: &str,
               id: Uuid,
               lid: LogId)
               -> Result<Document> {

        let mut client = Self::new_client(addr, username, plain_password, lid);

        let payload = encode(&Message::Get(id), SizeLimit::Infinite).unwrap();

        let response = match client.query(payload.as_slice()) {
            Ok(res) => res,
            Err(RError::Raft(RaftError::ClusterViolation(ref leader_str))) => {
                return Handler::get(&parse_addr(&leader_str),
                                    &username,
                                    &plain_password,
                                    id,
                                    lid);
            } 
            Err(err) => return Err(err),
        };

        let document: Document = decode(response.as_slice()).unwrap();

        Ok(document)
    }

    /// Inserts a new document
    /// 
    /// # Arguments
    /// * `addr` - The `SocketAddr` of the node
    /// * `username` - The `username` of the user
    /// * `plain_password` - The `password` of the user in plain text
    /// * `document` - The document which will be inserted 
    /// * `session` - The `TransactionId` of the current transaction. If no transaction is
    /// currently running, this might be random because it will be ignored
    /// * `lid` - The `LogId` in which the document should be
    pub fn post(addr: &SocketAddr,
                username: &str,
                plain_password: &str,
                document: Document,
                session: TransactionId,
                lid: LogId)
                -> Result<Uuid> {

        let mut client = Self::new_client(addr, username, plain_password, lid);

        let payload = encode(&Message::Post(document.clone()), SizeLimit::Infinite).unwrap();

        let response = match client.propose(session, payload.as_slice()) {
            Ok(res) => res,
            Err(RError::Raft(RaftError::ClusterViolation(ref leader_str))) => {
                return Handler::post(&parse_addr(&leader_str),
                                     &username,
                                     &plain_password,
                                     document,
                                     session,
                                     lid);
            } 
            Err(err) => return Err(err),
        };

        let uid: Uuid = decode(response.as_slice()).unwrap();

        Ok(uid)
    }

    /// Removes a document
    /// 
    /// # Arguments
    /// * `addr` - The `SocketAddr` of the node
    /// * `username` - The `username` of the user
    /// * `plain_password` - The `password` of the user in plain text
    /// * `id` - The `DocumentId` which will be requested
    /// * `session` - The `TransactionId` of the current transaction. If no transaction is
    /// currently running, this might be random because it will be ignored
    /// * `lid` - The `LogId` in which the document should be
    pub fn remove(addr: &SocketAddr,
                  username: &str,
                  plain_password: &str,
                  id: Uuid,
                  session: TransactionId,
                  lid: LogId)
                  -> Result<()> {
        let mut client = Self::new_client(addr, username, plain_password, lid);

        let payload = encode(&Message::Remove(id), SizeLimit::Infinite).unwrap();

        match client.propose(session, payload.as_slice()) {
            Ok(res) => res,
            Err(RError::Raft(RaftError::ClusterViolation(ref leader_str))) => {
                return Handler::remove(&parse_addr(&leader_str),
                                       &username,
                                       &plain_password,
                                       id,
                                       session,
                                       lid);
            } 
            Err(err) => return Err(err),
        };

        Ok(())
    }

    /// Updates a document
    /// 
    /// # Arguments
    /// * `addr` - The `SocketAddr` of the node
    /// * `username` - The `username` of the user
    /// * `plain_password` - The `password` of the user in plain text
    /// * `id` - The `DocumentId` which will be requested
    /// * `new_payload` - The new payload of the document with the `id`. It will replaced
    /// * `session` - The `TransactionId` of the current transaction. If no transaction is
    /// currently running, this might be random because it will be ignored
    /// * `lid` - The `LogId` in which the document should be
    pub fn put(addr: &SocketAddr,
               username: &str,
               plain_password: &str,
               id: Uuid,
               new_payload: Vec<u8>,
               session: TransactionId,
               lid: LogId)
               -> Result<()> {

        let mut client = Self::new_client(addr, username, plain_password, lid);

        let payload = encode(&Message::Put(id, new_payload.clone()), SizeLimit::Infinite).unwrap();

        match client.propose(session, payload.as_slice()) {
            Ok(res) => res,
            Err(RError::Raft(RaftError::ClusterViolation(ref leader_str))) => {
                return Handler::put(&parse_addr(&leader_str),
                                    &username,
                                    &plain_password,
                                    id,
                                    new_payload,
                                    session,
                                    lid);
            } 
            Err(err) => return Err(err),
        };

        Ok(())
    }

    /// Begins a new transaction 
    /// 
    /// # Arguments
    /// * `addr` - The `SocketAddr` of the node
    /// * `username` - The `username` of the user
    /// * `plain_password` - The `password` of the user in plain text
    /// * `session` - The `TransactionId` of the current transaction. If no transaction is
    /// currently running, this might be random because it will be ignored
    /// * `lid` - The `LogId` in which the document should be
    pub fn begin_transaction(addr: &SocketAddr,
                             username: &str,
                             password: &str,
                             session: TransactionId,
                             lid: LogId)
                             -> Result<String> {
        let mut client = Self::new_client(addr, username, password, lid);

        match client.begin_transaction(session) {
            Ok(res) => Ok(Uuid::from_bytes(res.as_slice()).unwrap().hyphenated().to_string()),
            Err(RError::Raft(RaftError::ClusterViolation(ref leader_str))) => {
                return Handler::begin_transaction(&parse_addr(&leader_str),
                                                  &username,
                                                  &password,
                                                  session,
                                                  lid);
            } 
            Err(err) => return Err(err),
        }
    }
    
    /// Commits a transaction 
    /// 
    /// # Arguments
    /// * `addr` - The `SocketAddr` of the node
    /// * `username` - The `username` of the user
    /// * `plain_password` - The `password` of the user in plain text
    /// * `session` - The `TransactionId` of the current transaction. If no transaction is
    /// currently running, this might be random because it will be ignored
    /// * `lid` - The `LogId` in which the document should be
    pub fn commit_transaction(addr: &SocketAddr,
                              username: &str,
                              password: &str,
                              lid: LogId,
                              session: TransactionId)
                              -> Result<String> {
        let mut client = Self::new_client(addr, username, password, lid);

        match client.end_transaction(session) {
            Ok(res) => Ok(from_utf8(res.as_slice()).unwrap().to_string()),
            Err(RError::Raft(RaftError::ClusterViolation(ref leader_str))) => {
                return Handler::commit_transaction(&parse_addr(&leader_str),
                                                   &username,
                                                   &password,
                                                   lid,
                                                   session);
            } 
            Err(err) => return Err(err),
        }
    }

    /// Rollbacks a transaction 
    /// 
    /// # Arguments
    /// * `addr` - The `SocketAddr` of the node
    /// * `username` - The `username` of the user
    /// * `plain_password` - The `password` of the user in plain text
    /// * `session` - The `TransactionId` of the current transaction. If no transaction is
    /// currently running, this might be random because it will be ignored
    /// * `lid` - The `LogId` in which the document should be
    pub fn rollback_transaction(addr: &SocketAddr,
                                username: &str,
                                password: &str,
                                lid: LogId,
                                session: TransactionId)
                                -> Result<String> {
        let mut client = Self::new_client(addr, username, password, lid);



        match client.rollback_transaction(session) {
            Ok(res) => Ok(from_utf8(res.as_slice()).unwrap().to_string()),
            Err(RError::Raft(RaftError::ClusterViolation(ref leader_str))) => {
                return Handler::rollback_transaction(&parse_addr(&leader_str),
                                                     &username,
                                                     &password,
                                                     lid,
                                                     session);
            } 
            Err(err) => return Err(err),
        }

    }
}
