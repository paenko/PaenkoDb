use std::net::ToSocketAddrs;
use std::net::SocketAddr;
use uuid::Uuid;

pub type DocumentId = Uuid;

#[derive(Serialize,Deserialize,Debug,Clone,Eq,PartialEq)]
pub struct Document {
    pub id: DocumentId,
    pub payload: Vec<u8>,
    pub version: usize,
}

impl Document {
    pub fn put(&mut self, new_payload: Vec<u8>) {
        self.payload = new_payload;

        self.version += 1;
    }
}

pub fn parse_addr(addr: &str) -> SocketAddr {
    addr.to_socket_addrs()
        .ok()
        .expect(&format!("unable to parse socket address: {}", addr))
        .next()
        .unwrap()
}

#[derive(Debug,Clone,Serialize,Deserialize,PartialEq)]
pub enum ActionType {
    Get,
    Put,
    Post,
    Remove,
}

// TODO make method private
#[derive(Debug,Clone,Deserialize,Serialize)]
pub struct DocumentRecord {
    id: DocumentId,
    path: String,
    pub method: ActionType,
    old: Option<Vec<u8>>,
}

impl DocumentRecord {
    pub fn new(id: DocumentId, path: String, method: ActionType) -> Self {
        DocumentRecord {
            id: id,
            path: path,
            method: method,
            old: None,
        }
    }

    pub fn set_old_payload(&mut self, old: Vec<u8>) {
        self.old = Some(old);
    }

    pub fn get_id(&self) -> DocumentId {
        self.id
    }

    pub fn get_old_payload(&self) -> Option<Vec<u8>> {
        self.old.clone()
    }
}
