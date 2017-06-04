use raft::state_machine;

use bincode::serde::serialize as encode;
use bincode::serde::deserialize as decode;
use bincode::SizeLimit;

use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::io::Error as IoError;
use std::path::{Path, PathBuf};

use types::Message;
use app::document::*;
use raft::state_machine::StateMachineError;
use std::collections::HashMap;

#[derive(Debug,Clone)]
pub struct DocumentStateMachine {
    map: HashMap<DocumentId, Document>,
    path: PathBuf,
}

impl DocumentStateMachine {
    pub fn new(path: &Path) -> Self {

        let s = DocumentStateMachine {
            path: path.to_path_buf(),
            map: HashMap::new(),
        };

        if !s.check_if_volume_exists() {
            s.create_dir()
                .expect(&format!("Cannot find volume {}. You might need to create it",
                                s.path.to_str().unwrap()));
        }

        s
    }

    fn check_if_volume_exists(&self) -> bool {
        self.path.exists()
    }

    fn create_dir(&self) -> ::std::io::Result<()> {
        ::std::fs::create_dir_all(&self.path)
    }

    pub fn exists(&self, did: &DocumentId) -> bool {
        self.map.contains_key(did)
    }

    pub fn get_documents(&self) -> Vec<DocumentId> {
        self.map.keys().into_iter().cloned().collect()
    }

    fn post(&mut self, document: Document) -> Vec<u8> {
        self.map.insert(document.id.clone(), document.clone());

        encode(&document, SizeLimit::Infinite).unwrap()
    }

    fn remove(&mut self, id: DocumentId) -> Vec<u8> {
        self.map.remove(&id);

        Vec::new()
    }

    fn put(&mut self, id: DocumentId, new_payload: Vec<u8>) -> Vec<u8> {
        let mut document = self.map.get_mut(&id).unwrap();
        document.put(new_payload);

        encode(&document, SizeLimit::Infinite).unwrap()
    }

    pub fn get_snapshot_map(&self) -> Result<Vec<u8>, IoError> {
        let mut fs = try!(File::open(&format!("./{}/snapshot_map", self.path.to_str().unwrap())));
        let mut buffer = Vec::new();

        fs.read_to_end(&mut buffer)?;

        Ok(buffer)
    }
}

impl state_machine::StateMachine for DocumentStateMachine {
    fn apply(&mut self, new_value: &[u8]) -> Result<Vec<u8>, StateMachineError> {
        let message = decode(&new_value)
            .map_err(StateMachineError::Deserialization)?;

        let response = match message {
            Message::Get(_) => self.query(new_value)?, // delegate to query when propose
            Message::Post(document) => self.post(document),
            Message::Remove(document) => self.remove(document.id),
            Message::Put(id, _, new_payload) => self.put(id, new_payload),
        };

        self.snapshot()?;

        Ok(response)
    }

    fn revert(&mut self, command: &[u8]) -> Result<(), StateMachineError> {
        let message = decode(&command)
            .map_err(StateMachineError::Deserialization)?;

        let response = match message {
            Message::Get(_) => return Err(StateMachineError::Other("Cannot skip get".to_owned())),
            Message::Post(document) => self.remove(document.id),
            Message::Remove(document) => self.post(document),
            Message::Put(id, document, _) => self.put(id, document.payload),
        };

        self.snapshot()?;

        Ok(())
    }

    fn query(&self, query: &[u8]) -> Result<Vec<u8>, StateMachineError> {
        let message = decode(&query).map_err(StateMachineError::Deserialization)?;

        let response = match message {
            Message::Get(id) => {
                let doc = match self.map.get(&id) {
                    Some(doc) => doc,
                    None => {
                        return Err(StateMachineError::NotFound);
                    }
                };

                encode(&doc, SizeLimit::Infinite).map_err(StateMachineError::Serialization)
            }
            _ => return Err(StateMachineError::Other("Wrong usage of .query".to_owned())),
        };

        response
    }

    fn snapshot(&self) -> Result<Vec<u8>, StateMachineError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&format!("{}/snapshot_map", self.path.to_str().unwrap()))
            .map_err(StateMachineError::Io)?;

        let map = encode(&self.map, SizeLimit::Infinite)
            .map_err(StateMachineError::Serialization)?;
        file.write_all(&map).map_err(StateMachineError::Io)?;

        Ok(map)
    }

    fn restore_snapshot(&mut self, snap_map: Vec<u8>) -> Result<(), StateMachineError> {
        let map: HashMap<DocumentId, Document> = decode(&snap_map).unwrap_or(HashMap::new());

        Ok(self.map = map)
    }
}


#[cfg(test)]
mod tests {
    extern crate tempdir;

    use super::*;
    use self::tempdir::TempDir;
    use types::Message;
    use app::document::Document;

    use bincode::serde::serialize as encode;
    use bincode::serde::deserialize as decode;
    use bincode::SizeLimit;

    use raft::StateMachine;

    fn setup() -> (DocumentStateMachine, TempDir) {
        let tempdir = TempDir::new("tmp").unwrap();
        let statemachine = DocumentStateMachine::new(tempdir.path());

        (statemachine, tempdir)
    }

    #[test]
    fn test_get() {
        let (mut statemachine, dir) = setup();
        let document = Document::new(vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let msg = Message::Post(document.clone());

        statemachine.apply(&encode(&msg, SizeLimit::Infinite).unwrap());

        let msg = Message::Get(document.id);

        let raw_document = statemachine
            .query(&encode(&msg, SizeLimit::Infinite).unwrap())
            .unwrap();
        let get_document: Document = decode(&raw_document).unwrap();

        assert_eq!(document, get_document);
    }

    #[test]
    fn test_post() {
        let (mut statemachine, dir) = setup();
        let document = Document::new(vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let msg = Message::Post(document.clone());

        let applied = statemachine
            .apply(&encode(&msg, SizeLimit::Infinite).unwrap())
            .unwrap();

        let document_applied = decode(applied.as_slice()).unwrap();

        assert_eq!(document, document_applied);
    }

    #[test]
    fn test_put() {
        let (mut statemachine, dir) = setup();
        let mut payload = vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut document = Document::new(payload.clone());
        let msg = Message::Post(document.clone());

        statemachine.apply(&encode(&msg, SizeLimit::Infinite).unwrap());
        document.payload.reverse();

        payload.reverse();
        let msg = Message::Put(document.id, document.clone(), payload.clone());

        let applied = statemachine
            .apply(&encode(&msg, SizeLimit::Infinite).unwrap())
            .unwrap();

        let document_applied: Document = decode(applied.as_slice()).unwrap();

        assert_eq!(document.payload, document_applied.payload);
        assert_eq!(document_applied.version, 1);
    }

    #[test]
    #[should_panic(expected = "NotFound")]
    fn test_remove() {
        let (mut statemachine, dir) = setup();
        let document = Document::new(vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let msg = Message::Post(document.clone());

        statemachine.apply(&encode(&msg, SizeLimit::Infinite).unwrap());


        let msg = Message::Remove(document.clone());
        statemachine.apply(&encode(&msg, SizeLimit::Infinite).unwrap());

        let msg = Message::Get(document.id);
        statemachine
            .query(&encode(&msg, SizeLimit::Infinite).unwrap())
            .unwrap();
    }

    #[test]
    fn test_revert_post() {
        let (mut statemachine, dir) = setup();

        let document = Document::new(vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let msg = Message::Post(document.clone());

        statemachine.apply(&encode(&msg, SizeLimit::Infinite).unwrap());

        statemachine.revert(&encode(&msg, SizeLimit::Infinite).unwrap());

        assert!(!statemachine.exists(&document.id));
    }

    #[test]
    fn test_revert_put() {
        let (mut statemachine, dir) = setup();

        let document = Document::new(vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let msg = Message::Post(document.clone());

        statemachine.apply(&encode(&msg, SizeLimit::Infinite).unwrap());

        let mut new_payload = document.payload.clone();

        new_payload.reverse();

        let msg = Message::Put(document.id, document.clone(), new_payload.clone());

        statemachine.apply(&encode(&msg, SizeLimit::Infinite).unwrap());

        let document: Document =
            decode(&statemachine
                        .query(&encode(&Message::Get(document.clone().id),
                                       SizeLimit::Infinite)
                                        .unwrap())
                        .unwrap())
                    .unwrap();

        assert_eq!(new_payload, document.payload);

        statemachine.revert(&encode(&msg, SizeLimit::Infinite).unwrap());

        let document: Document = decode(&statemachine
                                             .query(&encode(&Message::Get(document.id),
                                                            SizeLimit::Infinite)
                                                             .unwrap())
                                             .unwrap())
                .unwrap();

        assert_eq!(vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], document.payload);
    }

    #[test]
    fn test_revert_remove() {
        let (mut statemachine, dir) = setup();

        let document = Document::new(vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let msg = Message::Post(document.clone());

        statemachine.apply(&encode(&msg, SizeLimit::Infinite).unwrap());

        let msg = Message::Remove(document.clone());

        statemachine.apply(&encode(&msg, SizeLimit::Infinite).unwrap());

        assert!(!statemachine.exists(&document.id));

        statemachine.revert(&encode(&msg, SizeLimit::Infinite).unwrap());

        assert!(statemachine.exists(&document.id));
    }
}
