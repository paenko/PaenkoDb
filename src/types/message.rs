use document::{DocumentId, Document};

#[derive(Debug,Serialize,Deserialize)]
pub enum Message {
    Get(DocumentId),
    Post(Document),
    Remove(Document),
    Put(DocumentId, Document, Vec<u8>),
}
