use std::{error, fmt, result};
use std::fs::File;
use std::io::prelude::*;
use bincode::SizeLimit;
use bincode::serde::serialize as encode;
use bincode::serde::deserialize_from as decode_from;
use bincode::serde::serialize_into as encode_into;
use std::fs::OpenOptions;

use raft::persistent_log::Log;
use raft::LogIndex;
use raft::ServerId;
use raft::Term;
use raft::LogId;

#[derive(Clone, Debug)]
pub struct DocLog {
    entries: Vec<(Term, Vec<u8>)>,
    logid: LogId,
    prefix: String,
}

/// Non-instantiable error type for MemLog
pub enum Error { }

impl fmt::Display for Error {
    fn fmt(&self, _fmt: &mut fmt::Formatter) -> fmt::Result {
        unreachable!()
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, _fmt: &mut fmt::Formatter) -> fmt::Result {
        unreachable!()
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        unreachable!()
    }
}

impl DocLog {
    /// Creates a new log
    pub fn new(prefix: &str, lid: LogId) -> Self {
        let mut d = DocLog {
            prefix: prefix.to_string(),
            entries: Vec::new(),
            logid: lid,
        };

        d.create_dir().expect(&format!("Cannot find volume {}. You might need to create it",prefix));
        d.set_current_term(Term::from(0)).unwrap();
        d.restore_snapshot();

        d
    }
    
    /// Creates the directories if they do not exist
    fn create_dir(&self) -> ::std::io::Result<()>{
        ::std::fs::create_dir_all(&self.prefix)
    }

    /// Returns the directory which information will be saved to 
    pub fn get_volume(&self) -> String {
        self.prefix.clone()
    }

    // TODO implement Result
    /// Saves all LogEntries on the disk
    pub fn snapshot(&self) {
        let mut handler = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(format!("{}/{}_log", self.prefix, self.logid))
            .expect(&format!("Filehandler cannot open {}/{}_{}",
                             self.prefix,
                             self.logid,
                             "log"));

        encode_into(&mut handler, &self.entries.as_slice(), SizeLimit::Infinite)
            .expect("Log serialize_into failed");
    }

    /// Restores all LogEntries from disk
    pub fn restore_snapshot(&mut self) {
        let mut handler = match OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(format!("{}/{}_log", self.prefix, self.logid)) {
            Ok(s) => s,
            Err(_) => return,
        };


        self.entries = decode_from(&mut handler, SizeLimit::Infinite)
            .expect("Log serialize_into failed");
    }
}

// TODO error handling for IO
impl Log for DocLog {
    type Error = Error;

    fn current_term(&self) -> result::Result<Term, Error> {
        let mut term_handler = File::open(format!("{}/{}_{}", self.prefix, self.logid, "term"))
            .expect("Unable to read current_term");

        let term: Term = decode_from(&mut term_handler, SizeLimit::Infinite).unwrap();

        Ok(term)
    }

    fn set_current_term(&mut self, term: Term) -> result::Result<(), Error> {
        let mut term_handler = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(format!("{}/{}_{}", self.prefix, self.logid, "term"))
            .expect(&format!("Filehandler cannot open {}/{}_{}",
                             self.prefix,
                             self.logid,
                             "term"));

        let bytes = encode(&term, SizeLimit::Infinite).expect("Cannot encode term");

        term_handler.write_all(bytes.as_slice()).expect("Unable to save the current term");

        term_handler.flush().expect("Flushing failed");

        self.set_voted_for(None).unwrap();

        Ok(())
    }

    fn inc_current_term(&mut self) -> result::Result<Term, Error> {
        self.set_voted_for(None).unwrap();
        let new_term = self.current_term().unwrap() + 1;
        self.set_current_term(new_term).unwrap();
        self.current_term()
    }

    fn voted_for(&self) -> result::Result<Option<ServerId>, Error> {
        let mut voted_for_handler =
            File::open(format!("{}/{}_{}", self.prefix, self.logid, "voted_for"))
                .expect("Unable to read voted_for");

        let voted_for: Option<ServerId> = decode_from(&mut voted_for_handler, SizeLimit::Infinite)
            .unwrap();

        Ok(voted_for)
    }

    fn set_voted_for(&mut self, address: Option<ServerId>) -> result::Result<(), Error> {
        let mut voted_for_handler = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(format!("{}/{}_{}", self.prefix, self.logid, "voted_for"))
            .expect(&format!("Filehandler cannot open {}/{}_{}",
                             self.prefix,
                             self.logid,
                             "voted_for"));

        let bytes = encode(&address, SizeLimit::Infinite).expect("Cannot encode voted_for");

        voted_for_handler.write_all(bytes.as_slice()).expect("Unable to save the server vote");

        voted_for_handler.flush().expect("Flushing failed");

        Ok(())
    }

    fn latest_log_index(&self) -> result::Result<LogIndex, Error> {
        Ok(LogIndex::from(self.entries.len() as u64))
    }

    fn latest_log_term(&self) -> result::Result<Term, Error> {
        let len = self.entries.len();
        if len == 0 {
            Ok(Term::from(0))
        } else {
            Ok(self.entries[len - 1].0)
        }
    }

    fn entry(&self, index: LogIndex) -> result::Result<(Term, &[u8]), Error> {
        let (term, ref bytes) = self.entries[(index - 1).as_u64() as usize];
        Ok((term, bytes))
    }

    fn append_entries(&mut self,
                      from: LogIndex,
                      entries: &[(Term, &[u8])])
                      -> result::Result<(), Error> {
        assert!(self.latest_log_index().unwrap() + 1 >= from);
        self.entries.truncate((from - 1).as_u64() as usize);
        self.snapshot();
        Ok(self.entries.extend(entries.iter().map(|&(term, command)| (term, command.to_vec()))))
    }

    fn truncate(&mut self, lo: LogIndex) -> result::Result<(), Error> {
        Ok(self.entries.truncate(lo.as_u64() as usize))
    }

    fn rollback(&mut self, lo: LogIndex) -> result::Result<(Vec<(Term, Vec<u8>)>), Error> {
        Ok(self.entries[(lo.as_u64() as usize)..].to_vec())
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use raft::LogIndex;
    use raft::ServerId;
    use raft::Term;
    use raft::persistent_log::Log;
    use std::fs::File;
    use bincode::SizeLimit;
    use bincode::rustc_serialize::{encode_into, encode, decode, decode_from};
    use std::io::prelude::*;
    use std::fs::OpenOptions;
    use std::io::SeekFrom;
    use uuid::Uuid;
    use raft::LogId;

    lazy_static!{
        static ref lid : LogId = LogId::from("3d30aa56-98b2-4891-aec5-847cee6e1703").unwrap();
    }

    #[test]
    fn test_current_term() {
        let mut store = DocLog::new("/tmp", *lid);
        assert_eq!(Term::from(0), store.current_term().unwrap());
        store.set_voted_for(Some(ServerId::from(0))).unwrap();
        store.set_current_term(Term::from(42)).unwrap();
        assert_eq!(None, store.voted_for().unwrap());
        assert_eq!(Term::from(42), store.current_term().unwrap());
        store.inc_current_term().unwrap();
        assert_eq!(Term::from(43), store.current_term().unwrap());
    }

    #[test]
    fn test_voted_for() {
        let mut store = DocLog::new("/tmp", *lid);
        assert_eq!(None, store.voted_for().unwrap());
        let id = ServerId::from(0);
        store.set_voted_for(Some(id)).unwrap();
        assert_eq!(Some(id), store.voted_for().unwrap());
    }

    #[test]
    fn test_append_entries() {
        let mut store = DocLog::new("/tmp", *lid);
        assert_eq!(LogIndex::from(0), store.latest_log_index().unwrap());
        assert_eq!(Term::from(0), store.latest_log_term().unwrap());

        // [0.1, 0.2, 0.3, 1.4]
        store.append_entries(LogIndex::from(1),
                            &[(Term::from(0), &[1]),
                              (Term::from(0), &[2]),
                              (Term::from(0), &[3]),
                              (Term::from(1), &[4])])
            .unwrap();
        assert_eq!(LogIndex::from(4), store.latest_log_index().unwrap());
        assert_eq!(Term::from(1), store.latest_log_term().unwrap());
        assert_eq!((Term::from(0), &*vec![1u8]),
                   store.entry(LogIndex::from(1)).unwrap());
        assert_eq!((Term::from(0), &*vec![2u8]),
                   store.entry(LogIndex::from(2)).unwrap());
        assert_eq!((Term::from(0), &*vec![3u8]),
                   store.entry(LogIndex::from(3)).unwrap());
        assert_eq!((Term::from(1), &*vec![4u8]),
                   store.entry(LogIndex::from(4)).unwrap());

        // [0.1, 0.2, 0.3]
        store.append_entries(LogIndex::from(4), &[]).unwrap();
        assert_eq!(LogIndex::from(3), store.latest_log_index().unwrap());
        assert_eq!(Term::from(0), store.latest_log_term().unwrap());
        assert_eq!((Term::from(0), &*vec![1u8]),
                   store.entry(LogIndex::from(1)).unwrap());
        assert_eq!((Term::from(0), &*vec![2u8]),
                   store.entry(LogIndex::from(2)).unwrap());
        assert_eq!((Term::from(0), &*vec![3u8]),
                   store.entry(LogIndex::from(3)).unwrap());

        // [0.1, 0.2, 2.3, 3.4]
        store.append_entries(LogIndex::from(3),
                            &[(Term::from(2), &[3]), (Term::from(3), &[4])])
            .unwrap();
        assert_eq!(LogIndex::from(4), store.latest_log_index().unwrap());
        assert_eq!(Term::from(3), store.latest_log_term().unwrap());
        assert_eq!((Term::from(0), &*vec![1u8]),
                   store.entry(LogIndex::from(1)).unwrap());
        assert_eq!((Term::from(0), &*vec![2u8]),
                   store.entry(LogIndex::from(2)).unwrap());
        assert_eq!((Term::from(2), &*vec![3u8]),
                   store.entry(LogIndex::from(3)).unwrap());
        assert_eq!((Term::from(3), &*vec![4u8]),
                   store.entry(LogIndex::from(4)).unwrap());
    }
}
