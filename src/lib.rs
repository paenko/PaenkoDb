extern crate raft;
extern crate rustc_serialize;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate uuid;
extern crate toml;
extern crate base64;
extern crate bincode;
#[macro_use]
extern crate iron;
extern crate router;
extern crate params;
extern crate bodyparser;
extern crate iron_sessionstorage;

pub mod document;
//pub mod http_handler;
pub mod handler;
pub mod config;
pub mod doclog;
mod types;
pub mod statemachine;
mod parser;
pub mod login;
