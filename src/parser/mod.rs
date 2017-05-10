use config::Config;
use std::error;

pub mod toml;

pub trait Parser: Sized + 'static {
    type Error: error::Error + Sized + 'static;

    fn parse(input: &str) -> Result<Config, Self::Error>;
}
