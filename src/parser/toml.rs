use toml::{decode, DecodeError};
use parser::Parser as tParser;
use config::Config;

pub struct Parser;

impl tParser for Parser {
    type Error = DecodeError;

    fn parse(input: &str) -> Result<Config, Self::Error> {
        let decoded: Config = decode(input.parse().unwrap()).expect("An error occurred while parsing the config");

        Ok(decoded)
    }
}
