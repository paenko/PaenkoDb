use base64::{encode, decode};
use bincode::serde::{serialize,deserialize};
use bincode::SizeLimit;

#[derive(Clone, Serialize, Deserialize)]
/// This struct is contains the data for the HTTP-session
pub struct Login {
    /// The username of the user
    pub username: String,
    /// The password of the user. This is String is hashed
    pub hashed_password : String
}

impl Login{
    /// Creates new Login
    pub fn new(username: String, hashed_password: String) -> Self{
        Login{
            username,
            hashed_password
        }
    }
}

impl ::iron_sessionstorage::Value for Login {
    fn get_key() -> &'static str {
        "logged_in_user"
    }
    fn into_raw(self) -> String {
        encode(&serialize(&self,SizeLimit::Infinite).unwrap())
    }
    fn from_raw(value: String) -> Option<Self> {
        if value.is_empty() {
            None
        } else {
            let decoded = decode(&value).unwrap();
            let d = deserialize(&decoded).unwrap();

            Some(d)
        }
    }
}
