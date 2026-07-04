use crate::client::client_lib::*;
use std::io::Result;

fn main() -> Result<()> {
    let mut uname = "Sascha";
    let stream = client_lib::init_connection()?;
    let pub_key = "here must be your ssh pub key"
    let mut client = Client::create_client(uname.to_owned(), stream, pub_key);
    Ok(())
}
