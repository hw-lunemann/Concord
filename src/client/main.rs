mod client;
use crate::client::*;
use std::io::Result;

fn main() -> Result<()> {
    let mut uname = "Sascha";
    let stream = client::connect()?;
    let mut client = Client::log_in(uname.to_owned(), stream);
    Ok(())
}
