use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::io::prelude::*;
use std::net::TcpStream;

pub const SERVER_PORT: u16 = 15555;
pub type User = String;
pub type PublicKey = String;

#[derive(Serialize, Deserialize, Debug)]
pub struct LoginCommand {
    pub user_name: String,
    pub public_key: PublicKey,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientCommand {
    // Username, Public Key
    Logout(),
    // Message vom client zum server
    SendMessage(String),
    // TODO
    StartVoiceCall(),
    StartVideoCall(),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ServerCommand {
    // Message vom server zum client
    // timestamp secs, user name, content
    SendMessage(u64, String, String),
    // Connect new user
    UserConnected(User),
    // Disconnect user
    UserDisconnected(User),
}

pub fn send<M: Serialize>(stream: &mut TcpStream, message: &M) -> std::io::Result<()> {
    let data = serde_json::to_vec(message)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let len = (data.len() as u32).to_be_bytes();

    stream.write_all(&len)?;
    stream.write_all(&data)?;

    Ok(())
}

pub fn receive<M: DeserializeOwned>(stream: &mut TcpStream) -> std::io::Result<M> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let data_len = u32::from_be_bytes(len_buf) as usize;

    let mut buf = vec![0; data_len];
    stream.read_exact(&mut buf)?;

    let msg = serde_json::from_slice(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    Ok(msg)
}
