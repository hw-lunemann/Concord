use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::io::{Error, ErrorKind, prelude::*};
use std::net::TcpStream;

pub const server_port: i32 = 15555;
pub type User = String;

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientCommand {
    // Username, Public Key
    LogIn(String, String),
    // Message vom client zum server
    SendMessage(String),
    // TODO
    StartVoiceCall(),
    StartVideoCall(),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ServerResponse {
    // Okay
    Ack(ClientCommand),
    // Command, Reason for error
    Error(ClientCommand, String),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ServerCommand {
    // Message vom server zum client
    SendMessage(String),
    // Connect new user
    UserConnected(User),
    // Disconnect user
    UserDisconnected(User),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientResponse {
    // Okay
    Ack(ServerCommand),
    // Command, Reason for error
    Error(ServerCommand, String),
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
    let peek_len = stream.peek(&mut len_buf)?;
    if peek_len < 4 {
        return Err(Error::new(
            ErrorKind::WouldBlock,
            "Can't receive a whole package yet",
        ));
    }

    let data_len = u32::from_be_bytes(len_buf) as usize;

    let peek_len = stream.peek(&mut len_buf)?;
    if peek_len < data_len + 4 {
        return Err(Error::new(
            ErrorKind::WouldBlock,
            "Can't receive a whole package yet",
        ));
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0; len];

    // now everything can be read if the os buffer is large enough
    // consume first four bytes properly before reading Message
    stream.read_exact(&mut len_buf)?;
    stream.read_exact(&mut buf)?;

    let msg = serde_json::from_slice(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    Ok(msg)
}
