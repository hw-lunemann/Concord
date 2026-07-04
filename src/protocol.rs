use std::io::prelude::*;
use std::net::TcpStream;
use serde::{de::DeserializeOwned, Serialize};

type User = String;

#[derive(Serialize, Deserialize, Debug)]
enum ClientCommand {
    // Username, Public Key
    LogIn(String, String),
    // Message vom client zum server
    SendMessage(String),
    // TODO
    StartVoiceCall(),
    StartVideoCall()
}

#[derive(Serialize, Deserialize, Debug)]
enum ServerResponse {
    // Okay
    Ack(ClientCommand),
    // Command, Reason for error
    Error(ClientCommand, String)
}

#[derive(Serialize, Deserialize, Debug)]
enum ServerCommand {
    // Message vom server zum client
    SendMessage(String),
    // Connect new user
    UserConnected(User),
    // Disconnect user
    UserDisconnected(User)
}

#[derive(Serialize, Deserialize, Debug)]
enum ClientResponse {
    // Okay
    Ack(ServerCommand),
    // Command, Reason for error
    Error(ServerCommand, String)
}

fn send<M: Serialize>(stream: &mut TcpStream, message: &M) -> std::io::Result<()> {
    let data = serde_json::to_vec(message)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let len = (data.len() as u32).to_be_bytes();

    stream.write_all(&len)?;
    stream.write_all(&data)?;

    Ok(())

}

fn receive<M: DeserializeOwned>(stream: &mut TcpStream) -> std::io::Result<M> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut buf = vec![0; len];
    stream.read_exact(&mut buf)?;

    let msg = serde_json::from_slice(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    Ok(msg)
}
