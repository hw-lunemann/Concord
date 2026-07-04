use std::io::prelude::*;
use std::net::TcpStream;
use serde::{Deserialize, Serialize};

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
        
}

fn receive<M: Deserialize>(stream: &mut TcpStream) -> std::io::Result<M> {
        
}
