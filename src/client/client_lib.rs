use std::net::TcpStream;
use std::io::Result;
use std::time::Duration;
use crate::protocol::*;


pub struct Client {
    uname: String,
    stream: TcpStream,
    pub_key: String,
}

impl Client {

    pub fn create_client(uname: String, stream: TcpStream, pub_key) -> Client {
        Client {
            uname: uname,
            stream: stream,
            pub_key: pub_key
        }
    }
}

pub fn init_connection() -> Result<TcpStream> {
    let server_address = "127.0.0.1:80";
    let mut stream = TcpStream::connect(server_address)
    .expect("Couldn't connect to the server...");
    stream.set_read_timeout(Some(Duration::from_secs(5)))
    .expect("set_read_timeout call failed");
    stream.set_write_timeout(Some(Duration::from_secs(5)))
    .expect("set_write_timeout call failed");
    return Ok(stream);
}

pub fn log_in(&self) -> std::io::Result<()> {
    let stream = &mut self.stream;
    let uname = self.uname;
    let pub_key = self.pub_key;
    protocol::send<ClientCommand>(stream, &ClientCommand.LogIn(uname, pub_key))?;
    Ok(())
}

pub fn sendMessage(&self, message: String) -> std::io::Result<()> {
    let stream = &mut self.stream;
    protocol::send<ClientCommand>(stream, &ClientCommand.SendMessage(message))?;
    Ok(())
}

//TODO
pub fn startVoiceCall(&self, message: String) -> std::io::Result<()> {
    let stream = &mut self.stream;
    protocol::send<ClientCommand>(stream, &ClientCommand.StartVoiceCall())?;
    Ok(())
}

//TODO
pub fn startVideoCall(&self, message: String) -> std::io::Result<()> {
    let stream = &mut self.stream;
    protocol::send<ClientCommand>(stream, &ClientCommand.StartVideoCall())?;
    Ok(())
}
