use std::net::TcpStream;
use std::io::Result;
use std::time::Duration;

pub struct Client {
    uname: String,
    stream: TcpStream,
}

impl Client {

    pub fn log_in(uname: String, stream: TcpStream) -> Client {
        Client {
            uname: uname,
            stream: stream
        }
    }
}

pub fn connect() -> Result<TcpStream> {
    let server_address = "127.0.0.1:80";
    let mut stream = TcpStream::connect(server_address)
    .expect("Couldn't connect to the server...");
    stream.set_read_timeout(Some(Duration::from_secs(5)))
    .expect("set_read_timeout call failed");
    stream.set_write_timeout(Some(Duration::from_secs(5)))
    .expect("set_write_timeout call failed");
    return Ok(stream);
}


