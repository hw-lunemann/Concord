use std::{
    collections::{HashMap, HashSet},
    error::Error,
    net::{Shutdown, TcpListener, TcpStream},
};

use std::io::ErrorKind;

use crate::protocol::{self, ClientCommand, ServerCommand};

type ClientId = usize;
type PublicKey = String;

#[derive(Default)]
struct Clients {
    // data
    public_key: Vec<PublicKey>,
    user_name: Vec<String>,
    stream: Vec<Option<TcpStream>>,
    // subsets
    active: HashSet<ClientId>,
    // maps
    id_by_public_key: HashMap<PublicKey, ClientId>,
}

impl Clients {
    fn init() -> Clients {
        Clients::default()
    }

    fn next_id(&self) -> ClientId {
        self.public_key.len()
    }

    fn login(&mut self, public_key: PublicKey, user_name: String, stream: TcpStream) -> ClientId {
        let id = if let Some(&old_id) = self.id_by_public_key.get(&public_key) {
            self.user_name[old_id] = user_name;
            self.stream[old_id] = Some(stream);

            old_id
        } else {
            let id = self.next_id();
            self.public_key.push(public_key.clone());
            self.user_name.push(user_name);
            self.stream.push(Some(stream));

            self.id_by_public_key.insert(public_key, id);

            id
        };

        // mark active in any case.
        self.active.insert(id);
        return id;
    }

    fn logout(&mut self, id: ClientId) {
        if let Some(stream) = &self.stream[id] {
            if let Err(e) = stream.shutdown(Shutdown::Both) {
                println!("Failed TcpStream::shutdown on logout with: {:?}", e);
            }
        }
        self.stream[id] = None;
        self.active.remove(&id);
    }
}

pub struct Server {
    clients: Clients,
}

impl Server {
    pub fn new() -> Server {
        Server {
            clients: Clients::init(),
        }
    }

    pub fn run(mut self) -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:80")?;
        listener.set_nonblocking(true)?;

        let mut unauthenticated_connections = Vec::new();

        loop {
            while let Ok((stream, remote_addr)) = listener.accept() {
                if stream.set_nonblocking(false).is_ok() {
                    unauthenticated_connections.push((stream, remote_addr));
                    println!("Connection from Client {} accepted", remote_addr);
                } else {
                    let _ = stream.shutdown(Shutdown::Both);
                    println!(
                        "Connection from Client {} couldn't be set to nonblocking and was dropped.",
                        remote_addr
                    )
                };
            }

            let mut remaining_unauthenticated_connections = Vec::new();
            for (mut stream, remote_addr) in unauthenticated_connections.into_iter() {
                match protocol::receive::<ClientCommand>(&mut stream) {
                    Ok(command) => match self.handle_client_login(stream, command) {
                        Err(msg) => {
                            println!(
                                "First command from new connection with {} is not a login: {:?}",
                                remote_addr, msg
                            );
                        }
                        Ok(client_id) => {
                            println!(
                                "Login from {} with username {}",
                                remote_addr, self.clients.user_name[client_id]
                            );
                        }
                    },
                    Err(e) => {
                        if e.kind() != ErrorKind::WouldBlock {
                            let _ = stream.shutdown(Shutdown::Both);
                            println!("Receive failed, dropped connection: {}", e);
                        } else {
                            remaining_unauthenticated_connections.push((stream, remote_addr));
                        }
                    }
                }
            }
            unauthenticated_connections = remaining_unauthenticated_connections;

            for client in self.clients.active.clone() {
                let result = {
                    let stream = self.clients.stream[client]
                        .as_mut()
                        .expect("active client has None stream");

                    protocol::receive::<ClientCommand>(stream)
                };

                match result {
                    Ok(command) => self.handle_client_command(client, command),
                    Err(e) => {
                        if e.kind() != ErrorKind::WouldBlock {
                            self.clients.logout(client);
                            println!("Receive failed, dropped connection: {}", e);
                        }
                    }
                }
            }
        }
    }

    fn handle_client_login(
        &mut self,
        stream: TcpStream,
        login: ClientCommand,
    ) -> std::result::Result<ClientId, String> {
        if let ClientCommand::LogIn(user_name, public_key) = login {
            Ok(self.clients.login(public_key, user_name, stream))
        } else {
            Err(format!(
                "called login handler with wrong ClientCommand variant: {:?}",
                login
            ))
        }
    }

    fn handle_client_command(&mut self, client_id: ClientId, command: ClientCommand) {
        match command {
            ClientCommand::SendMessage(message) => {
                let other_clients = self
                    .clients
                    .active
                    .iter()
                    .filter(|&other| other != &client_id);

                let mut stale_clients = Vec::new();

                for &other in other_clients {
                    if let Some(ref mut stream) = self.clients.stream[other] {
                        let res =
                            protocol::send(stream, &ServerCommand::SendMessage(message.clone()));
                        if let Err(e) = res {
                            println!(
                                "Send failed on ServerCommand, client will be marked as stale: {:?}",
                                e
                            );
                            stale_clients.push(other);
                        }
                    }
                }

                for stale_client in stale_clients {
                    self.clients.logout(stale_client);
                }
            }
            // TODO
            _ => {
                println!("Invalid ClientCommand: {:?}", command);
            }
        }
    }
}
