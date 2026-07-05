use std::{
    collections::{HashMap, HashSet},
    error::Error,
    net::{Shutdown, TcpStream},
};

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

    fn login(&mut self, public_key: PublicKey, user_name: String, stream: TcpStream) {
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
    }

    fn logout(&mut self, id: ClientId) {
        if let Some(stream) = &self.stream[id] {
            if let Err(e) = stream.shutdown(Shutdown::Both) {
                println!("Failed TcpStream::shutdown with: {:?}", e);
            }
        }
        self.stream[id] = None;
        self.active.remove(&id);
    }
}

struct Server {
    clients: Clients,
}

impl Server {
    fn new() -> Server {
        Server {
            clients: Clients::init(),
        }
    }

    fn handle_client_login(&mut self, stream: TcpStream, login: ClientCommand) {
        if let ClientCommand::LogIn(user_name, public_key) = login {
            self.clients.login(public_key, user_name, stream);
        } else {
            println!(
                "BUG: called login handler with wrong ClientCommand variant: {:?}",
                login
            );
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
