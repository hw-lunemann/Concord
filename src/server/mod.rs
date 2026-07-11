mod clients;
mod net;

use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::{net::TcpListener, sync::mpsc};

use crate::{
    protocol::{self, ClientCommand, ServerCommand},
    server::{
        clients::{ClientId, Clients, ClientsHandle},
        net::{ClientCommandInternal, ServerCommandInternal, TcpConnection, TcpConnectionHandle},
    },
};

pub struct Server {
    clients: ClientsHandle,
    connections: HashMap<ClientId, TcpConnectionHandle>,
}

impl Server {
    pub fn new() -> Server {
        Server {
            clients: Clients::init(),
            connections: HashMap::new(),
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let (server_handle_tx, mut server_handle_rx) = mpsc::channel(32);

        Self::connection_listener_task(server_handle_tx.clone());

        while let Some(msg) = server_handle_rx.recv().await {
            match msg {
                ServerMessage::NewConnection(conn) => {
                    Self::login_listener_task(server_handle_tx.clone(), conn)
                }
                ServerMessage::Login(conn, login_command) => {
                    self.handle_client_login(server_handle_tx.clone(), conn, login_command)
                        .await
                }
                ServerMessage::Command(sender, command) => {
                    self.handle_client_command(sender, command).await
                }
                ServerMessage::Logout(client) => self.logout(client),
                ServerMessage::Error(error) => return Err(error),
            };
        }

        println!("All server tasks have exited.");

        Ok(())
    }

    async fn handle_client_login(
        &mut self,
        server_handle_tx: mpsc::Sender<ServerMessage>,
        conn: TcpConnectionHandle,
        login_command: protocol::LoginCommand,
    ) {
        let client = self.clients.register_or_update(login_command).await;
        self.connections.insert(client, conn.clone());
        Self::command_listener_task(server_handle_tx.clone(), conn, client);
    }

    fn connection_listener_task(server_handle_tx: mpsc::Sender<ServerMessage>) {
        tokio::spawn(async move {
            let server_handle_tx = server_handle_tx.clone();

            match TcpListener::bind("127.0.0.1:80").await {
                Ok(listener) => loop {
                    match listener.accept().await {
                        Ok(conn) => {
                            let conn_handle = TcpConnection::init(conn);
                            let _ = server_handle_tx
                                .send(ServerMessage::NewConnection(conn_handle))
                                .await;
                        }
                        Err(err) => println!("Failed to accept TCP connection: {}", err),
                    };
                },
                Err(err) => {
                    let err: anyhow::Error = err.into();
                    server_handle_tx
                        .send(ServerMessage::Error(
                            err.context("Failed to set up the connection listener."),
                        ))
                        .await
                }
            }
        });
    }

    fn login_listener_task(
        server_handle_tx: mpsc::Sender<ServerMessage>,
        conn: TcpConnectionHandle,
    ) {
        tokio::spawn(async move {
            match conn.receive_login().await {
                Ok(login_command) => {
                    if let Err(err) = server_handle_tx
                        .send(ServerMessage::Login(conn, login_command))
                        .await
                    {
                        println!("{err:#}");
                    };
                }
                Err(err) => {
                    println!("{err}");
                }
            }
        });
    }

    fn command_listener_task(
        server_handle_tx: mpsc::Sender<ServerMessage>,
        conn: TcpConnectionHandle,
        client: ClientId,
    ) {
        tokio::spawn(async move {
            while let Ok(command) = conn.receive().await {
                let _ = server_handle_tx
                    .send(ServerMessage::Command(client, command))
                    .await;
            }
            println!("Receive failed, dropping connection of {client}.");
            server_handle_tx
                .send(ServerMessage::Logout(client))
                .await
                .expect("Server task has exited.")
        });
    }

    async fn handle_client_command(&mut self, sender: usize, command: ClientCommandInternal) {
        match command {
            ClientCommandInternal::Logout() => self.logout(sender),
            ClientCommandInternal::SendMessage(message) => {
                self.broadcast(sender, message).await;
            }
            ClientCommandInternal::StartVoiceCall() => unimplemented!(),
            ClientCommandInternal::StartVideoCall() => unimplemented!(),
        }
    }

    async fn broadcast(&mut self, sender_id: ClientId, message: Arc<str>) {
        let sender = self.clients.get_client(sender_id).await.expect("");
        let sender_name: Arc<str> = sender.user_name.into();

        let other_clients = self
            .connections
            .iter_mut()
            .filter(|&(other, _)| other != &sender_id);

        let mut stale_clients = Vec::new();

        for (id, conn) in other_clients {
            let res = conn
                .send(ServerCommandInternal::SendMessage(
                    timestamp_secs(),
                    sender_name.clone(),
                    message.clone(),
                ))
                .await;
            if let Err(e) = res {
                println!(
                    "Send failed on ServerCommand, client will be marked as stale: {:?}",
                    e
                );
                stale_clients.push(id.clone());
            }
        }

        for client in stale_clients {
            self.logout(client);
        }
    }

    fn logout(&mut self, client: ClientId) {
        self.connections.remove(&client);
    }
}

enum ServerMessage {
    NewConnection(TcpConnectionHandle),
    Login(TcpConnectionHandle, protocol::LoginCommand),
    Logout(ClientId),
    Command(ClientId, ClientCommandInternal),
    Error(anyhow::Error),
}

fn timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("SystemTime::now < UNIX_EPOCH")
        .as_secs()
}
