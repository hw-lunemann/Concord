mod clients;
mod net;
mod util;

use std::{collections::HashMap, sync::Arc};

use anyhow;
use tokio::sync::mpsc;

use crate::{
    protocol::{self, ClientCommand, ServerCommand},
    server::{
        clients::{ClientId, Clients, ClientsHandle},
        net::{Connection, ConnectionHandle, ConnectionListener, Initializing},
        util::timestamp_secs,
    },
};

pub type ServerHandle = mpsc::Sender<ServerMessage>;

pub struct Server {
    clients: ClientsHandle,
    connections: HashMap<ClientId, ConnectionHandle>,
}

impl Server {
    pub fn new() -> Server {
        Self {
            clients: Clients::init(),
            connections: HashMap::new(),
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let (server_handle_tx, mut server_handle_rx) = mpsc::channel(32);

        let listener = ConnectionListener::init("127.0.0.1:48584", server_handle_tx).await?;
        tokio::spawn(listener.run());

        while let Some(msg) = server_handle_rx.recv().await {
            match msg {
                ServerMessage::Login(conn, login_command) => self.login(conn, login_command).await,
                ServerMessage::Command(sender, command) => {
                    self.handle_client_command(sender, command).await
                }
                ServerMessage::Logout(client) => self.logout(client).await,
                ServerMessage::FatalError(error) => return Err(error),
            };
        }

        Ok(())
    }

    async fn login(
        &mut self,
        conn: Connection<Initializing>,
        login_command: protocol::LoginCommand,
    ) {
        let user_name = login_command.user_name.clone();
        let client = self.clients.register_or_update(login_command).await;
        let conn = conn.authenticate_as(client);
        self.connections.insert(client, conn);
        let recipients = self.connections.keys().filter(|&id| id != &client);
        self.broadcast(recipients, ServerCommand::UserConnected(user_name))
            .await;
    }

    async fn handle_client_command(&mut self, sender: usize, command: ClientCommand) {
        match command {
            ClientCommand::Logout() => self.logout(sender).await,
            ClientCommand::SendMessage(message) => self.handle_send_message(sender, message).await,
            _ => unimplemented!(),
        }
    }

    async fn handle_send_message(&self, sender: ClientId, message: String) {
        if let Some(name) = self.clients.get_name(sender).await {
            let recipients = self.connections.keys().filter(|&id| id != &sender);
            let message_command = ServerCommand::SendMessage(timestamp_secs(), name, message);
            self.broadcast(recipients, message_command).await;
        }
    }

    async fn broadcast(
        &self,
        recipients: impl IntoIterator<Item = &ClientId>,
        command: ServerCommand,
    ) {
        let command: Arc<[u8]> = Arc::from(command.serialize());

        for recipient in recipients {
            if let Some(connection) = self.connections.get(&recipient) {
                connection.send(command.clone()).await;
            }
        }
    }

    async fn logout(&mut self, client: ClientId) {
        self.connections.remove(&client);
        if let Some(sender_name) = self.clients.get_name(client).await {
            let recipients = self.connections.keys().filter(|&id| id != &client);
            self.broadcast(recipients, ServerCommand::UserDisconnected(sender_name))
                .await;
        };

        self.clients.remove(client).await;
    }
}

pub enum ServerMessage {
    Login(Connection<Initializing>, protocol::LoginCommand),
    Logout(ClientId),
    Command(ClientId, ClientCommand),
    FatalError(anyhow::Error),
}

impl ServerCommand {
    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(&self).expect("ServerCommand: Serialization Bug")
    }
}
