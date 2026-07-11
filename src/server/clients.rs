use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot};

use crate::protocol::{self, LoginCommand};

pub type ClientId = usize;
pub type PublicKey = String;

#[derive(Default)]
pub struct Clients {
    // data
    public_key: Vec<Option<PublicKey>>,
    user_name: Vec<Option<String>>,
    // maps
    id_by_public_key: HashMap<PublicKey, ClientId>,
}

impl Clients {
    pub fn init() -> ClientsHandle {
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move { Clients::default().run(rx).await });
        ClientsHandle { inner: tx }
    }

    async fn run(mut self, mut rx: mpsc::Receiver<ClientsMessage>) {
        while let Some(msg) = rx.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&mut self, msg: ClientsMessage) {
        match msg {
            ClientsMessage::AddOrUpdate(login_data, respond_to) => {
                let id = self.register_or_update(login_data);
                let _ = respond_to.send(id);
            }
            ClientsMessage::Remove(client) => {
                self.remove(client);
            }
            ClientsMessage::GetClient(id, respond_to) => {
                let user_name = self.user_name[id].clone();
                let public_key = self.public_key[id].clone();
                let response = match (user_name, public_key) {
                    (Some(user_name), Some(public_key)) => Some(Client {
                        user_name,
                        public_key,
                    }),
                    _ => None,
                };
                let _ = respond_to.send(response);
            }
        }
    }

    fn next_id(&self) -> ClientId {
        self.public_key.len()
    }

    fn register_or_update(&mut self, login_data: LoginCommand) -> ClientId {
        if let Some(&old_id) = self.id_by_public_key.get(&login_data.public_key) {
            self.user_name[old_id] = Some(login_data.user_name);
            old_id
        } else {
            let id = self.next_id();
            self.public_key.push(Some(login_data.public_key.clone()));
            self.user_name.push(Some(login_data.user_name));
            self.id_by_public_key.insert(login_data.public_key, id);

            id
        }
    }

    fn remove(&mut self, client: ClientId) {
        if let Some(public_key) = &self.public_key[client] {
            self.id_by_public_key.remove(public_key);
        }
        self.public_key[client] = None;
        self.user_name[client] = None;
    }
}

#[derive(Clone)]
pub struct ClientsHandle {
    inner: mpsc::Sender<ClientsMessage>,
}

impl ClientsHandle {
    pub async fn register_or_update(&self, login_data: protocol::LoginCommand) -> ClientId {
        let (respond_to, response) = oneshot::channel();
        self.inner
            .send(ClientsMessage::AddOrUpdate(login_data, respond_to))
            .await
            .expect("Clients task has died.");

        response.await.expect("Client task has died.")
    }

    pub async fn remove(&self, id: ClientId) {
        self.inner
            .send(ClientsMessage::Remove(id))
            .await
            .expect("Clients task has died.");
    }

    pub async fn get_client(&self, id: ClientId) -> Option<Client> {
        let (respond_to, response) = oneshot::channel();
        self.inner
            .send(ClientsMessage::GetClient(id, respond_to))
            .await
            .expect("Clients task has died.");

        response.await.expect("Client task has died.")
    }
}

pub enum ClientsMessage {
    AddOrUpdate(protocol::LoginCommand, oneshot::Sender<ClientId>),
    Remove(ClientId),
    GetClient(ClientId, oneshot::Sender<Option<Client>>),
}

pub struct Client {
    pub user_name: String,
    pub public_key: PublicKey,
}
