use std::{net::SocketAddr, sync::Arc};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, oneshot},
};

use crate::protocol::{ClientCommand, LoginCommand, ServerCommand, User};

pub struct TcpConnection {
    pub stream: TcpStream,
    pub addr: SocketAddr,
}

impl TcpConnection {
    pub fn init(conn: (TcpStream, SocketAddr)) -> TcpConnectionHandle {
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            Self {
                stream: conn.0,
                addr: conn.1,
            }
            .run(rx)
            .await
        });

        TcpConnectionHandle { inner: tx }
    }

    async fn run(mut self, mut rx: mpsc::Receiver<TcpConnectionMessage>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                TcpConnectionMessage::Send(command, respond_to) => {
                    let res = self.send(command).await;
                    let _ = respond_to.send(res);
                }
                TcpConnectionMessage::Login(respond_to) => {
                    let res = self.receive::<LoginCommand>().await;
                    let _ = respond_to.send(res);
                }
                TcpConnectionMessage::Receive(respond_to) => {
                    let res = self.receive::<ClientCommandInternal>().await;
                    let _ = respond_to.send(res);
                }
            }
        }
    }

    async fn send<M: Serialize>(&mut self, message: M) -> std::io::Result<()> {
        let data = serde_json::to_vec(&message)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let len = (data.len() as u32).to_be_bytes();

        self.stream.write_all(&len).await?;
        self.stream.write_all(&data).await?;

        Ok(())
    }

    async fn receive<M: DeserializeOwned>(&mut self) -> std::io::Result<M> {
        // read header (just length of data for now)
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        // read data
        let mut buf = vec![0; len];
        self.stream.read_exact(&mut buf).await?;

        let msg = serde_json::from_slice(&buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(msg)
    }
}

#[derive(Clone)]
pub struct TcpConnectionHandle {
    inner: mpsc::Sender<TcpConnectionMessage>,
}

impl TcpConnectionHandle {
    pub async fn send(&self, command: ServerCommandInternal) -> std::io::Result<()> {
        let (respond_to, response) = oneshot::channel();
        let _ = self
            .inner
            .send(TcpConnectionMessage::Send(command, respond_to))
            .await;
        response.await.expect("TcpConnection task died.")
    }

    pub async fn receive(&self) -> std::io::Result<ClientCommandInternal> {
        let (respond_to, response) = oneshot::channel();
        let _ = self
            .inner
            .send(TcpConnectionMessage::Receive(respond_to))
            .await;
        response.await.expect("TcpConnection task died.")
    }

    pub async fn receive_login(&self) -> std::io::Result<LoginCommand> {
        let (respond_to, response) = oneshot::channel();
        let _ = self
            .inner
            .send(TcpConnectionMessage::Login(respond_to))
            .await;
        response.await.expect("TcpConnection task died.")
    }
}

enum TcpConnectionMessage {
    Send(ServerCommandInternal, oneshot::Sender<std::io::Result<()>>),
    Login(oneshot::Sender<std::io::Result<LoginCommand>>),
    Receive(oneshot::Sender<std::io::Result<ClientCommandInternal>>),
}

#[derive(Deserialize, Debug)]
pub enum ClientCommandInternal {
    // Username, Public Key
    Logout(),
    // Message vom client zum server
    SendMessage(Arc<str>),
    // TODO
    StartVoiceCall(),
    StartVideoCall(),
}

impl From<ClientCommand> for ClientCommandInternal {
    fn from(cmd: ClientCommand) -> Self {
        match cmd {
            ClientCommand::Logout() => ClientCommandInternal::Logout(),
            ClientCommand::SendMessage(data) => ClientCommandInternal::SendMessage(data.into()),
            ClientCommand::StartVoiceCall() => ClientCommandInternal::StartVoiceCall(),
            ClientCommand::StartVideoCall() => ClientCommandInternal::StartVideoCall(),
        }
    }
}

#[derive(Serialize, Debug)]
pub enum ServerCommandInternal {
    // Message vom server zum client
    SendMessage(u64, Arc<str>, Arc<str>),
    // Connect new user
    UserConnected(User),
    // Disconnect user
    UserDisconnected(User),
}

impl From<ServerCommand> for ServerCommandInternal {
    fn from(cmd: ServerCommand) -> Self {
        match cmd {
            ServerCommand::SendMessage(time, msg, name) => {
                ServerCommandInternal::SendMessage(time, msg.into(), name.into())
            }
            ServerCommand::UserConnected(user) => ServerCommandInternal::UserConnected(user.into()),
            ServerCommand::UserDisconnected(user) => {
                ServerCommandInternal::UserDisconnected(user.into())
            }
        }
    }
}
