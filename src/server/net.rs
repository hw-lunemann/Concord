use std::{marker::PhantomData, net::SocketAddr, sync::Arc};

use anyhow::Context;
use serde::de::DeserializeOwned;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpStream, ToSocketAddrs,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::mpsc,
};

use crate::server::{ServerHandle, ServerMessage, clients::ClientId};

async fn send<Writer: AsyncWriteExt + Unpin, Data: AsRef<[u8]>>(
    writer: &mut Writer,
    data: Data,
) -> anyhow::Result<()> {
    let data = data.as_ref();
    writer.write_all(&(data.len() as u32).to_be_bytes()).await?;
    writer.write_all(data).await?;
    Ok(())
}

async fn receive<Reader: AsyncReadExt + Unpin, M: DeserializeOwned>(
    reader: &mut Reader,
) -> anyhow::Result<M> {
    // read header (just length of data for now)
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    // read data
    let mut buf = vec![0; len];
    reader.read_exact(&mut buf).await?;

    serde_json::from_slice(&buf).with_context(|| "Failed to deserialize command. Wrong type?")
}

// connection states
pub struct Initializing;
pub struct Authenticated;

pub struct Connection<State> {
    pub stream: TcpStream,
    pub addr: SocketAddr,
    server: ServerHandle,
    state: PhantomData<State>,
}

impl Connection<Initializing> {
    pub fn init(tcp_connection: (TcpStream, SocketAddr), server: ServerHandle) -> Self {
        println!("New connection from: {}", tcp_connection.1);
        Self {
            stream: tcp_connection.0,
            addr: tcp_connection.1,
            server: server,
            state: PhantomData,
        }
    }

    pub async fn listen_for_login(mut self) {
        match receive(&mut self.stream).await {
            Ok(login_data) => {
                let _ = self
                    .server
                    .clone()
                    .send(ServerMessage::Login(self, login_data))
                    .await;
            }
            Err(err) => println!(
                "Failed to receive login command from {}: {}",
                self.addr, err
            ),
        }
    }

    pub fn authenticate_as(self, client: ClientId) -> ConnectionHandle {
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            Connection::<Authenticated> {
                stream: self.stream,
                state: PhantomData,
                server: self.server,
                addr: self.addr.clone(),
            }
            .run(rx, client)
            .await;
        });

        ConnectionHandle { inner: tx }
    }
}

impl Connection<Authenticated> {
    pub async fn run(
        self,
        rx: mpsc::Receiver<impl AsRef<[u8]> + Send + 'static>,
        client: ClientId,
    ) {
        let (reader, writer) = self.stream.into_split();

        let mut read_task = tokio::spawn(Self::read_loop(
            reader,
            self.server.clone(),
            client,
            self.addr,
        ));
        let mut write_task = tokio::spawn(Self::write_loop(writer, rx, self.addr));

        tokio::select! {
            _ = &mut read_task => write_task.abort(),
            _ = &mut write_task => read_task.abort()
        };

        self.server
            .send(ServerMessage::Logout(client))
            .await
            .expect("Server task exited.");
    }

    async fn read_loop(
        mut reader: OwnedReadHalf,
        server: mpsc::Sender<ServerMessage>,
        client: ClientId,
        addr: SocketAddr,
    ) {
        loop {
            match receive(&mut reader).await {
                Ok(command) => {
                    let _ = server.send(ServerMessage::Command(client, command)).await;
                }
                Err(err) => {
                    return println!("Connection with {addr} broken: {err}");
                }
            }
        }
    }

    async fn write_loop(
        mut writer: OwnedWriteHalf,
        mut rx: mpsc::Receiver<impl AsRef<[u8]> + Send>,
        addr: SocketAddr,
    ) {
        while let Some(data) = rx.recv().await {
            if let Err(err) = send(&mut writer, data).await {
                return println!("Connection with {addr} broken: {err}");
            }
        }
        println!("No more listeners for connection with {addr}");
    }
}

#[derive(Clone)]
pub struct ConnectionHandle {
    inner: mpsc::Sender<Arc<[u8]>>,
}

impl ConnectionHandle {
    pub async fn send(&self, data: Arc<[u8]>) {
        let _ = self.inner.send(data).await;
    }
}

pub struct ConnectionListener {
    inner: TcpListener,
    server: ServerHandle,
}

impl ConnectionListener {
    pub async fn init<A: ToSocketAddrs>(addr: A, server: ServerHandle) -> std::io::Result<Self> {
        let inner = TcpListener::bind(addr).await?;
        Ok(Self { inner, server })
    }

    pub async fn run(self) {
        loop {
            match self.inner.accept().await {
                Ok(conn) => {
                    tokio::spawn(Connection::init(conn, self.server.clone()).listen_for_login());
                }
                Err(err) => println!("Failed to accept TCP connection: {}", err),
            };
        }
    }
}
