use concord::server::Server;

#[tokio::main]
async fn main() {
    let server = Server::new();
    if let Err(e) = server.run().await {
        println!("Server error: {:#}", e);
    }
    println!("Stopping Server");
}
