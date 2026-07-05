use concord::server::Server;

fn main() {
    let mut server = Server::new();
    if let Err(e) = server.run() {
        println!("Server error: {}", e);
    }
}
