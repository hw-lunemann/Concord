type User = String;

enum ClientCommand {
    // Username, Public Key
    LogIn(String, String),
    // Message vom client zum server
    SendMessage(String),
    // TODO
    StartVoiceCall(),
    StartVideoCall()
}

enum ServerResponse {
    // Okay
    Ack(ClientCommand),
    // Command, Reason for error
    Error(ClientCommand, String)
}

enum ServerCommand {
    // Message vom server zum client
    SendMessage(String),
    // Connect new user
    UserConnected(User),
    // Disconnect user
    UserDisconnected(User)
}

enum ClientResponse {
    // Okay
    Ack(ServerCommand),
    // Command, Reason for error
    Error(ServerCommand, String)
}
