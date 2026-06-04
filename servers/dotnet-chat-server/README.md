# .NET SignalR Chat Server

A simple .NET 9.0 SignalR server for testing the tt-signalr-client Rust library.

## Requirements

- [.NET 9.0 SDK](https://dotnet.microsoft.com/download/dotnet/9.0) or later

## Running the Server

```bash
cd servers/dotnet-chat-server
dotnet run
```

The server will start on `http://localhost:5000` with the SignalR hub available at `/chathub`.

## Hub Methods

### SendMessage

Receives a message from the client and broadcasts it to all connected clients.

**Input:**
```json
{
  "text": "Hello from client",
  "timestamp": 1234567890
}
```

**Broadcast:** Sends the same message to all clients via `ReceiveMessage`.

## Testing with the Rust Client

1. Start the server:
   ```bash
   cd servers/dotnet-chat-server
   dotnet run
   ```

2. In another terminal, run the Rust client:
   ```bash
   cd ../..
   cargo run --example basic
   ```

You should see:
- Server logs showing the client connection and message receipt
- Client logs showing the connection, message send, and message receipt (echoed back)

## Hub Endpoints

- **Hub Path:** `/chathub`
- **URL:** `http://localhost:5000/chathub`

## Client Methods (called by server)

- `ReceiveMessage(message)` - Receives a message broadcast from the server

## Server Methods (called by client)

- `SendMessage(message)` - Sends a message to the server for broadcasting
