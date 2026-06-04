# tt-signalr-client

A Rust SignalR client library using fastwebsockets for efficient WebSocket communication.

[![Current Version](https://img.shields.io/crates/v/tt-signalr-client)](https://crates.io/crates/tt-signalr-client)
[![Documentation](https://docs.rs/tt-signalr-client/badge.svg)](https://docs.rs/tt-signalr-client)
![license](https://shields.io/badge/license-MIT-blue)

## Overview

`tt-signalr-client` is a Rust library for calling SignalR hubs using [fastwebsockets](https://github.com/denoland/fastwebsockets) with the `unstable-split` feature. This implementation is inspired by [rust_signalr_client](https://github.com/danielleiszen/rust_signalr_client) but specifically designed for non-WASM targets with fastwebsockets as the WebSocket implementation.

Use `disconnect_gracefully().await` during application shutdown when you want the server to observe a clean WebSocket close instead of a dropped connection.

Read more about SignalR in the [official documentation](https://learn.microsoft.com/en-us/aspnet/core/signalr/introduction?view=aspnetcore-9.0).

## Features

- ✅ Async/await support with tokio
- ✅ Method invocation with arguments
- ✅ Streaming support for large data sets
- ✅ Callback registration for hub-to-client calls
- ✅ Authentication (Basic and Bearer)
- ✅ Connection configuration (secure/unsecure, custom ports)
- ✅ Opt-in automatic reconnect with configurable backoff
- ✅ Built with fastwebsockets for high performance
- ✅ MessagePack protocol support for efficient binary serialization

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
tt-signalr-client = "0.1.0"
```

## Documentation

- API reference: [docs.rs/tt-signalr-client](https://docs.rs/tt-signalr-client)
- Runnable examples: `examples/basic.rs`, `examples/messagepack.rs`, `examples/skip_negotiation.rs`, and `examples/secure_connection.rs`
- Local integration server: `servers/dotnet-chat-server/`

## Observability

This library emits diagnostics with [`tracing`](https://docs.rs/tracing).  
Initialize subscribers in your binary (not inside the library) so applications stay in control of global logging/tracing setup.

```rust
fn init_tracing() {
    let _ = tracing_log::LogTracer::init();
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}
```

## Usage Examples

### Basic Connection

```rust
use tt_signalr_client::SignalRClient;

#[tokio::main]
async fn main() {
    // Connect to SignalR server with default configuration
    let mut client = SignalRClient::connect("localhost", "test")
        .await
        .unwrap();

    // Disconnect when done
    client.disconnect();
}
```

### Custom Configuration

```rust
use tt_signalr_client::SignalRClient;

#[tokio::main]
async fn main() {
    let mut client = SignalRClient::connect_with("localhost", "test", |c| {
        c.with_port(5220);  // Set custom port
        c.unsecure();       // Use HTTP instead of HTTPS
    }).await.unwrap();

    client.disconnect();
}
```

### MessagePack Protocol

By default, the client uses JSON for serialization. You can use the more efficient MessagePack binary protocol instead:

```rust
use tt_signalr_client::{Protocol, SignalRClient};

#[tokio::main]
async fn main() {
    let mut client = SignalRClient::connect_with("localhost", "test", |c| {
        c.with_port(5220);
        c.unsecure();
        c.with_protocol(Protocol::MessagePack); // Use MessagePack instead of JSON
    }).await.unwrap();

    // All method invocations and callbacks now use MessagePack serialization
    client.disconnect();
}
```

MessagePack provides:
- **Smaller payload sizes**: Binary format is more compact than JSON
- **Faster serialization**: Binary encoding/decoding is typically faster
- **Better performance**: Reduced bandwidth and processing overhead

Note: Your SignalR server must also support the MessagePack protocol. For ASP.NET Core SignalR, add the `Microsoft.AspNetCore.SignalR.Protocols.MessagePack` NuGet package.

### Skip Negotiation

For servers that don't require the negotiation step, you can skip it and connect directly to the WebSocket:

```rust
use tt_signalr_client::SignalRClient;

#[tokio::main]
async fn main() {
    let mut client = SignalRClient::connect_with("localhost", "test", |c| {
        c.with_port(5220);
        c.unsecure();
        c.skip_negotiation(); // Skip HTTP negotiation
    }).await.unwrap();

    client.disconnect();
}
```

When using `skip_negotiation()` with bearer token authentication, the token is automatically added as an `access_token` query parameter:

```rust
use tt_signalr_client::SignalRClient;

#[tokio::main]
async fn main() {
    let mut client = SignalRClient::connect_with("localhost", "test", |c| {
        c.with_port(5220);
        c.unsecure();
        c.authenticate_bearer("your_token".to_string());
        c.skip_negotiation(); // Token added as ?access_token=your_token
    }).await.unwrap();

    client.disconnect();
}
```

### Automatic Reconnect

Automatic reconnect is opt-in so existing applications that handle disconnects themselves keep the same behavior. When enabled, the client reconnects in the background after an unexpected socket close and preserves registered callbacks for the new connection.

```rust
use std::time::Duration;
use tt_signalr_client::SignalRClient;

#[tokio::main]
async fn main() {
    let mut client = SignalRClient::connect_with("localhost", "test", |c| {
        c.with_port(5220);
        c.unsecure();
        c.with_auto_reconnect();
        c.with_reconnect_delays(Duration::from_secs(1), Duration::from_secs(30));
        c.with_unlimited_reconnect_attempts();
    }).await.unwrap();

    client.disconnect();
}
```

Pending invocations are completed with an error if the connection is lost before the server sends a completion, which prevents callers from waiting forever on a response that can no longer arrive.

### Invoking Methods

```rust
use tt_signalr_client::SignalRClient;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct TestEntity {
    text: String,
    number: i32,
}

#[tokio::main]
async fn main() {
    let mut client = SignalRClient::connect("localhost", "test").await.unwrap();

    // Invoke method without arguments
    let result: Result<TestEntity, String> = client
        .invoke("SingleEntity".to_string())
        .await;

    if let Ok(entity) = result {
        println!("Received entity: {}, {}", entity.text, entity.number);
    }

    // Invoke method with arguments
    let result = client.invoke_with_args::<bool, _>("PushEntity".to_string(), |c| {
        c.argument(TestEntity {
            text: "test".to_string(),
            number: 42,
        });
    }).await;

    client.disconnect();
}
```

### Streaming Data

```rust
use tt_signalr_client::SignalRClient;
use futures::StreamExt;

#[tokio::main]
async fn main() {
    let mut client = SignalRClient::connect("localhost", "test").await.unwrap();

    // Enumerate streaming results
    let mut stream = client.enumerate::<TestEntity>("GetEntities".to_string()).await;
    
    while let Some(entity) = stream.next().await {
        println!("Received: {}, {}", entity.text, entity.number);
    }

    client.disconnect();
}
```

### Registering Callbacks

```rust
use tt_signalr_client::{SignalRClient, InvocationContext};

#[tokio::main]
async fn main() {
    let mut client = SignalRClient::connect("localhost", "test").await.unwrap();

    // Register a callback
    let handler = client.register("OnMessage".to_string(), |ctx: InvocationContext| {
        if let Ok(message) = ctx.argument::<String>(0) {
            println!("Received message: {}", message);
        }
    });

    // ... do work ...

    // Unregister when done
    handler.unregister();
    client.disconnect();
}
```

### Callback with Response

```rust
use tt_signalr_client::{SignalRClient, InvocationContext};

#[tokio::main]
async fn main() {
    let mut client = SignalRClient::connect("localhost", "test").await.unwrap();

    let handler = client.register("GetData".to_string(), |mut ctx: InvocationContext| {
        if let Ok(request) = ctx.argument::<String>(0) {
            println!("Request: {}", request);
            
            // Respond asynchronously
            InvocationContext::spawn(async move {
                let response = format!("Response to: {}", request);
                let _ = ctx.complete(response).await;
            });
        }
    });

    client.disconnect();
}
```

### Authentication

```rust
use tt_signalr_client::SignalRClient;

#[tokio::main]
async fn main() {
    // Bearer token authentication
    let mut client = SignalRClient::connect_with("localhost", "test", |c| {
        c.authenticate_bearer("your_bearer_token".to_string());
    }).await.unwrap();

    // Or basic authentication
    let mut client2 = SignalRClient::connect_with("localhost", "test", |c| {
        c.authenticate_basic("username".to_string(), Some("password".to_string()));
    }).await.unwrap();

    client.disconnect();
}
```

## API Overview

### SignalRClient

The main client for connecting to and interacting with SignalR hubs.

#### Methods

- `connect(domain, hub)` - Connect with default configuration
- `connect_with(domain, hub, options)` - Connect with custom configuration
- `invoke<T>(target)` - Invoke a method and await response
- `invoke_with_args<T, F>(target, config)` - Invoke with arguments
- `send(target)` - Send message without awaiting response
- `send_with_args<F>(target, config)` - Send message with arguments
- `enumerate<T>(target)` - Get streaming results
- `enumerate_with_args<T, F>(target, config)` - Get streaming results with arguments
- `register(target, callback)` - Register callback for hub-to-client calls
- `disconnect()` - Close connection

### ConnectionConfiguration

Configure connection properties:

- `with_port(port)` - Set custom port
- `with_hub(hub)` - Set hub name
- `secure()` - Use HTTPS/WSS
- `unsecure()` - Use HTTP/WS
- `authenticate_basic(user, password)` - Basic authentication
- `authenticate_bearer(token)` - Bearer token authentication
- `skip_negotiation()` - Skip HTTP negotiation and connect directly to WebSocket
- `with_protocol(protocol)` - Set protocol (JSON or MessagePack, defaults to JSON)
- `with_auto_reconnect()` - Enable background reconnects after unexpected disconnects
- `without_auto_reconnect()` - Disable automatic reconnect behavior
- `with_reconnect_delays(initial, max)` - Configure exponential reconnect backoff
- `with_max_reconnect_attempts(max_attempts)` - Cap reconnect attempts
- `with_unlimited_reconnect_attempts()` - Remove the reconnect attempt cap

### Protocol

Available protocols for serialization:

- `Protocol::Json` - JSON text format (default)
- `Protocol::MessagePack` - Efficient binary format

### InvocationContext

Context for callback invocations:

- `argument<T>(index)` - Get argument by index
- `complete<T>(result)` - Send response to hub
- `spawn(future)` - Spawn async task (cross-platform)

## Implementation Details

- **WebSocket Library**: [fastwebsockets](https://github.com/denoland/fastwebsockets) with `unstable-split` feature
- **HTTP Client**: hyper for HTTP negotiation
- **Async Runtime**: tokio
- **No WASM Support**: Focused on native targets only

## Testing

A .NET 9.0 SignalR test server is included in the `servers/dotnet-chat-server/` directory for testing the client.

### Running the Test Server

1. Ensure you have [.NET 9.0 SDK](https://dotnet.microsoft.com/download/dotnet/9.0) installed
2. Start the server:
   ```bash
   cd servers/dotnet-chat-server
   dotnet run
   ```
3. In another terminal, run the example client:
   ```bash
   cargo run --example basic
   ```

The server will be available at `http://localhost:5000/chathub` and will echo messages back to all connected clients.

See `servers/dotnet-chat-server/README.md` for more details about the local test server.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgements

This project is inspired by [rust_signalr_client](https://github.com/danielleiszen/rust_signalr_client) by Daniel Leiszen. Special thanks for the great work on the original implementation.
