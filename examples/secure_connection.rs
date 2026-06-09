#![allow(clippy::ignored_unit_patterns, clippy::uninlined_format_args, clippy::unwrap_used)]

use ac_signalr_client::SignalRClient;
use serde::Deserialize;
use serde::Serialize;

fn init_tracing(default_filter: &str) {
    let _ = tracing_log::LogTracer::init();
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Message {
    text: String,
    timestamp: i64,
}

#[tokio::main]
async fn main() {
    init_tracing("info");

    println!("Connecting to SignalR server with SSL/TLS (HTTPS/WSS)...");
    println!("Note: This example requires a server with HTTPS/WSS support.");
    println!(
        "For testing with a local server without SSL, use the 'basic' example with .unsecure()"
    );
    println!();

    // Example: Connect to a SignalR server with SSL/TLS support
    // By default, connections are secure (HTTPS for negotiation, WSS for WebSocket)
    let mut client = match SignalRClient::connect_with("your-server.com", "chathub", |c| {
        // Optional: specify custom port (defaults to 443 for secure connections)
        c.with_port(443);
        // Optional: add authentication
        c.authenticate_bearer("your_bearer_token".to_string());
        // .secure() is the default, no need to call it explicitly
        // But you can call it for clarity:
        c.secure();
    })
    .await
    {
        Ok(client) => {
            println!("✓ Connected successfully via HTTPS/WSS!");
            client
        }
        Err(e) => {
            eprintln!("✗ Failed to connect: {}", e);
            eprintln!();
            eprintln!("This example requires a server with SSL/TLS support.");
            eprintln!("Make sure:");
            eprintln!("  1. The server hostname is correct");
            eprintln!("  2. The server has a valid SSL certificate");
            eprintln!("  3. The server is running and accessible");
            eprintln!();
            eprintln!("For local testing without SSL, use:");
            eprintln!("  cargo run --example basic");
            return;
        }
    };

    // Register a callback to receive messages from the server
    let _handler = client.register("ReceiveMessage".to_string(), |ctx| {
        if let Ok(message) = ctx.argument::<Message>(0) {
            println!("📨 Received: {} at {}", message.text, message.timestamp);
        }
    });

    // Send a message to the server
    println!("Sending message...");
    match client
        .send_with_args("SendMessage".to_string(), |c| {
            c.argument(Message {
                text: "Hello from Rust via SSL/TLS!".to_string(),
                timestamp: chrono::Utc::now().timestamp(),
            });
        })
        .await
    {
        Ok(_) => println!("Message sent!"),
        Err(e) => eprintln!("Failed to send message: {}", e),
    }

    // Keep the connection alive for a bit
    println!("Waiting for messages... (Press Ctrl+C to exit)");
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

    // Disconnect
    println!("Disconnecting...");
    client.disconnect_gracefully().await.unwrap();
    println!("Done!");
}
