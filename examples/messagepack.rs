#![allow(clippy::ignored_unit_patterns, clippy::uninlined_format_args, clippy::unwrap_used)]

use ac_signalr_client::Protocol;
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

    println!("Connecting to SignalR server with MessagePack protocol...");

    // Connect to SignalR server with MessagePack protocol
    let mut client = match SignalRClient::connect_with("localhost", "chathub", |c| {
        c.with_port(5000);
        c.unsecure(); // Use HTTP for local development
        c.with_protocol(Protocol::MessagePack); // Use MessagePack instead of JSON
    })
    .await
    {
        Ok(client) => {
            println!("Connected successfully with MessagePack protocol!");
            client
        }
        Err(e) => {
            eprintln!("Failed to connect: {}", e);
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
                text: "Hello from Rust with MessagePack!".to_string(),
                timestamp: chrono::Utc::now().timestamp(),
            });
        })
        .await
    {
        Ok(_) => println!("✓ Message sent successfully"),
        Err(e) => eprintln!("✗ Failed to send message: {}", e),
    }

    // Keep the connection alive for a bit to receive responses
    println!("Waiting for messages...");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    println!("Disconnecting...");
    client.disconnect_gracefully().await.unwrap();
    println!("Disconnected.");
}
