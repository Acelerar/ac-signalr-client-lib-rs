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

    println!("Connecting to SignalR server with skip_negotiation...");

    // Example 1: Connect with skip_negotiation (no auth)
    println!("\n=== Example 1: Skip negotiation without authentication ===");
    let mut client = match SignalRClient::connect_with("localhost", "chathub", |c| {
        c.with_port(5000);
        c.unsecure(); // Use HTTP for local development
        c.skip_negotiation(); // Skip negotiation and connect directly to WebSocket
    })
    .await
    {
        Ok(client) => {
            println!("Connected successfully (with skip_negotiation, no auth)!");
            client
        }
        Err(e) => {
            eprintln!("Failed to connect: {}", e);
            println!("\nTrying Example 2 with bearer token...\n");

            // Example 2: Connect with skip_negotiation and bearer token
            // When using skip_negotiation with bearer token, the token is automatically
            // added as an access_token query parameter to the WebSocket URL
            println!("=== Example 2: Skip negotiation with bearer token ===");
            match SignalRClient::connect_with("localhost", "chathub", |c| {
                c.with_port(5000);
                c.unsecure();
                c.authenticate_bearer("your_bearer_token".to_string());
                c.skip_negotiation(); // Token will be added as ?access_token=your_bearer_token
            })
            .await
            {
                Ok(client) => {
                    println!("Connected successfully (with skip_negotiation and bearer token)!");
                    client
                }
                Err(e) => {
                    eprintln!("Failed to connect with bearer token: {}", e);
                    return;
                }
            }
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
                text: "Hello from Rust (skip_negotiation)!".to_string(),
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
