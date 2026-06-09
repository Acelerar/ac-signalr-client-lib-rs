#![allow(
    clippy::ignored_unit_patterns,
    clippy::struct_field_names,
    clippy::uninlined_format_args,
    clippy::unwrap_used
)]

use ac_signalr_client::SignalRClient;
use std::time::Duration;
use serde::Deserialize;
use serde::Serialize;

fn init_tracing(default_filter: &str) {
    let _ = tracing_log::LogTracer::init();
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum GatewayTrade {
    Symbol(String),
    Trades(Vec<Trade>),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Trade {
    #[serde(rename = "symbolId")]
    pub symbol_id: String,
    pub price: f64,
    pub timestamp: String, // You can use chrono::DateTime if you want to parse it
    #[serde(rename = "type")]
    pub trade_type: u8,
    pub volume: u32,
}

#[tokio::main]
async fn main() {
    init_tracing("trace");

    println!("Connecting to SignalR server...");

    let token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1laWRlbnRpZmllciI6IjcxNzAiLCJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9zaWQiOiJiNGU4YmYxYi1kOTFkLTQwNGQtOTZkOS1kYjJmZTJmNjEyZjMiLCJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1lIjoiZHQtNjA5MCIsImh0dHA6Ly9zY2hlbWFzLm1pY3Jvc29mdC5jb20vd3MvMjAwOC8wNi9pZGVudGl0eS9jbGFpbXMvcm9sZSI6InVzZXIiLCJodHRwOi8vc2NoZW1hcy5taWNyb3NvZnQuY29tL3dzLzIwMDgvMDYvaWRlbnRpdHkvY2xhaW1zL2F1dGhlbnRpY2F0aW9ubWV0aG9kIjoiYXBpLWtleSIsIm1zZCI6IkNNRUdST1VQX1RPQiIsIm1mYSI6InZlcmlmaWVkIiwiZXhwIjoxNzYwNjk5MzE0fQ.yI7uUvZQNPDNT2RkVcWhjXboddQH-98Y7E7jwZ8SEGA".to_string();
    let contract_id = "CON.F.US.MNQ.Z25";

    // Connect to SignalR server
    let mut client =
        match SignalRClient::connect_with("rtc.daytraders.projectx.com", "hubs/market", |c| {
            c.skip_negotiation();
            c.secure();
            c.authenticate_bearer(token.clone());
        })
        .await
        {
            Ok(client) => {
                println!("Connected successfully!");
                client
            }
            Err(e) => {
                eprintln!("Failed to connect: {}", e);
                return;
            }
        };

    // Register a callback to receive messages from the server
    let _handler = client.register("GatewayTrade".to_string(), |ctx| {
        if let Ok(argument) = ctx.argument::<GatewayTrade>(1) {
            match argument {
                GatewayTrade::Symbol(symbol) => println!("Symbol: {}", symbol),
                GatewayTrade::Trades(trades) => {
                    for trade in trades {
                        println!(
                            "Trade: symbol={}, price={}, timestamp={}, type={}, volume={}",
                            trade.symbol_id,
                            trade.price,
                            trade.timestamp,
                            trade.trade_type,
                            trade.volume
                        );
                    }
                }
            }
        }
    });

    let _handler = client.register("GatewayDepth".to_string(), |ctx| {
        if let Ok(argument) = ctx.argument::<GatewayTrade>(1) {
            match argument {
                GatewayTrade::Symbol(symbol) => println!("Symbol: {}", symbol),
                GatewayTrade::Trades(trades) => {
                    for trade in trades {
                        println!(
                            "Trade: symbol={}, price={}, timestamp={}, type={}, volume={}",
                            trade.symbol_id,
                            trade.price,
                            trade.timestamp,
                            trade.trade_type,
                            trade.volume
                        );
                    }
                }
            }
        }
    });

    match client
        .send_with_args("SubscribeContractTrades".to_string(), |c| {
            c.argument(contract_id);
        })
        .await
    {
        Ok(_) => println!("Message sent!"),
        Err(e) => eprintln!("Failed to send message: {}", e),
    }

    match client
        .send_with_args("SubscribeContractMarketDepth".to_string(), |c| {
            c.argument(contract_id);
        })
        .await
    {
        Ok(_) => println!("Message sent!"),
        Err(e) => eprintln!("Failed to send message: {}", e),
    }

    loop {
        std::thread::sleep(Duration::from_millis(100));
    }
}
