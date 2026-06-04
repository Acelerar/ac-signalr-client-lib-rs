#!/bin/bash

# Integration test script for tt-signalr-client
# This script starts the .NET test server and runs the Rust client

set -e

echo "========================================="
echo "SignalR Client Integration Test"
echo "========================================="
echo ""

# Check if dotnet is installed
if ! command -v dotnet &> /dev/null; then
    echo "Error: dotnet is not installed"
    echo "Please install .NET 9.0 SDK from https://dotnet.microsoft.com/download/dotnet/9.0"
    exit 1
fi

# Check if cargo is installed
if ! command -v cargo &> /dev/null; then
    echo "Error: cargo is not installed"
    echo "Please install Rust from https://rustup.rs"
    exit 1
fi

echo "Starting .NET SignalR test server..."
cd servers/dotnet-chat-server
dotnet build --nologo -v quiet

# Start the server in the background
dotnet run --no-build &
SERVER_PID=$!

# Give the server time to start
echo "Waiting for server to start..."
sleep 3

cd ../..

echo ""
echo "Running Rust client example..."
echo ""

# Set environment variable for logging
export RUST_LOG=info

# Run the client
cargo run --example basic

echo ""
echo "Test completed!"

# Kill the server
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null || true

echo "Done!"
