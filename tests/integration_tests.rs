#![allow(
    clippy::cast_lossless,
    clippy::indexing_slicing,
    clippy::needless_pass_by_value,
    clippy::redundant_closure_for_method_calls,
    clippy::unwrap_used,
    clippy::unused_async
)]

use ac_signalr_client::CallbackHandler;
use ac_signalr_client::InvocationContext;
use ac_signalr_client::SignalRClient;
use ac_signalr_client::RECORD_SEPARATOR;
use fastwebsockets::upgrade;
use fastwebsockets::Frame;
use fastwebsockets::OpCode;
use fastwebsockets::Payload;
use fastwebsockets::WebSocket;
use http_body_util::combinators::BoxBody;
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::body::Incoming;
use hyper::header::HeaderValue;
use hyper::header::CONTENT_TYPE;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::upgrade::Upgraded;
use hyper::Request;
use hyper::Response;
use hyper::StatusCode;
use hyper_util::rt::TokioIo;
use serde_json::json;
use serde_json::Value;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

type TestBody = BoxBody<Bytes, Infallible>;

#[derive(Default)]
struct TestHubServerOptions {
    close_first_connection_after_handshake: bool,
    close_first_connection_after_handshake_delay: Option<Duration>,
    send_callback_after_handshake: bool,
}

struct TestHubServerState {
    connections: AtomicUsize,
    negotiations: AtomicUsize,
    close_first_connection_after_handshake: bool,
    close_first_connection_after_handshake_delay: Option<Duration>,
    send_callback_after_handshake: bool,
    client_close_frames: AtomicUsize,
    received_targets: Mutex<Vec<String>>,
    completions: Mutex<Vec<Value>>,
}

struct TestHubServer {
    addr: SocketAddr,
    state: Arc<TestHubServerState>,
    task: JoinHandle<()>,
}

impl TestHubServer {
    async fn start(options: TestHubServerOptions) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(TestHubServerState {
            connections: AtomicUsize::new(0),
            negotiations: AtomicUsize::new(0),
            close_first_connection_after_handshake: options.close_first_connection_after_handshake,
            close_first_connection_after_handshake_delay: options
                .close_first_connection_after_handshake_delay,
            send_callback_after_handshake: options.send_callback_after_handshake,
            client_close_frames: AtomicUsize::new(0),
            received_targets: Mutex::new(Vec::new()),
            completions: Mutex::new(Vec::new()),
        });

        let server_state = state.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let state = server_state.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let service = service_fn(move |request| handle_request(request, state.clone()));
                    let connection = http1::Builder::new()
                        .serve_connection(io, service)
                        .with_upgrades();
                    let _ = connection.await;
                });
            }
        });

        Self { addr, state, task }
    }

    async fn connect_client(&self) -> SignalRClient {
        SignalRClient::connect_with("127.0.0.1", "hub", |config| {
            config.with_port(self.addr.port() as i32);
            config.unsecure();
        })
        .await
        .unwrap()
    }

    async fn connect_client_with_auto_reconnect(&self) -> SignalRClient {
        SignalRClient::connect_with("127.0.0.1", "hub", |config| {
            config.with_port(self.addr.port() as i32);
            config.unsecure();
            config.with_auto_reconnect();
            config.with_reconnect_delays(Duration::from_millis(5), Duration::from_millis(20));
        })
        .await
        .unwrap()
    }

    async fn wait_for_connection_count(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if self.state.connections.load(Ordering::SeqCst) >= expected {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    async fn wait_for_target(&self, expected: &str) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if self
                    .state
                    .received_targets
                    .lock()
                    .await
                    .iter()
                    .any(|target| target == expected)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    async fn wait_for_completion_result(&self, expected: &str) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if self
                    .state
                    .completions
                    .lock()
                    .await
                    .iter()
                    .any(|completion| completion["result"] == expected)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    async fn wait_for_close_frame_count(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if self.state.client_close_frames.load(Ordering::SeqCst) >= expected {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}

impl Drop for TestHubServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handle_request(
    mut request: Request<Incoming>,
    state: Arc<TestHubServerState>,
) -> Result<Response<TestBody>, Infallible> {
    if request.uri().path().ends_with("/negotiate") {
        state.negotiations.fetch_add(1, Ordering::SeqCst);
        return Ok(json_response(json!({
            "connectionId": "test-connection",
            "connectionToken": "test-token",
            "negotiateVersion": 1,
            "availableTransports": [
                {
                    "transport": "WebSockets",
                    "transferFormats": ["Text", "Binary"]
                }
            ]
        })));
    }

    if upgrade::is_upgrade_request(&request) {
        return match upgrade::upgrade(&mut request) {
            Ok((response, websocket)) => {
                tokio::spawn(handle_websocket(websocket, state));
                Ok(response.map(|body| body.boxed()))
            }
            Err(error) => Ok(text_response(
                StatusCode::BAD_REQUEST,
                format!("upgrade failed: {error}"),
            )),
        };
    }

    Ok(text_response(StatusCode::NOT_FOUND, "not found"))
}

async fn handle_websocket(websocket: upgrade::UpgradeFut, state: Arc<TestHubServerState>) {
    let Ok(mut websocket) = websocket.await else {
        return;
    };

    let connection_number = state.connections.fetch_add(1, Ordering::SeqCst) + 1;

    if websocket.read_frame().await.is_err() {
        return;
    }

    let _ = send_json(&mut websocket, json!({})).await;

    if state.send_callback_after_handshake {
        let _ = send_json(
            &mut websocket,
            json!({
                "type": 1,
                "target": "EarlyCallback",
                "arguments": ["sent-during-setup"]
            }),
        )
        .await;
    }

    if state.close_first_connection_after_handshake && connection_number == 1 {
        if let Some(delay) = state.close_first_connection_after_handshake_delay {
            tokio::time::sleep(delay).await;
        }
        let _ = websocket
            .write_frame(Frame::close(1000, b"reconnect test"))
            .await;
        return;
    }

    loop {
        let Ok(frame) = websocket.read_frame().await else {
            break;
        };

        match frame.opcode {
            OpCode::Text => {
                if let Ok(text) = String::from_utf8(frame.payload.to_vec()) {
                    handle_client_text(&mut websocket, &state, text).await;
                }
            }
            OpCode::Close => {
                state.client_close_frames.fetch_add(1, Ordering::SeqCst);
                break;
            }
            _ => {}
        }
    }
}

async fn handle_client_text(
    websocket: &mut WebSocket<TokioIo<Upgraded>>,
    state: &Arc<TestHubServerState>,
    text: String,
) {
    for message in text
        .split(RECORD_SEPARATOR)
        .filter(|message| !message.is_empty())
    {
        let Ok(value) = serde_json::from_str::<Value>(message) else {
            continue;
        };

        if value["type"] == 3 {
            state.completions.lock().await.push(value);
            continue;
        }

        let target = value["target"].as_str().unwrap_or_default().to_string();
        if !target.is_empty() {
            state.received_targets.lock().await.push(target.clone());
        }

        match target.as_str() {
            "Echo" => {
                send_completion(websocket, &value, json!("echo-result")).await;
            }
            "EchoArgs" => {
                let result = value["arguments"][0].clone();
                send_completion(websocket, &value, result).await;
            }
            "Fail" => {
                send_error_completion(websocket, &value, "server rejected invocation").await;
            }
            "StreamNumbers" => {
                send_stream_items(websocket, &value).await;
            }
            "StreamError" => {
                send_stream_error(websocket, &value).await;
            }
            "TriggerCallback" => {
                let _ = send_json(
                    websocket,
                    json!({
                        "type": 1,
                        "target": "ClientCallback",
                        "arguments": ["from-server"]
                    }),
                )
                .await;
            }
            "TriggerCompletion" => {
                let _ = send_json(
                    websocket,
                    json!({
                        "type": 1,
                        "invocationId": "server-invocation-1",
                        "target": "NeedsResponse",
                        "arguments": ["request"]
                    }),
                )
                .await;
            }
            "NoReplyAndClose" => {
                let _ = websocket.write_frame(Frame::close(1000, b"closed")).await;
            }
            _ => {}
        }
    }
}

async fn send_completion(
    websocket: &mut WebSocket<TokioIo<Upgraded>>,
    invocation: &Value,
    result: Value,
) {
    let Some(invocation_id) = invocation["invocationId"].as_str() else {
        return;
    };

    let _ = send_json(
        websocket,
        json!({
            "type": 3,
            "invocationId": invocation_id,
            "result": result
        }),
    )
    .await;
}

async fn send_error_completion(
    websocket: &mut WebSocket<TokioIo<Upgraded>>,
    invocation: &Value,
    error: &str,
) {
    let Some(invocation_id) = invocation["invocationId"].as_str() else {
        return;
    };

    let _ = send_json(
        websocket,
        json!({
            "type": 3,
            "invocationId": invocation_id,
            "error": error
        }),
    )
    .await;
}

async fn send_stream_items(websocket: &mut WebSocket<TokioIo<Upgraded>>, invocation: &Value) {
    let Some(invocation_id) = invocation["invocationId"].as_str() else {
        return;
    };

    for item in [1, 2, 3] {
        let _ = send_json(
            websocket,
            json!({
                "type": 2,
                "invocationId": invocation_id,
                "item": item
            }),
        )
        .await;
    }

    let _ = send_json(
        websocket,
        json!({
            "type": 3,
            "invocationId": invocation_id
        }),
    )
    .await;
}

async fn send_stream_error(websocket: &mut WebSocket<TokioIo<Upgraded>>, invocation: &Value) {
    let Some(invocation_id) = invocation["invocationId"].as_str() else {
        return;
    };

    let _ = send_json(
        websocket,
        json!({
            "type": 2,
            "invocationId": invocation_id,
            "item": 7
        }),
    )
    .await;

    let _ = send_json(
        websocket,
        json!({
            "type": 3,
            "invocationId": invocation_id,
            "error": "stream failed"
        }),
    )
    .await;
}

async fn send_json(
    websocket: &mut WebSocket<TokioIo<Upgraded>>,
    value: Value,
) -> Result<(), fastwebsockets::WebSocketError> {
    let mut bytes = serde_json::to_vec(&value).unwrap();
    bytes.extend_from_slice(RECORD_SEPARATOR.as_bytes());
    websocket
        .write_frame(Frame::text(Payload::Owned(bytes)))
        .await
}

fn json_response(value: Value) -> Response<TestBody> {
    let mut response = Response::new(Full::new(Bytes::from(value.to_string())).boxed());
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

fn text_response(status: StatusCode, text: impl Into<String>) -> Response<TestBody> {
    let mut response = Response::new(Full::new(Bytes::from(text.into())).boxed());
    *response.status_mut() = status;
    response
}

#[tokio::test]
async fn test_client_creation() {
    // This test just verifies the library exports are available
    // We can't actually connect without a real server
    let _ = std::mem::size_of::<SignalRClient>();
}

#[test]
fn test_manual_future() {
    use ac_signalr_client::ManualFuture;

    let (future, completer) = ManualFuture::<i32>::new();
    assert!(!future.is_completed());

    completer.complete(42);
    assert!(future.is_completed());
}

#[tokio::test]
async fn test_manual_future_await() {
    use ac_signalr_client::ManualFuture;

    let (future, completer) = ManualFuture::<String>::new();

    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        completer.complete("test".to_string());
    });

    let result = future.await;
    assert_eq!(result, "test");
}

#[tokio::test]
async fn test_manual_stream() {
    use ac_signalr_client::ManualStream;
    use futures::StreamExt;

    let (mut stream, completer) = ManualStream::<i32>::create();

    tokio::spawn(async move {
        for i in 0..5 {
            completer.push(i);
        }
        completer.close();
    });

    let mut results = Vec::new();
    while let Some(item) = stream.next().await {
        results.push(item);
    }

    assert_eq!(results, vec![0, 1, 2, 3, 4]);
}

#[test]
fn test_completed_future() {
    use ac_signalr_client::CompletedFuture;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;

    let mut future = CompletedFuture::new(100);
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);

    let result = Pin::new(&mut future).poll(&mut cx);
    match result {
        Poll::Ready(v) => assert_eq!(v, 100),
        Poll::Pending => panic!("Future should be ready immediately"),
    }
}

#[test]
fn test_record_separator() {
    use ac_signalr_client::RECORD_SEPARATOR;

    // Test that the record separator is defined
    assert_eq!(RECORD_SEPARATOR, "\u{001E}");
}

#[tokio::test]
async fn test_skip_negotiation_configuration() {
    // Test that skip_negotiation can be configured
    // We can't test actual connection without a server, but we can test the configuration
    use ac_signalr_client::SignalRClient;

    // This would skip negotiation if a server was available:
    // let mut client = SignalRClient::connect_with("localhost", "hub", |c| {
    //     c.skip_negotiation();
    // }).await;

    // For now, just verify the library compiles with the API
    let _ = std::mem::size_of::<SignalRClient>();
}

#[tokio::test]
async fn test_invoke_receives_completion() {
    let server = TestHubServer::start(TestHubServerOptions::default()).await;
    let mut client = server.connect_client().await;

    let result: String = client.invoke("Echo".to_string()).await.unwrap();

    assert_eq!(result, "echo-result");
    assert_eq!(server.state.negotiations.load(Ordering::SeqCst), 1);
    server.wait_for_target("Echo").await;
}

#[tokio::test]
async fn test_invoke_with_args_round_trips_argument() {
    let server = TestHubServer::start(TestHubServerOptions::default()).await;
    let mut client = server.connect_client().await;

    let result: String = client
        .invoke_with_args("EchoArgs".to_string(), |args| {
            args.argument("argument-value");
        })
        .await
        .unwrap();

    assert_eq!(result, "argument-value");
}

#[tokio::test]
async fn test_invoke_returns_server_completion_error() {
    let server = TestHubServer::start(TestHubServerOptions::default()).await;
    let mut client = server.connect_client().await;

    let error = client
        .invoke::<String>("Fail".to_string())
        .await
        .unwrap_err();

    assert_eq!(error, "server rejected invocation");
}

#[tokio::test]
async fn test_send_delivers_fire_and_forget_invocation() {
    let server = TestHubServer::start(TestHubServerOptions::default()).await;
    let mut client = server.connect_client().await;

    client.send("Notify".to_string()).await.unwrap();

    server.wait_for_target("Notify").await;
}

#[tokio::test]
async fn test_send_with_args_delivers_fire_and_forget_invocation() {
    let server = TestHubServer::start(TestHubServerOptions::default()).await;
    let mut client = server.connect_client().await;

    client
        .send_with_args("NotifyArgs".to_string(), |args| {
            args.argument(123);
        })
        .await
        .unwrap();

    server.wait_for_target("NotifyArgs").await;
}

#[tokio::test]
async fn test_enumerate_reads_stream_items_until_completion() {
    use futures::StreamExt;

    let server = TestHubServer::start(TestHubServerOptions::default()).await;
    let mut client = server.connect_client().await;

    let mut stream = client.enumerate::<i32>("StreamNumbers".to_string()).await;
    let mut values = Vec::new();
    while let Some(value) = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
    {
        values.push(value);
    }

    assert_eq!(values, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_enumerate_with_args_closes_on_server_error_completion() {
    use futures::StreamExt;

    let server = TestHubServer::start(TestHubServerOptions::default()).await;
    let mut client = server.connect_client().await;

    let mut stream = client
        .enumerate_with_args::<i32, _>("StreamError".to_string(), |args| {
            args.argument("subscription");
        })
        .await;
    let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap();

    assert_eq!(first, Some(7));
    assert_eq!(closed, None);
}

#[tokio::test]
async fn test_registered_callback_receives_server_invocation() {
    let server = TestHubServer::start(TestHubServerOptions::default()).await;
    let mut client = server.connect_client().await;
    let (tx, mut rx) = mpsc::unbounded_channel();

    let handler = client.register(
        "ClientCallback".to_string(),
        move |ctx: InvocationContext| {
            let message: String = ctx.argument(0).unwrap();
            let _ = tx.send(message);
        },
    );

    client.send("TriggerCallback".to_string()).await.unwrap();
    let message = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(message, "from-server");
    handler.unregister();
}

#[tokio::test]
async fn test_invocation_context_can_complete_server_invocation() {
    let server = TestHubServer::start(TestHubServerOptions::default()).await;
    let mut client = server.connect_client().await;

    let handler = client.register(
        "NeedsResponse".to_string(),
        move |mut ctx: InvocationContext| {
            InvocationContext::spawn(async move {
                let request: String = ctx.argument(0).unwrap();
                ctx.complete(format!("ack-{request}")).await.unwrap();
            });
        },
    );

    client.send("TriggerCompletion".to_string()).await.unwrap();
    server.wait_for_completion_result("ack-request").await;
    handler.unregister();
}

#[tokio::test]
async fn test_pending_invocation_returns_error_when_connection_closes() {
    let server = TestHubServer::start(TestHubServerOptions::default()).await;
    let mut client = server.connect_client().await;

    let error = client
        .invoke::<String>("NoReplyAndClose".to_string())
        .await
        .unwrap_err();

    assert!(error.contains("Connection lost before the invocation completed"));
}

#[tokio::test]
async fn test_auto_reconnect_restores_connection_after_close() {
    let server = TestHubServer::start(TestHubServerOptions {
        close_first_connection_after_handshake: true,
        ..TestHubServerOptions::default()
    })
    .await;
    let mut client = server.connect_client_with_auto_reconnect().await;

    server.wait_for_connection_count(2).await;
    let result: String = client.invoke("Echo".to_string()).await.unwrap();

    assert_eq!(result, "echo-result");
}

#[tokio::test]
async fn test_callback_received_before_registration_is_replayed() {
    let server = TestHubServer::start(TestHubServerOptions {
        send_callback_after_handshake: true,
        ..TestHubServerOptions::default()
    })
    .await;
    let mut client = server.connect_client().await;

    // Ensure the setup-time message has reached the reader before registering
    // the callback. The deferred storage must retain it for replay.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let handler = client.register(
        "EarlyCallback".to_string(),
        move |ctx: InvocationContext| {
            let message: String = ctx.argument(0).unwrap();
            let _ = tx.send(message);
        },
    );

    let message = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message, "sent-during-setup");
    handler.unregister();
}

#[tokio::test]
async fn test_reconnect_lifecycle_callbacks_run() {
    let server = TestHubServer::start(TestHubServerOptions {
        close_first_connection_after_handshake: true,
        close_first_connection_after_handshake_delay: Some(Duration::from_millis(100)),
        ..TestHubServerOptions::default()
    })
    .await;
    let mut client = server.connect_client_with_auto_reconnect().await;
    let reconnecting = Arc::new(AtomicUsize::new(0));
    let reconnected = Arc::new(AtomicUsize::new(0));

    let reconnecting_count = reconnecting.clone();
    client.on_reconnecting(move || {
        reconnecting_count.fetch_add(1, Ordering::SeqCst);
        async {}
    });

    let reconnected_count = reconnected.clone();
    client.on_reconnected(move || {
        reconnected_count.fetch_add(1, Ordering::SeqCst);
        async {}
    });

    server.wait_for_connection_count(2).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if reconnecting.load(Ordering::SeqCst) >= 1 && reconnected.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_reconnecting_callback_is_not_called_without_auto_reconnect() {
    let server = TestHubServer::start(TestHubServerOptions {
        close_first_connection_after_handshake: true,
        close_first_connection_after_handshake_delay: Some(Duration::from_millis(100)),
        ..TestHubServerOptions::default()
    })
    .await;
    let mut client = server.connect_client().await;
    let reconnecting = Arc::new(AtomicUsize::new(0));
    let reconnecting_count = reconnecting.clone();

    client.on_reconnecting(move || {
        reconnecting_count.fetch_add(1, Ordering::SeqCst);
        async {}
    });

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(reconnecting.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_skip_negotiation_connects_directly_to_websocket() {
    let server = TestHubServer::start(TestHubServerOptions::default()).await;
    let mut client = SignalRClient::connect_with("127.0.0.1", "hub", |config| {
        config.with_port(server.addr.port() as i32);
        config.unsecure();
        config.skip_negotiation();
    })
    .await
    .unwrap();

    let result: String = client.invoke("Echo".to_string()).await.unwrap();

    assert_eq!(result, "echo-result");
    assert_eq!(server.state.negotiations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_disconnect_is_idempotent() {
    let server = TestHubServer::start(TestHubServerOptions::default()).await;
    let client = server.connect_client().await;

    client.disconnect();
}

#[tokio::test]
async fn test_graceful_disconnect_sends_close_frame_with_registered_callback() {
    let server = TestHubServer::start(TestHubServerOptions::default()).await;
    let mut client = server.connect_client().await;
    let _handler = client.register("ClientCallback".to_string(), |_ctx: InvocationContext| {});

    client.disconnect_gracefully().await.unwrap();

    server.wait_for_close_frame_count(1).await;
}

#[test]
fn test_manual_future_cancel_marks_future_completed() {
    use ac_signalr_client::ManualFuture;

    let (future, completer) = ManualFuture::<i32>::new();
    assert!(!completer.is_completed());

    completer.cancel();

    assert!(future.is_completed());
}
