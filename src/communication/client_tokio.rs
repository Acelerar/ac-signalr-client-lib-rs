use super::Communication;
use super::HttpClient;
use crate::client::Protocol;
use crate::execution::Storage;
use crate::execution::UpdatableActionStorage;
use crate::protocol::negotiate::MessageType;
use crate::protocol::HandshakeRequest;
use crate::protocol::MessageParser;
use crate::protocol::Ping;
use crate::protocol::RECORD_SEPARATOR;
use fastwebsockets::Frame;
use fastwebsockets::OpCode;
use fastwebsockets::WebSocket;
use http::Uri;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::interval;
use tokio::time::timeout;
use tokio::time::Duration;
use tracing::error;
use tracing::info;

const SIGNALR_PING_INTERVAL: Duration = Duration::from_secs(15);
const GRACEFUL_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

type ReconnectCallback =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;

enum WriterCommand {
    Send(Vec<u8>),
    Close {
        completion: oneshot::Sender<Result<(), String>>,
    },
}

struct CommunicationConnection {
    _generation: usize,
    _sender: UnboundedSender<WriterCommand>,
    _receiver_handle: Option<JoinHandle<()>>,
    _writer_handle: Option<JoinHandle<()>>,
    _protocol: Protocol,
}

impl CommunicationConnection {
    fn new(
        generation: usize,
        ws: WebSocket<TokioIo<Upgraded>>,
        storage: impl Storage + Send + 'static,
        protocol: Protocol,
        disconnect_tx: UnboundedSender<ConnectionEvent>,
    ) -> Self {
        let (tx, rx) = unbounded_channel();

        let mut connection = CommunicationConnection {
            _generation: generation,
            _sender: tx,
            _receiver_handle: None,
            _writer_handle: None,
            _protocol: protocol,
        };

        connection.start_split_tasks(ws, storage, rx, disconnect_tx);
        connection
    }

    fn start_split_tasks(
        &mut self,
        ws: WebSocket<TokioIo<Upgraded>>,
        mut storage: impl Storage + Send + 'static,
        mut rx: UnboundedReceiver<WriterCommand>,
        disconnect_tx: UnboundedSender<ConnectionEvent>,
    ) {
        // Use the unstable-split feature to split the WebSocket
        let (mut ws_reader, mut ws_writer) = ws.split(tokio::io::split);

        // Create a channel for the reader to send automatic frames (like pongs) to the writer
        let (auto_frame_tx, mut auto_frame_rx) = unbounded_channel::<Frame<'static>>();

        let protocol = self._protocol;
        let generation = self._generation;
        let reader_disconnect_tx = disconnect_tx.clone();
        let writer_disconnect_tx = disconnect_tx;

        // Start reader task - handles incoming messages
        let reader_handle = tokio::spawn(async move {
            let reason = loop {
                // Create a send function for automatic frame responses
                let auto_tx = auto_frame_tx.clone();
                let mut send_fn = move |frame: Frame<'static>| {
                    let _ = auto_tx.send(frame);
                    futures::future::ready(Ok::<_, std::io::Error>(()))
                };

                match ws_reader.read_frame(&mut send_fn).await {
                    Ok(frame) => match frame.opcode {
                        OpCode::Text => {
                            if let Ok(text) = String::from_utf8(frame.payload.to_vec()) {
                                for message in CommunicationClient::get_messages_text(text) {
                                    if let Ok(message_type) = read_signalr_message_type(
                                        message.as_bytes(),
                                        Protocol::Json,
                                    ) {
                                        if let Err(e) = storage.process_message(
                                            message.as_bytes(),
                                            message_type,
                                            Protocol::Json,
                                        ) {
                                            error!("Error occurred parsing message {}", e);
                                        }
                                    } else {
                                        error!("Message could not be parsed: {:?}", message);
                                    }
                                }
                            }
                        }
                        OpCode::Binary => {
                            let payload = frame.payload.to_vec();
                            for message_bytes in CommunicationClient::get_messages_binary(payload) {
                                if let Ok(message_type) =
                                    read_signalr_message_type(&message_bytes, protocol)
                                {
                                    if let Err(e) = storage.process_message(
                                        &message_bytes,
                                        message_type,
                                        protocol,
                                    ) {
                                        error!("Error occurred parsing message {}", e);
                                    }
                                } else {
                                    error!("Binary message could not be parsed");
                                }
                            }
                        }
                        OpCode::Close => {
                            info!("Received close frame");
                            break "Received close frame".to_string();
                        }
                        _ => {}
                    },
                    Err(e) => {
                        error!("Error reading frame: {}", e);
                        break format!("Error reading frame: {}", e);
                    }
                }
            };

            let _ =
                reader_disconnect_tx.send(ConnectionEvent::ConnectionLost { generation, reason });
        });

        // Start writer task - handles both outgoing messages and automatic frames
        let writer_handle = tokio::spawn(async move {
            let mut signalr_ping_interval = interval(SIGNALR_PING_INTERVAL);
            signalr_ping_interval.tick().await;

            let reason = loop {
                tokio::select! {
                    // Handle user messages
                    Some(command) = rx.recv() => {
                        match command {
                            WriterCommand::Send(data) => {
                                let frame = match protocol {
                                    Protocol::Json => Frame::text(fastwebsockets::Payload::Borrowed(&data)),
                                    Protocol::MessagePack => Frame::binary(fastwebsockets::Payload::Borrowed(&data)),
                                };

                                if let Err(e) = ws_writer.write_frame(frame).await {
                                    error!("Error writing frame: {}", e);
                                    break Some(format!("Error writing frame: {}", e));
                                }
                            }
                            WriterCommand::Close { completion } => {
                                let result = ws_writer
                                    .write_frame(Frame::close(1000, &[]))
                                    .await
                                    .map_err(|e| format!("Error writing close frame: {}", e));
                                let _ = completion.send(result.map(|_| ()));
                                break None;
                            }
                        }
                    }
                    // Handle automatic frames (like pongs)
                    Some(frame) = auto_frame_rx.recv() => {
                        if let Err(e) = ws_writer.write_frame(frame).await {
                            error!("Error writing automatic frame: {}", e);
                            break Some(format!("Error writing automatic frame: {}", e));
                        }
                    }
                    _ = signalr_ping_interval.tick() => {
                        let data = match serialize_signalr_ping(protocol) {
                            Ok(data) => data,
                            Err(e) => break Some(format!("SignalR ping serialization failed: {}", e)),
                        };

                        let frame = match protocol {
                            Protocol::Json => Frame::text(fastwebsockets::Payload::Borrowed(&data)),
                            Protocol::MessagePack => Frame::binary(fastwebsockets::Payload::Borrowed(&data)),
                        };

                        if let Err(e) = ws_writer.write_frame(frame).await {
                            error!("Error writing SignalR ping: {}", e);
                            break Some(format!("Error writing SignalR ping: {}", e));
                        }
                    }
                    else => break None,
                }
            };

            if let Some(reason) = reason {
                let _ = writer_disconnect_tx
                    .send(ConnectionEvent::ConnectionLost { generation, reason });
            }
        });

        self._receiver_handle = Some(reader_handle);
        self._writer_handle = Some(writer_handle);
    }

    fn send(&self, data: Vec<u8>) -> Result<(), String> {
        self._sender
            .send(WriterCommand::Send(data))
            .map_err(|e| format!("Failed to send: {}", e))
    }

    async fn close_gracefully(&self) -> Result<(), String> {
        let (completion_tx, completion_rx) = oneshot::channel();
        self._sender
            .send(WriterCommand::Close {
                completion: completion_tx,
            })
            .map_err(|e| format!("Failed to request graceful close: {}", e))?;

        timeout(GRACEFUL_CLOSE_TIMEOUT, completion_rx)
            .await
            .map_err(|_| "Timed out waiting to send close frame".to_string())?
            .map_err(|_| "Writer closed before acknowledging close frame".to_string())?
    }

    fn stop(&mut self) {
        if let Some(receiver) = self._receiver_handle.take() {
            receiver.abort();
        }
        if let Some(writer) = self._writer_handle.take() {
            writer.abort();
        }
    }
}

impl Drop for CommunicationConnection {
    fn drop(&mut self) {
        self.stop();
    }
}

fn read_signalr_message_type(message: &[u8], protocol: Protocol) -> Result<MessageType, String> {
    if protocol == Protocol::MessagePack && is_messagepack_ping(message) {
        return Ok(MessageType::Ping);
    }

    MessageParser::deserialize::<Ping>(message, protocol).map(|message| message.message_type())
}

fn serialize_signalr_ping(protocol: Protocol) -> Result<Vec<u8>, String> {
    match protocol {
        Protocol::Json => MessageParser::serialize(&Ping::new(), Protocol::Json),
        Protocol::MessagePack => Ok(vec![0x02, 0x91, 0x06]),
    }
}

fn is_messagepack_ping(message: &[u8]) -> bool {
    message == [0x02, 0x91, 0x06].as_slice() || message == [0x91, 0x06].as_slice()
}

#[derive(Debug)]
enum ConnectionEvent {
    ConnectionLost { generation: usize, reason: String },
    Shutdown,
}

enum ConnectionState {
    NotConnected,
    Reconnecting,
    Connected {
        generation: usize,
        connection: Arc<CommunicationConnection>,
    },
}

struct SharedCommunicationClient {
    _configuration: super::ConnectionData,
    _state: Mutex<ConnectionState>,
    _events: UnboundedSender<ConnectionEvent>,
    _shutdown: AtomicBool,
    _next_generation: AtomicUsize,
    _reconnecting_handler: Mutex<Option<ReconnectCallback>>,
    _reconnected_handler: Mutex<Option<ReconnectCallback>>,
}

pub struct CommunicationClient {
    _shared: Arc<SharedCommunicationClient>,
    _actions: UpdatableActionStorage,
    _protocol: Protocol,
}

impl Clone for CommunicationClient {
    fn clone(&self) -> Self {
        Self {
            _shared: self._shared.clone(),
            _actions: self._actions.clone(),
            _protocol: self._protocol,
        }
    }
}

impl Communication for CommunicationClient {
    async fn connect(configuration: &super::ConnectionData) -> Result<Self, String> {
        info!(
            "Creating communication client to {}",
            &configuration.get_endpoint()
        );

        let (events_tx, events_rx) = unbounded_channel();
        let actions = UpdatableActionStorage::new_with_deferred_message_capacity(
            configuration.get_deferred_message_capacity(),
        );
        let shared = Arc::new(SharedCommunicationClient {
            _configuration: configuration.clone(),
            _state: Mutex::new(ConnectionState::NotConnected),
            _events: events_tx,
            _shutdown: AtomicBool::new(false),
            _next_generation: AtomicUsize::new(1),
            _reconnecting_handler: Mutex::new(None),
            _reconnected_handler: Mutex::new(None),
        });

        let generation = shared._next_generation.fetch_add(1, Ordering::SeqCst);
        let connection = CommunicationClient::connect_socket(
            configuration,
            actions.clone(),
            shared._events.clone(),
            generation,
        )
        .await
        .map(Arc::new)?;

        {
            let mut state = shared
                ._state
                .lock()
                .map_err(|_| "Cannot lock connection state".to_string())?;
            *state = ConnectionState::Connected {
                generation,
                connection,
            };
        }

        CommunicationClient::spawn_reconnect_monitor(&shared, actions.clone(), events_rx);

        Ok(Self {
            _shared: shared,
            _actions: actions,
            _protocol: configuration.get_protocol(),
        })
    }

    fn get_storage(&self) -> Result<crate::execution::UpdatableActionStorage, String> {
        Ok(self._actions.clone())
    }

    async fn send<T: serde::Serialize>(&mut self, data: T) -> Result<(), String> {
        let (generation, connection) = self.current_connection()?;
        let bytes = MessageParser::serialize(&data, self._protocol)
            .map_err(|e| format!("Serialization failed: {}", e))?;

        if let Err(error) = connection.send(bytes) {
            let _ = self._shared._events.send(ConnectionEvent::ConnectionLost {
                generation,
                reason: error.clone(),
            });
            return Err(error);
        }

        Ok(())
    }

    async fn disconnect_gracefully(&mut self) -> Result<(), String> {
        self._actions.cancel_pending("Client disconnected");
        self._shared._shutdown.store(true, Ordering::SeqCst);

        let connection = {
            let state = self
                ._shared
                ._state
                .lock()
                .map_err(|_| "Cannot lock connection state".to_string())?;

            match &*state {
                ConnectionState::Connected { connection, .. } => Some(connection.clone()),
                ConnectionState::NotConnected | ConnectionState::Reconnecting => None,
            }
        };

        let close_result = if let Some(connection) = connection {
            info!("Sending graceful WebSocket close frame.");
            connection.close_gracefully().await
        } else {
            Ok(())
        };

        match self._shared._state.lock() {
            Ok(mut state) => {
                if !matches!(*state, ConnectionState::NotConnected) {
                    *state = ConnectionState::NotConnected;
                }
            }
            Err(_) => {
                error!("Cannot lock connection state");
            }
        }

        let _ = self._shared._events.send(ConnectionEvent::Shutdown);
        close_result
    }

    fn disconnect(&mut self) {
        let count = Arc::strong_count(&self._shared) - 1;
        if count > 0 {
            info!(
                "The underlying connection has {} more references, not disconnecting.",
                count
            );
            return;
        }

        self._shared._shutdown.store(true, Ordering::SeqCst);

        match self._shared._state.lock() {
            Ok(mut state) => {
                if matches!(*state, ConnectionState::NotConnected) {
                    info!("The client is not connected, cannot disconnect");
                } else {
                    info!("The underlying connection is going to be disposed.");
                    *state = ConnectionState::NotConnected;
                }
            }
            Err(_) => {
                error!("Cannot lock connection state");
            }
        }

        let _ = self._shared._events.send(ConnectionEvent::Shutdown);
    }
}

impl CommunicationClient {
    pub(crate) fn set_reconnecting_handler<F, Fut>(&self, callback: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let callback: ReconnectCallback = Arc::new(move || Box::pin(callback()));
        if let Ok(mut handler) = self._shared._reconnecting_handler.lock() {
            *handler = Some(callback);
        } else {
            error!("Cannot lock reconnecting callback");
        }
    }

    pub(crate) fn set_reconnected_handler<F, Fut>(&self, callback: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let callback: ReconnectCallback = Arc::new(move || Box::pin(callback()));
        if let Ok(mut handler) = self._shared._reconnected_handler.lock() {
            *handler = Some(callback);
        } else {
            error!("Cannot lock reconnected callback");
        }
    }

    fn current_connection(&self) -> Result<(usize, Arc<CommunicationConnection>), String> {
        let state = self
            ._shared
            ._state
            .lock()
            .map_err(|_| "Cannot lock connection state".to_string())?;

        match &*state {
            ConnectionState::NotConnected => {
                Err("Client is not connected, cannot send".to_string())
            }
            ConnectionState::Reconnecting => Err("Client is reconnecting, cannot send".to_string()),
            ConnectionState::Connected {
                generation,
                connection,
            } => Ok((*generation, connection.clone())),
        }
    }

    fn spawn_reconnect_monitor(
        shared: &Arc<SharedCommunicationClient>,
        actions: UpdatableActionStorage,
        events_rx: UnboundedReceiver<ConnectionEvent>,
    ) {
        let shared = Arc::downgrade(shared);
        tokio::spawn(async move {
            CommunicationClient::run_reconnect_monitor(shared, actions, events_rx).await;
        });
    }

    async fn run_reconnect_monitor(
        shared: Weak<SharedCommunicationClient>,
        mut actions: UpdatableActionStorage,
        mut events_rx: UnboundedReceiver<ConnectionEvent>,
    ) {
        while let Some(event) = events_rx.recv().await {
            let Some(shared) = shared.upgrade() else {
                break;
            };

            match event {
                ConnectionEvent::Shutdown => break,
                ConnectionEvent::ConnectionLost { generation, reason } => {
                    if shared._shutdown.load(Ordering::SeqCst) {
                        break;
                    }

                    let should_reconnect =
                        CommunicationClient::mark_connection_lost(&shared, generation, &reason);

                    if !should_reconnect {
                        continue;
                    }

                    actions.cancel_pending(&format!(
                        "Connection lost before the invocation completed: {}",
                        reason
                    ));

                    if !shared._configuration.get_reconnect_policy().enabled {
                        continue;
                    }

                    let reconnecting_handler = shared
                        ._reconnecting_handler
                        .lock()
                        .ok()
                        .and_then(|handler| handler.clone());
                    if let Some(callback) = reconnecting_handler {
                        callback().await;
                    }

                    CommunicationClient::reconnect_until_connected(shared, actions.clone()).await;
                }
            }
        }
    }

    fn mark_connection_lost(
        shared: &Arc<SharedCommunicationClient>,
        generation: usize,
        reason: &str,
    ) -> bool {
        match shared._state.lock() {
            Ok(mut state) => match &*state {
                ConnectionState::Connected {
                    generation: current_generation,
                    ..
                } if *current_generation == generation => {
                    error!("SignalR connection lost: {}", reason);
                    if shared._configuration.get_reconnect_policy().enabled {
                        *state = ConnectionState::Reconnecting;
                    } else {
                        *state = ConnectionState::NotConnected;
                    }
                    true
                }
                _ => false,
            },
            Err(_) => {
                error!("Cannot lock connection state");
                false
            }
        }
    }

    async fn reconnect_until_connected(
        shared: Arc<SharedCommunicationClient>,
        actions: UpdatableActionStorage,
    ) {
        let policy = shared._configuration.get_reconnect_policy();
        let mut attempt = 1;

        loop {
            if shared._shutdown.load(Ordering::SeqCst) {
                return;
            }

            if let Some(max_attempts) = policy.max_attempts {
                if attempt > max_attempts {
                    error!("SignalR reconnect attempts exhausted");
                    if let Ok(mut state) = shared._state.lock() {
                        *state = ConnectionState::NotConnected;
                    }
                    return;
                }
            }

            tokio::time::sleep(policy.delay_for_attempt(attempt)).await;

            if shared._shutdown.load(Ordering::SeqCst) {
                return;
            }

            match HttpClient::negotiate(shared._configuration.get_source_configuration()).await {
                Ok(configuration) => {
                    let generation = shared._next_generation.fetch_add(1, Ordering::SeqCst);
                    match CommunicationClient::connect_socket(
                        &configuration,
                        actions.clone(),
                        shared._events.clone(),
                        generation,
                    )
                    .await
                    {
                        Ok(connection) => {
                            if let Ok(mut state) = shared._state.lock() {
                                *state = ConnectionState::Connected {
                                    generation,
                                    connection: Arc::new(connection),
                                };
                            }
                            info!("SignalR reconnect succeeded on attempt {}", attempt);
                            let reconnected_handler = shared
                                ._reconnected_handler
                                .lock()
                                .ok()
                                .and_then(|handler| handler.clone());
                            if let Some(callback) = reconnected_handler {
                                callback().await;
                            }
                            return;
                        }
                        Err(error) => {
                            error!(
                                "SignalR reconnect socket attempt {} failed: {}",
                                attempt, error
                            );
                        }
                    }
                }
                Err(error) => {
                    error!(
                        "SignalR reconnect negotiation attempt {} failed: {}",
                        attempt, error
                    );
                }
            }

            attempt += 1;
        }
    }

    async fn connect_socket(
        configuration: &super::ConnectionData,
        actions: UpdatableActionStorage,
        events: UnboundedSender<ConnectionEvent>,
        generation: usize,
    ) -> Result<CommunicationConnection, String> {
        use hyper::Request;
        use hyper_util::rt::TokioIo;
        use tokio::net::TcpStream;

        let endpoint = Uri::from_str(&configuration.get_endpoint()).map_err(|e| {
            format!(
                "The endpoint URI {:?} is invalid: {}",
                configuration.get_endpoint(),
                e
            )
        })?;

        info!("Connecting to endpoint {}", endpoint);

        let host = endpoint.host().ok_or("No host in URI")?.to_string();
        let port = endpoint
            .port_u16()
            .unwrap_or(if endpoint.scheme_str() == Some("wss") {
                443
            } else {
                80
            });
        let addr = format!("{}:{}", host, port);

        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| format!("Failed to connect: {}", e))?;

        let is_wss = endpoint.scheme_str() == Some("wss");

        // Build path with query string for HTTP/1.1 request
        let path_and_query = endpoint
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");

        let mut req_builder = Request::builder()
            .method("GET")
            .uri(path_and_query)
            .header("Host", &host)
            .header("Upgrade", "websocket")
            .header("Connection", "Upgrade")
            .header(
                "Sec-WebSocket-Key",
                fastwebsockets::handshake::generate_key(),
            )
            .header("Sec-WebSocket-Version", "13");
        if let Some(header_value) =
            HttpClient::authorization_header(&configuration.get_authentication())
        {
            req_builder = req_builder.header("Authorization", header_value);
        }
        let req = req_builder
            .body(http_body_util::Empty::<bytes::Bytes>::new())
            .map_err(|e| format!("Failed to build request: {}", e))?;

        let res = if is_wss {
            // For WSS, wrap the TCP stream with TLS
            use rustls::pki_types::ServerName;
            use tokio_rustls::TlsConnector;

            let mut root_store = rustls::RootCertStore::empty();
            let certs = rustls_native_certs::load_native_certs();
            for cert in certs.certs {
                root_store
                    .add(cert)
                    .map_err(|e| format!("Failed to add cert: {}", e))?;
            }

            let config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();

            let connector = TlsConnector::from(Arc::new(config));
            let server_name = ServerName::try_from(host.to_string())
                .map_err(|e| format!("Invalid DNS name: {}", e))?;

            let tls_stream = connector
                .connect(server_name, stream)
                .await
                .map_err(|e| format!("TLS connection failed: {}", e))?;

            let io = TokioIo::new(tls_stream);
            let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
                .await
                .map_err(|e| format!("Handshake failed: {}", e))?;

            tokio::spawn(async move {
                if let Err(err) = conn.with_upgrades().await {
                    error!("Connection failed: {:?}", err);
                }
            });

            sender
                .send_request(req)
                .await
                .map_err(|e| format!("Failed to send request: {}", e))?
        } else {
            // For WS, use plain TCP
            let io = TokioIo::new(stream);
            let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
                .await
                .map_err(|e| format!("Handshake failed: {}", e))?;

            tokio::spawn(async move {
                if let Err(err) = conn.with_upgrades().await {
                    error!("Connection failed: {:?}", err);
                }
            });

            sender
                .send_request(req)
                .await
                .map_err(|e| format!("Failed to send request: {}", e))?
        };

        if res.status() != hyper::StatusCode::SWITCHING_PROTOCOLS {
            return Err(format!(
                "Server did not upgrade to websocket: {}",
                res.status()
            ));
        }

        let upgraded = hyper::upgrade::on(res)
            .await
            .map_err(|e| format!("Failed to upgrade: {}", e))?;

        let mut ws =
            WebSocket::after_handshake(TokioIo::new(upgraded), fastwebsockets::Role::Client);

        // Send handshake before splitting the connection
        info!("Initiating handshake...");
        let handshake = HandshakeRequest::new(configuration.get_protocol().as_str());
        let handshake_bytes = MessageParser::serialize(&handshake, Protocol::Json)
            .map_err(|e| format!("Handshake serialization failed: {}", e))?;
        ws.write_frame(Frame::text(fastwebsockets::Payload::Borrowed(
            &handshake_bytes,
        )))
        .await
        .map_err(|e| format!("Handshake send failed: {}", e))?;

        // Read handshake response
        let _handshake_response = ws
            .read_frame()
            .await
            .map_err(|e| format!("Handshake response read failed: {}", e))?;

        // Create connection with split WebSocket using unstable-split
        Ok(CommunicationConnection::new(
            generation,
            ws,
            actions,
            configuration.get_protocol(),
            events,
        ))
    }

    fn get_messages_text(message: String) -> Vec<String> {
        message
            .split(RECORD_SEPARATOR)
            .map(|s| MessageParser::strip_record_separator(s).to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    fn get_messages_binary(mut data: Vec<u8>) -> Vec<Vec<u8>> {
        let separator_bytes = RECORD_SEPARATOR.as_bytes();
        let mut messages = Vec::new();

        while let Some(pos) = data
            .windows(separator_bytes.len())
            .position(|window| window == separator_bytes)
        {
            let mut rest = data.split_off(pos);
            if !data.is_empty() {
                messages.push(std::mem::take(&mut data));
            }
            rest.drain(..separator_bytes.len());
            data = rest;
        }

        if !data.is_empty() {
            messages.push(data);
        }

        messages
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn json_client_ping_is_signalr_ping() {
        let response = serialize_signalr_ping(Protocol::Json).unwrap();
        let response = String::from_utf8(response).unwrap();

        assert_eq!(response, "{\"type\":6}\u{001e}");
    }

    #[test]
    fn messagepack_client_ping_is_signalr_ping() {
        let response = serialize_signalr_ping(Protocol::MessagePack).unwrap();

        assert_eq!(response, vec![0x02, 0x91, 0x06]);
    }

    #[test]
    fn messagepack_signalr_ping_type_is_detected() {
        let message_type =
            read_signalr_message_type(&[0x02, 0x91, 0x06], Protocol::MessagePack).unwrap();

        assert_eq!(message_type, MessageType::Ping);
    }
}
