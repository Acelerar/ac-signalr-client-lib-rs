use crate::client::Authentication;
use crate::client::ConnectionConfiguration;
use crate::client::Protocol;
use crate::client::ReconnectPolicy;
use crate::execution::UpdatableActionStorage;
use crate::protocol::negotiate::NegotiateResponseV0;
use base64::engine::general_purpose;
use base64::Engine;
use serde::de::DeserializeOwned;
use serde::Serialize;

const WEB_SOCKET_TRANSPORT: &str = "WebSockets";
const TEXT_TRANSPORT_FORMAT: &str = "Text";
const BINARY_TRANSPORT_FORMAT: &str = "Binary";

#[derive(Clone, Debug)]
pub struct ConnectionData {
    endpoint: String,
    connection_id: String,
    protocol: Protocol,
    authentication: Authentication,
    source_configuration: ConnectionConfiguration,
}

impl ConnectionData {
    pub fn new(
        endpoint: String,
        connection_id: String,
        protocol: Protocol,
        authentication: Authentication,
        source_configuration: ConnectionConfiguration,
    ) -> Self {
        ConnectionData {
            endpoint,
            connection_id,
            protocol,
            authentication,
            source_configuration,
        }
    }

    pub fn get_endpoint(&self) -> String {
        self.endpoint.clone()
    }

    #[allow(dead_code)]
    pub fn get_connection_id(&self) -> String {
        self.connection_id.clone()
    }

    pub fn get_protocol(&self) -> Protocol {
        self.protocol
    }

    pub fn get_authentication(&self) -> Authentication {
        self.authentication.clone()
    }

    pub(crate) fn get_source_configuration(&self) -> ConnectionConfiguration {
        self.source_configuration.clone()
    }

    pub(crate) fn get_reconnect_policy(&self) -> ReconnectPolicy {
        self.source_configuration.get_reconnect_policy()
    }
}

pub trait Communication: Clone {
    async fn connect(configuration: &ConnectionData) -> Result<Self, String>;
    async fn send<T: Serialize>(&mut self, data: T) -> Result<(), String>;
    fn get_storage(&self) -> Result<UpdatableActionStorage, String>;
    async fn disconnect_gracefully(&mut self) -> Result<(), String>;
    fn disconnect(&mut self);
}

pub struct HttpClient;

impl HttpClient {
    pub(crate) async fn negotiate(
        options: ConnectionConfiguration,
    ) -> Result<ConnectionData, String> {
        if options.get_skip_negotiation() {
            // Skip negotiation and return direct WebSocket endpoint
            let mut endpoint = options.get_socket_url();

            // When skipping negotiation with bearer token, add it as access_token query parameter
            if let Authentication::Bearer { token } = options.get_authentication() {
                endpoint = format!("{}?access_token={}", endpoint, token);
            }

            Ok(ConnectionData::new(
                endpoint,
                String::new(), // Empty connection ID when skipping negotiation
                options.get_protocol(),
                options.get_authentication(),
                options,
            ))
        } else {
            let negotiate_endpoint =
                format!("{}/negotiate?negotiateVersion=1", options.get_web_url());
            let negotiation = HttpClient::post::<NegotiateResponseV0>(
                negotiate_endpoint.clone(),
                options.get_authentication(),
            )
            .await?;

            HttpClient::create_configuration(
                options.get_socket_url(),
                negotiation,
                options.get_protocol(),
                options.get_authentication(),
                options,
            )
            .ok_or_else(|| {
                "The negotiation concluded no matching communication protocols".to_string()
            })
        }
    }

    fn create_configuration(
        endpoint: String,
        negotiate: NegotiateResponseV0,
        protocol: Protocol,
        authentication: Authentication,
        source_configuration: ConnectionConfiguration,
    ) -> Option<ConnectionData> {
        let transfer_format = match protocol {
            Protocol::Json => TEXT_TRANSPORT_FORMAT,
            Protocol::MessagePack => BINARY_TRANSPORT_FORMAT,
        };
        let fit = negotiate
            .available_transports
            .iter()
            .find(|i| i.transport == WEB_SOCKET_TRANSPORT)
            .and_then(|i| {
                i.transfer_formats
                    .iter()
                    .find(|j| j.as_str() == transfer_format)
            })
            .is_some();

        if fit {
            // Use connection_token if available (negotiateVersion 1+), otherwise use connection_id
            let id_param = negotiate
                .connection_token
                .as_ref()
                .unwrap_or(&negotiate.connection_id);

            // Append connection ID/token to the WebSocket endpoint as required by SignalR protocol
            let endpoint_with_id = format!("{}?id={}", endpoint, id_param);
            Some(ConnectionData::new(
                endpoint_with_id,
                negotiate.connection_id,
                protocol,
                authentication,
                source_configuration,
            ))
        } else {
            None
        }
    }

    fn basic_auth(username: &str, password: Option<&str>) -> String {
        let mut ret = String::new();

        if let Some(password) = password {
            general_purpose::STANDARD.encode_string(format!("{username}:{password}"), &mut ret);
        } else {
            general_purpose::STANDARD.encode_string(format!("{username}:"), &mut ret);
        }

        format!("Basic {}", &ret)
    }

    pub(crate) fn authorization_header(authentication: &Authentication) -> Option<String> {
        match authentication {
            Authentication::None => None,
            Authentication::Basic { user, password } => {
                Some(HttpClient::basic_auth(user, password.as_deref()))
            }
            Authentication::Bearer { token } => Some(format!("Bearer {}", token)),
        }
    }

    pub async fn post<T: 'static + DeserializeOwned + Send>(
        endpoint: String,
        authentication: Authentication,
    ) -> Result<T, String> {
        use http::Uri;
        use hyper::Request;
        use hyper::StatusCode;
        use hyper_util::rt::TokioIo;
        use tokio::net::TcpStream;

        let uri: Uri = endpoint
            .parse()
            .map_err(|e| format!("Invalid URI: {}", e))?;

        let host = uri.host().ok_or("No host in URI")?;
        let port = uri
            .port_u16()
            .unwrap_or(if uri.scheme_str() == Some("https") {
                443
            } else {
                80
            });
        let addr = format!("{}:{}", host, port);

        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| format!("Connection failed: {}", e))?;

        let is_https = uri.scheme_str() == Some("https");

        // Build path with query string for HTTP/1.1 request
        let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");

        let mut req = Request::builder()
            .method("POST")
            .uri(path_and_query)
            .header("Host", host)
            .header("Content-Length", "0");

        if let Some(header_value) = HttpClient::authorization_header(&authentication) {
            req = req.header("Authorization", header_value);
        }

        let req = req
            .body(http_body_util::Empty::<bytes::Bytes>::new())
            .map_err(|e| format!("Request build failed: {}", e))?;

        let res = if is_https {
            // For HTTPS, wrap the TCP stream with TLS
            use rustls::pki_types::ServerName;
            use std::sync::Arc;
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
                if let Err(err) = conn.await {
                    tracing::error!("Connection failed: {:?}", err);
                }
            });

            sender
                .send_request(req)
                .await
                .map_err(|e| format!("Request failed: {}", e))?
        } else {
            // For HTTP, use plain TCP
            let io = TokioIo::new(stream);
            let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
                .await
                .map_err(|e| format!("Handshake failed: {}", e))?;

            tokio::spawn(async move {
                if let Err(err) = conn.await {
                    tracing::error!("Connection failed: {:?}", err);
                }
            });

            sender
                .send_request(req)
                .await
                .map_err(|e| format!("Request failed: {}", e))?
        };

        if res.status() != StatusCode::OK {
            return Err(format!("HTTP error: {}", res.status()));
        }

        let body_bytes = http_body_util::BodyExt::collect(res.into_body())
            .await
            .map_err(|e| format!("Failed to read body: {}", e))?
            .to_bytes();

        let text =
            String::from_utf8(body_bytes.to_vec()).map_err(|e| format!("Invalid UTF-8: {}", e))?;

        serde_json::from_str::<T>(&text).map_err(|e| format!("Deserialization failed: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ConnectionConfiguration;
    use crate::protocol::negotiate::TransportSpec;

    #[tokio::test]
    async fn test_skip_negotiation_with_bearer_token() {
        let mut config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        config.skip_negotiation();
        config.authenticate_bearer("test_token_123".to_string());
        config.with_port(5000);
        config.unsecure();

        let result = HttpClient::negotiate(config).await;
        assert!(result.is_ok());

        let connection_data = result.unwrap();
        let endpoint = connection_data.get_endpoint();

        // Should include access_token query parameter when skip_negotiation is used with bearer token
        assert!(endpoint.contains("access_token=test_token_123"));
        assert!(endpoint.starts_with("ws://localhost:5000/hub"));
    }

    #[tokio::test]
    async fn test_skip_negotiation_without_auth() {
        let mut config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        config.skip_negotiation();
        config.with_port(5000);
        config.unsecure();

        let result = HttpClient::negotiate(config).await;
        assert!(result.is_ok());

        let connection_data = result.unwrap();
        let endpoint = connection_data.get_endpoint();

        // Should not include access_token query parameter when no bearer token
        assert!(!endpoint.contains("access_token"));
        assert_eq!(endpoint, "ws://localhost:5000/hub");
    }

    #[tokio::test]
    async fn test_secure_skip_negotiation_uses_wss() {
        let mut config = ConnectionConfiguration::new("example.com".to_string(), "hub".to_string());
        config.skip_negotiation();
        config.secure(); // Explicitly use secure connection

        let result = HttpClient::negotiate(config).await;
        assert!(result.is_ok());

        let connection_data = result.unwrap();
        let endpoint = connection_data.get_endpoint();

        // Should use wss:// scheme for secure WebSocket
        assert!(endpoint.starts_with("wss://example.com/hub"));
    }

    #[test]
    fn test_create_configuration_accepts_text_for_json() {
        let config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        let negotiation = NegotiateResponseV0 {
            connection_id: "connection-id".to_string(),
            connection_token: Some("connection-token".to_string()),
            negotiate_version: 1,
            available_transports: vec![TransportSpec {
                transport: WEB_SOCKET_TRANSPORT.to_string(),
                transfer_formats: vec![TEXT_TRANSPORT_FORMAT.to_string()],
            }],
        };

        let data = HttpClient::create_configuration(
            "ws://localhost/hub".to_string(),
            negotiation,
            Protocol::Json,
            Authentication::None,
            config,
        )
        .unwrap();

        assert_eq!(
            data.get_endpoint(),
            "ws://localhost/hub?id=connection-token"
        );
        assert_eq!(data.get_connection_id(), "connection-id");
    }

    #[test]
    fn test_create_configuration_accepts_binary_for_messagepack() {
        let config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        let negotiation = NegotiateResponseV0 {
            connection_id: "connection-id".to_string(),
            connection_token: None,
            negotiate_version: 1,
            available_transports: vec![TransportSpec {
                transport: WEB_SOCKET_TRANSPORT.to_string(),
                transfer_formats: vec![BINARY_TRANSPORT_FORMAT.to_string()],
            }],
        };

        let data = HttpClient::create_configuration(
            "ws://localhost/hub".to_string(),
            negotiation,
            Protocol::MessagePack,
            Authentication::None,
            config,
        )
        .unwrap();

        assert_eq!(data.get_endpoint(), "ws://localhost/hub?id=connection-id");
        assert_eq!(data.get_protocol(), Protocol::MessagePack);
    }

    #[test]
    fn test_create_configuration_rejects_text_for_messagepack() {
        let config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        let negotiation = NegotiateResponseV0 {
            connection_id: "connection-id".to_string(),
            connection_token: None,
            negotiate_version: 1,
            available_transports: vec![TransportSpec {
                transport: WEB_SOCKET_TRANSPORT.to_string(),
                transfer_formats: vec![TEXT_TRANSPORT_FORMAT.to_string()],
            }],
        };

        let data = HttpClient::create_configuration(
            "ws://localhost/hub".to_string(),
            negotiation,
            Protocol::MessagePack,
            Authentication::None,
            config,
        );

        assert!(data.is_none());
    }
}
