use std::time::Duration;

/// Serialization protocol used for SignalR payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    /// JSON text frames using the standard SignalR JSON protocol.
    Json,
    /// MessagePack binary frames. The server must support the MessagePack hub protocol.
    MessagePack,
}

impl Protocol {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Protocol::Json => "json",
            Protocol::MessagePack => "messagepack",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Authentication {
    None,
    Basic {
        user: String,
        password: Option<String>,
    },
    Bearer {
        token: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReconnectPolicy {
    pub(crate) enabled: bool,
    pub(crate) initial_delay: Duration,
    pub(crate) max_delay: Duration,
    pub(crate) max_attempts: Option<usize>,
}

impl ReconnectPolicy {
    const DEFAULT_INITIAL_DELAY: Duration = Duration::from_secs(1);
    const DEFAULT_MAX_DELAY: Duration = Duration::from_secs(30);

    fn disabled() -> Self {
        Self {
            enabled: false,
            initial_delay: Self::DEFAULT_INITIAL_DELAY,
            max_delay: Self::DEFAULT_MAX_DELAY,
            max_attempts: None,
        }
    }

    pub(crate) fn delay_for_attempt(&self, attempt: usize) -> Duration {
        let multiplier = 1_u32
            .checked_shl(attempt.saturating_sub(1) as u32)
            .unwrap_or(u32::MAX);
        self.initial_delay
            .saturating_mul(multiplier)
            .min(self.max_delay)
    }
}

/// Connection options passed to [`crate::SignalRClient::connect_with`].
///
/// New configurations default to HTTPS/WSS, JSON serialization, no authentication,
/// and automatic reconnect disabled.
#[derive(Clone, Debug)]
pub struct ConnectionConfiguration {
    _secure: bool,
    _domain: String,
    _hub: String,
    _port: Option<i32>,
    _authentication: Authentication,
    _skip_negotiation: bool,
    _protocol: Protocol,
    _reconnect: ReconnectPolicy,
    _deferred_message_capacity: usize,
}

impl ConnectionConfiguration {
    pub(crate) fn new(domain: String, hub: String) -> Self {
        ConnectionConfiguration {
            _authentication: Authentication::None,
            _domain: domain,
            _secure: true,
            _hub: hub,
            _port: None,
            _skip_negotiation: false,
            _protocol: Protocol::Json,
            _reconnect: ReconnectPolicy::disabled(),
            _deferred_message_capacity: 4096,
        }
    }

    /// Uses a custom port when building the negotiation and websocket URLs.
    pub fn with_port(&mut self, port: i32) -> &ConnectionConfiguration {
        self._port = Some(port);
        self
    }

    /// Overrides the hub path used for negotiation and websocket connections.
    pub fn with_hub(&mut self, hub: String) -> &ConnectionConfiguration {
        self._hub = hub;
        self
    }

    /// Forces HTTPS/WSS transport selection.
    pub fn secure(&mut self) -> &ConnectionConfiguration {
        self._secure = true;
        self
    }

    /// Forces HTTP/WS transport selection.
    pub fn unsecure(&mut self) -> &ConnectionConfiguration {
        self._secure = false;
        self
    }

    /// Sends HTTP basic authentication credentials during negotiation.
    pub fn authenticate_basic(
        &mut self,
        user: String,
        password: Option<String>,
    ) -> &ConnectionConfiguration {
        self._authentication = Authentication::Basic { user, password };
        self
    }

    /// Sends a bearer token during negotiation.
    ///
    /// When [`Self::skip_negotiation`] is enabled, the token is sent as an
    /// `access_token` query parameter on the websocket URL.
    pub fn authenticate_bearer(&mut self, token: String) -> &ConnectionConfiguration {
        self._authentication = Authentication::Bearer { token };
        self
    }

    /// Connects directly to the websocket endpoint without the HTTP negotiation step.
    pub fn skip_negotiation(&mut self) -> &ConnectionConfiguration {
        self._skip_negotiation = true;
        self
    }

    /// Selects the SignalR protocol used for outgoing and incoming messages.
    pub fn with_protocol(&mut self, protocol: Protocol) -> &ConnectionConfiguration {
        self._protocol = protocol;
        self
    }

    /// Sets the number of server-to-client messages retained until a callback
    /// is registered. This covers messages sent immediately during connection
    /// setup, before the caller can register its handlers.
    pub fn with_deferred_message_capacity(&mut self, capacity: usize) -> &ConnectionConfiguration {
        self._deferred_message_capacity = capacity;
        self
    }

    /// Enables automatic reconnect after unexpected socket disconnects.
    pub fn with_auto_reconnect(&mut self) -> &ConnectionConfiguration {
        self._reconnect.enabled = true;
        self
    }

    /// Disables automatic reconnect.
    pub fn without_auto_reconnect(&mut self) -> &ConnectionConfiguration {
        self._reconnect.enabled = false;
        self
    }

    /// Configures exponential reconnect backoff.
    ///
    /// The first retry waits for `initial_delay`. Each later retry doubles the
    /// previous delay until it reaches `max_delay`.
    pub fn with_reconnect_delays(
        &mut self,
        initial_delay: Duration,
        max_delay: Duration,
    ) -> &ConnectionConfiguration {
        self._reconnect.initial_delay = initial_delay;
        self._reconnect.max_delay = max_delay.max(initial_delay);
        self
    }

    /// Limits the number of reconnect attempts made after an unexpected disconnect.
    pub fn with_max_reconnect_attempts(&mut self, max_attempts: usize) -> &ConnectionConfiguration {
        self._reconnect.max_attempts = Some(max_attempts);
        self
    }

    /// Removes the reconnect attempt cap.
    pub fn with_unlimited_reconnect_attempts(&mut self) -> &ConnectionConfiguration {
        self._reconnect.max_attempts = None;
        self
    }

    pub(crate) fn get_web_url(&self) -> String {
        format!(
            "{}://{}/{}",
            self.get_http_schema(),
            self.get_domain(),
            self._hub
        )
    }

    pub(crate) fn get_socket_url(&self) -> String {
        format!(
            "{}://{}/{}",
            self.get_socket_schema(),
            self.get_domain(),
            self._hub
        )
    }

    pub(crate) fn get_authentication(&self) -> Authentication {
        self._authentication.clone()
    }

    pub(crate) fn get_skip_negotiation(&self) -> bool {
        self._skip_negotiation
    }

    pub(crate) fn get_protocol(&self) -> Protocol {
        self._protocol
    }

    pub(crate) fn get_reconnect_policy(&self) -> ReconnectPolicy {
        self._reconnect.clone()
    }

    pub(crate) fn get_deferred_message_capacity(&self) -> usize {
        self._deferred_message_capacity
    }

    fn get_http_schema(&self) -> String {
        if self._secure {
            "https".to_string()
        } else {
            "http".to_string()
        }
    }

    fn get_socket_schema(&self) -> String {
        if self._secure {
            "wss".to_string()
        } else {
            "ws".to_string()
        }
    }

    fn get_domain(&self) -> String {
        match self._port {
            Some(port) => format!("{}:{}", self._domain, port),
            None => self._domain.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_configuration_defaults() {
        let config = ConnectionConfiguration::new("localhost".to_string(), "testhub".to_string());
        assert_eq!(config._domain, "localhost");
        assert_eq!(config._hub, "testhub");
        assert!(config._secure);
        assert!(config._port.is_none());
    }

    #[test]
    fn test_with_port() {
        let mut config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        config.with_port(8080);
        assert_eq!(config._port, Some(8080));
    }

    #[test]
    fn test_secure_unsecure() {
        let mut config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        config.unsecure();
        assert!(!config._secure);
        config.secure();
        assert!(config._secure);
    }

    #[test]
    fn test_with_hub() {
        let mut config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        config.with_hub("newhub".to_string());
        assert_eq!(config._hub, "newhub");
    }

    #[test]
    fn test_get_web_url_secure_no_port() {
        let config = ConnectionConfiguration::new("example.com".to_string(), "myhub".to_string());
        assert_eq!(config.get_web_url(), "https://example.com/myhub");
    }

    #[test]
    fn test_get_web_url_unsecure_with_port() {
        let mut config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        config.unsecure();
        config.with_port(5000);
        assert_eq!(config.get_web_url(), "http://localhost:5000/hub");
    }

    #[test]
    fn test_get_socket_url_secure() {
        let config = ConnectionConfiguration::new("example.com".to_string(), "hub".to_string());
        assert_eq!(config.get_socket_url(), "wss://example.com/hub");
    }

    #[test]
    fn test_get_socket_url_unsecure_with_port() {
        let mut config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        config.unsecure();
        config.with_port(8080);
        assert_eq!(config.get_socket_url(), "ws://localhost:8080/hub");
    }

    #[test]
    fn test_authenticate_basic() {
        let mut config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        config.authenticate_basic("user123".to_string(), Some("pass456".to_string()));
        match config.get_authentication() {
            Authentication::Basic { user, password } => {
                assert_eq!(user, "user123");
                assert_eq!(password, Some("pass456".to_string()));
            }
            _ => panic!("Expected Basic authentication"),
        }
    }

    #[test]
    fn test_authenticate_bearer() {
        let mut config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        config.authenticate_bearer("token123".to_string());
        match config.get_authentication() {
            Authentication::Bearer { token } => {
                assert_eq!(token, "token123");
            }
            _ => panic!("Expected Bearer authentication"),
        }
    }

    #[test]
    fn test_get_domain_with_port() {
        let mut config = ConnectionConfiguration::new("example.com".to_string(), "hub".to_string());
        config.with_port(3000);
        assert_eq!(config.get_domain(), "example.com:3000");
    }

    #[test]
    fn test_get_domain_without_port() {
        let config = ConnectionConfiguration::new("example.com".to_string(), "hub".to_string());
        assert_eq!(config.get_domain(), "example.com");
    }

    #[test]
    fn test_skip_negotiation_default() {
        let config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        assert!(!config.get_skip_negotiation());
    }

    #[test]
    fn test_skip_negotiation() {
        let mut config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        config.skip_negotiation();
        assert!(config.get_skip_negotiation());
    }

    #[test]
    fn test_with_protocol_json() {
        let mut config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        assert_eq!(config.get_protocol(), Protocol::Json); // Default should be JSON
        config.with_protocol(Protocol::Json);
        assert_eq!(config.get_protocol(), Protocol::Json);
    }

    #[test]
    fn test_deferred_message_capacity_defaults_to_4096() {
        let config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        assert_eq!(config.get_deferred_message_capacity(), 4096);
    }

    #[test]
    fn test_deferred_message_capacity_can_be_configured() {
        let mut config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        config.with_deferred_message_capacity(12);
        assert_eq!(config.get_deferred_message_capacity(), 12);
    }

    #[test]
    fn test_with_protocol_messagepack() {
        let mut config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        config.with_protocol(Protocol::MessagePack);
        assert_eq!(config.get_protocol(), Protocol::MessagePack);
    }

    #[test]
    fn test_protocol_as_str() {
        assert_eq!(Protocol::Json.as_str(), "json");
        assert_eq!(Protocol::MessagePack.as_str(), "messagepack");
    }

    #[test]
    fn test_reconnect_disabled_by_default() {
        let config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        let policy = config.get_reconnect_policy();
        assert!(!policy.enabled);
        assert_eq!(policy.initial_delay, Duration::from_secs(1));
        assert_eq!(policy.max_delay, Duration::from_secs(30));
        assert_eq!(policy.max_attempts, None);
    }

    #[test]
    fn test_enable_disable_auto_reconnect() {
        let mut config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        config.with_auto_reconnect();
        assert!(config.get_reconnect_policy().enabled);

        config.without_auto_reconnect();
        assert!(!config.get_reconnect_policy().enabled);
    }

    #[test]
    fn test_reconnect_delays_keep_max_at_least_initial() {
        let mut config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        config.with_reconnect_delays(Duration::from_secs(5), Duration::from_secs(1));

        let policy = config.get_reconnect_policy();
        assert_eq!(policy.initial_delay, Duration::from_secs(5));
        assert_eq!(policy.max_delay, Duration::from_secs(5));
    }

    #[test]
    fn test_reconnect_attempt_limit() {
        let mut config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        config.with_max_reconnect_attempts(3);
        assert_eq!(config.get_reconnect_policy().max_attempts, Some(3));

        config.with_unlimited_reconnect_attempts();
        assert_eq!(config.get_reconnect_policy().max_attempts, None);
    }

    #[test]
    fn test_reconnect_delay_backoff_is_capped() {
        let mut config = ConnectionConfiguration::new("localhost".to_string(), "hub".to_string());
        config.with_reconnect_delays(Duration::from_millis(10), Duration::from_millis(25));

        let policy = config.get_reconnect_policy();
        assert_eq!(policy.delay_for_attempt(1), Duration::from_millis(10));
        assert_eq!(policy.delay_for_attempt(2), Duration::from_millis(20));
        assert_eq!(policy.delay_for_attempt(3), Duration::from_millis(25));
        assert_eq!(policy.delay_for_attempt(30), Duration::from_millis(25));
    }
}
