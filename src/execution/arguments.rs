use serde::Serialize;
use tracing::error;

use crate::protocol::invoke::Invocation;

/// Builder passed to invocation configuration closures for appending arguments.
pub struct ArgumentConfiguration {
    invocation: Option<Invocation>,
}

impl ArgumentConfiguration {
    pub(crate) fn new(invocation: Invocation) -> Self {
        Self {
            invocation: Some(invocation),
        }
    }

    /// Serializes and appends one argument to the invocation payload.
    ///
    /// Serialization failures are logged and leave the invocation unchanged.
    pub fn argument<T: Serialize>(&mut self, value: T) -> &mut ArgumentConfiguration {
        if let Some(invocation) = self.invocation.as_mut() {
            if invocation.with_argument(value).is_err() {
                error!("Argument could not be put into invocation data.");
            }
        }

        self
    }

    pub(crate) fn build_invocation(mut self) -> Invocation {
        if let Some(invocation) = self.invocation.take() {
            invocation
        } else {
            panic!("Invocation cannot be built before it is provided");
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn test_new_argument_configuration() {
        let inv = Invocation::create_single("TestMethod");
        let config = ArgumentConfiguration::new(inv);
        assert!(config.invocation.is_some());
    }

    #[test]
    fn test_argument_single() {
        let inv = Invocation::create_single("TestMethod");
        let mut config = ArgumentConfiguration::new(inv);
        config.argument("test string");

        let built = config.build_invocation();
        assert_eq!(built.arguments.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_argument_multiple() {
        let inv = Invocation::create_single("TestMethod");
        let mut config = ArgumentConfiguration::new(inv);
        config.argument("string").argument(42).argument(true);

        let built = config.build_invocation();
        assert_eq!(built.arguments.as_ref().unwrap().len(), 3);
    }

    #[test]
    fn test_argument_chaining() {
        let inv = Invocation::create_single("TestMethod");
        let mut config = ArgumentConfiguration::new(inv);
        config.argument("first");
        config.argument(100);
        let built = config.build_invocation();

        assert_eq!(built.arguments.as_ref().unwrap().len(), 2);
        assert_eq!(built.get_target(), "TestMethod");
    }

    #[test]
    #[should_panic(expected = "Invocation cannot be built before it is provided")]
    fn test_build_without_invocation_panics() {
        let config = ArgumentConfiguration { invocation: None };
        config.build_invocation();
    }
}
