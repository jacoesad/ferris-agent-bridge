use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    Fatal,
    Recoverable,
    UserVisible,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeError {
    class: ErrorClass,
    message: String,
}

impl RuntimeError {
    pub fn fatal(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Fatal, message)
    }

    pub fn recoverable(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Recoverable, message)
    }

    pub fn user_visible(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::UserVisible, message)
    }

    pub fn new(class: ErrorClass, message: impl Into<String>) -> Self {
        Self {
            class,
            message: message.into(),
        }
    }

    pub fn class(&self) -> ErrorClass {
        self.class
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn is_fatal(&self) -> bool {
        self.class == ErrorClass::Fatal
    }

    pub fn is_recoverable(&self) -> bool {
        self.class == ErrorClass::Recoverable
    }

    pub fn is_user_visible(&self) -> bool {
        self.class == ErrorClass::UserVisible
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.class, self.message)
    }
}

impl Error for RuntimeError {}

#[cfg(test)]
mod tests {
    use super::{ErrorClass, RuntimeError};

    #[test]
    fn classifies_runtime_errors() {
        let error = RuntimeError::recoverable("event decode failed");

        assert_eq!(error.class(), ErrorClass::Recoverable);
        assert!(error.is_recoverable());
        assert!(!error.is_fatal());
        assert!(!error.is_user_visible());
    }
}
