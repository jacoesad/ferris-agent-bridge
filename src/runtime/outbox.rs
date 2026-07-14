use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, de};

mod recovery;
mod retry;
mod worker;

pub use recovery::OutboundDeliveryStartupRecoveryReport;
pub(crate) use recovery::STARTUP_RECOVERY_DIAGNOSTIC;
pub use retry::OutboundRetryPolicy;
pub use worker::{OutboxWorker, OutboxWorkerOutcome};

use super::{
    message::{Message, MessageAuthor},
    session::{SessionId, SessionScope},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct OutboundDeliveryId(String);

impl OutboundDeliveryId {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();

        if !is_valid_id(&value) {
            return Err(format!("invalid outbound delivery id `{value}`"));
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OutboundDeliveryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for OutboundDeliveryId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboundDeliveryStatus {
    Pending,
    Delivering,
    Delivered,
    Failed,
    Uncertain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboundDeliveryEnqueueStatus {
    Queued,
    Duplicate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundDeliveryAttempt {
    delivery_id: OutboundDeliveryId,
    session_scope: SessionScope,
    message: Message,
    attempt_number: u32,
}

impl OutboundDeliveryAttempt {
    pub(super) fn from_record(
        delivery: &OutboundDeliveryRecord,
        session_scope: SessionScope,
    ) -> Result<Self, String> {
        if delivery.status() != OutboundDeliveryStatus::Delivering {
            return Err(format!(
                "outbound delivery {} cannot create an adapter attempt from {:?}",
                delivery.id(),
                delivery.status()
            ));
        }

        if SessionId::for_scope(&session_scope) != *delivery.session_id() {
            return Err(format!(
                "outbound delivery {} session scope does not match its session id",
                delivery.id()
            ));
        }

        Ok(Self {
            delivery_id: delivery.id().clone(),
            session_scope,
            message: delivery.message().clone(),
            attempt_number: delivery.delivery_attempts(),
        })
    }

    pub fn delivery_id(&self) -> &OutboundDeliveryId {
        &self.delivery_id
    }

    pub fn idempotency_key(&self) -> &str {
        self.delivery_id.as_str()
    }

    pub fn session_scope(&self) -> &SessionScope {
        &self.session_scope
    }

    pub fn message(&self) -> &Message {
        &self.message
    }

    pub fn attempt_number(&self) -> u32 {
        self.attempt_number
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OutboundDeliveryRecord {
    id: OutboundDeliveryId,
    session_id: SessionId,
    message: Message,
    status: OutboundDeliveryStatus,
    created_at_unix: u64,
    updated_at_unix: u64,
    delivery_attempts: u32,
    delivered_at_unix: Option<u64>,
    last_error: Option<String>,
}

impl OutboundDeliveryRecord {
    pub fn new(
        id: OutboundDeliveryId,
        session_id: SessionId,
        message: Message,
        created_at_unix: u64,
    ) -> Result<Self, String> {
        let record = Self {
            id,
            session_id,
            message,
            status: OutboundDeliveryStatus::Pending,
            created_at_unix,
            updated_at_unix: created_at_unix,
            delivery_attempts: 0,
            delivered_at_unix: None,
            last_error: None,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn id(&self) -> &OutboundDeliveryId {
        &self.id
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn message(&self) -> &Message {
        &self.message
    }

    pub fn status(&self) -> OutboundDeliveryStatus {
        self.status
    }

    pub fn created_at_unix(&self) -> u64 {
        self.created_at_unix
    }

    pub fn updated_at_unix(&self) -> u64 {
        self.updated_at_unix
    }

    pub fn delivery_attempts(&self) -> u32 {
        self.delivery_attempts
    }

    pub fn delivered_at_unix(&self) -> Option<u64> {
        self.delivered_at_unix
    }

    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    pub fn begin_delivery(&mut self, started_at_unix: u64) -> Result<(), String> {
        if !matches!(
            self.status,
            OutboundDeliveryStatus::Pending | OutboundDeliveryStatus::Failed
        ) {
            return Err(format!(
                "outbound delivery {} cannot start from {:?}",
                self.id, self.status
            ));
        }

        if started_at_unix < self.created_at_unix {
            return Err(format!(
                "outbound delivery {} cannot start before created_at_unix",
                self.id
            ));
        }

        if started_at_unix < self.updated_at_unix {
            return Err(format!(
                "outbound delivery {} cannot start before updated_at_unix",
                self.id
            ));
        }

        let next_attempts = self
            .delivery_attempts
            .checked_add(1)
            .ok_or_else(|| format!("outbound delivery {} attempts overflowed", self.id))?;

        self.status = OutboundDeliveryStatus::Delivering;
        self.delivery_attempts = next_attempts;
        self.delivered_at_unix = None;
        self.last_error = None;
        self.touch_at(started_at_unix);
        Ok(())
    }

    pub fn mark_delivered(&mut self, delivered_at_unix: u64) -> Result<(), String> {
        if self.status != OutboundDeliveryStatus::Delivering {
            return Err(format!(
                "outbound delivery {} cannot complete from {:?}",
                self.id, self.status
            ));
        }

        if delivered_at_unix < self.created_at_unix {
            return Err(format!(
                "outbound delivery {} cannot complete before created_at_unix",
                self.id
            ));
        }

        if delivered_at_unix < self.updated_at_unix {
            return Err(format!(
                "outbound delivery {} cannot complete before updated_at_unix",
                self.id
            ));
        }

        self.status = OutboundDeliveryStatus::Delivered;
        self.delivered_at_unix = Some(delivered_at_unix);
        self.last_error = None;
        self.touch_at(delivered_at_unix);
        Ok(())
    }

    pub fn mark_failed(
        &mut self,
        failed_at_unix: u64,
        error: impl Into<String>,
    ) -> Result<(), String> {
        self.mark_unsuccessful(
            OutboundDeliveryStatus::Failed,
            failed_at_unix,
            error,
            "fail",
            "failure",
        )
    }

    pub fn mark_uncertain(
        &mut self,
        uncertain_at_unix: u64,
        error: impl Into<String>,
    ) -> Result<(), String> {
        self.mark_unsuccessful(
            OutboundDeliveryStatus::Uncertain,
            uncertain_at_unix,
            error,
            "become uncertain",
            "uncertain outcome",
        )
    }

    pub fn resolve_uncertain_as_delivered(&mut self, resolved_at_unix: u64) -> Result<(), String> {
        if self.status != OutboundDeliveryStatus::Uncertain {
            return Err(format!(
                "outbound delivery {} cannot resolve as delivered from {:?}",
                self.id, self.status
            ));
        }

        self.validate_resolution_time(resolved_at_unix, "resolve as delivered")?;
        self.status = OutboundDeliveryStatus::Delivered;
        self.delivered_at_unix = Some(resolved_at_unix);
        self.last_error = None;
        self.touch_at(resolved_at_unix);
        Ok(())
    }

    pub fn resolve_uncertain_as_failed(
        &mut self,
        resolved_at_unix: u64,
        error: impl Into<String>,
    ) -> Result<(), String> {
        if self.status != OutboundDeliveryStatus::Uncertain {
            return Err(format!(
                "outbound delivery {} cannot resolve as failed from {:?}",
                self.id, self.status
            ));
        }

        self.validate_resolution_time(resolved_at_unix, "resolve as failed")?;
        let error = error.into();
        if error.trim().is_empty() {
            return Err(format!(
                "outbound delivery {} resolution failure is empty",
                self.id
            ));
        }

        self.status = OutboundDeliveryStatus::Failed;
        self.delivered_at_unix = None;
        self.last_error = Some(error);
        self.touch_at(resolved_at_unix);
        Ok(())
    }

    fn mark_unsuccessful(
        &mut self,
        status: OutboundDeliveryStatus,
        updated_at_unix: u64,
        error: impl Into<String>,
        action: &str,
        error_label: &str,
    ) -> Result<(), String> {
        debug_assert!(matches!(
            status,
            OutboundDeliveryStatus::Failed | OutboundDeliveryStatus::Uncertain
        ));

        if self.status != OutboundDeliveryStatus::Delivering {
            return Err(format!(
                "outbound delivery {} cannot {action} from {:?}",
                self.id, self.status,
            ));
        }

        if updated_at_unix < self.created_at_unix {
            return Err(format!(
                "outbound delivery {} cannot {action} before created_at_unix",
                self.id,
            ));
        }

        if updated_at_unix < self.updated_at_unix {
            return Err(format!(
                "outbound delivery {} cannot {action} before updated_at_unix",
                self.id,
            ));
        }

        let error = error.into();
        if error.trim().is_empty() {
            return Err(format!(
                "outbound delivery {} {error_label} is empty",
                self.id
            ));
        }

        self.status = status;
        self.delivered_at_unix = None;
        self.last_error = Some(error);
        self.touch_at(updated_at_unix);
        Ok(())
    }

    fn validate_resolution_time(&self, resolved_at_unix: u64, action: &str) -> Result<(), String> {
        if resolved_at_unix < self.created_at_unix {
            return Err(format!(
                "outbound delivery {} cannot {action} before created_at_unix",
                self.id
            ));
        }

        if resolved_at_unix < self.updated_at_unix {
            return Err(format!(
                "outbound delivery {} cannot {action} before updated_at_unix",
                self.id
            ));
        }

        Ok(())
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.message.session_id.as_ref() != Some(&self.session_id) {
            return Err(format!(
                "outbound delivery {} message session does not match delivery session",
                self.id
            ));
        }

        if self.message.author == MessageAuthor::User {
            return Err(format!(
                "outbound delivery {} cannot contain a user-authored message",
                self.id
            ));
        }

        if self.created_at_unix < self.message.created_at_unix {
            return Err(format!(
                "outbound delivery {} has created_at_unix before message created_at_unix",
                self.id
            ));
        }

        if self.updated_at_unix < self.created_at_unix {
            return Err(format!(
                "outbound delivery {} has updated_at_unix before created_at_unix",
                self.id
            ));
        }

        if let Some(delivered_at_unix) = self.delivered_at_unix {
            if delivered_at_unix < self.created_at_unix {
                return Err(format!(
                    "outbound delivery {} has delivered_at_unix before created_at_unix",
                    self.id
                ));
            }

            if self.updated_at_unix < delivered_at_unix {
                return Err(format!(
                    "outbound delivery {} has updated_at_unix before delivered_at_unix",
                    self.id
                ));
            }
        }

        if let Some(error) = &self.last_error {
            if error.trim().is_empty() {
                return Err(format!("outbound delivery {} last_error is empty", self.id));
            }
        }

        match self.status {
            OutboundDeliveryStatus::Pending => {
                if self.delivery_attempts != 0
                    || self.delivered_at_unix.is_some()
                    || self.last_error.is_some()
                {
                    return Err(format!(
                        "pending outbound delivery {} must not have attempts, delivery time, or error",
                        self.id
                    ));
                }
            }
            OutboundDeliveryStatus::Delivering => {
                if self.delivery_attempts == 0
                    || self.delivered_at_unix.is_some()
                    || self.last_error.is_some()
                {
                    return Err(format!(
                        "delivering outbound delivery {} must have attempts without delivery time or error",
                        self.id
                    ));
                }
            }
            OutboundDeliveryStatus::Delivered => {
                if self.delivery_attempts == 0
                    || self.delivered_at_unix.is_none()
                    || self.last_error.is_some()
                {
                    return Err(format!(
                        "delivered outbound delivery {} must have attempts and delivered_at_unix only",
                        self.id
                    ));
                }

                if self.delivered_at_unix != Some(self.updated_at_unix) {
                    return Err(format!(
                        "delivered outbound delivery {} delivered_at_unix must match updated_at_unix",
                        self.id
                    ));
                }
            }
            OutboundDeliveryStatus::Failed | OutboundDeliveryStatus::Uncertain => {
                if self.delivery_attempts == 0
                    || self.delivered_at_unix.is_some()
                    || self.last_error.is_none()
                {
                    return Err(format!(
                        "{:?} outbound delivery {} must have attempts and last_error only",
                        self.status, self.id
                    ));
                }
            }
        }

        Ok(())
    }

    fn touch_at(&mut self, updated_at_unix: u64) {
        self.updated_at_unix = self.updated_at_unix.max(updated_at_unix);
    }
}

impl<'de> Deserialize<'de> for OutboundDeliveryRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct OutboundDeliveryRecordWire {
            id: OutboundDeliveryId,
            session_id: SessionId,
            message: Message,
            status: OutboundDeliveryStatus,
            created_at_unix: u64,
            updated_at_unix: u64,
            delivery_attempts: u32,
            delivered_at_unix: Option<u64>,
            last_error: Option<String>,
        }

        let wire = OutboundDeliveryRecordWire::deserialize(deserializer)?;
        let record = Self {
            id: wire.id,
            session_id: wire.session_id,
            message: wire.message,
            status: wire.status,
            created_at_unix: wire.created_at_unix,
            updated_at_unix: wire.updated_at_unix,
            delivery_attempts: wire.delivery_attempts,
            delivered_at_unix: wire.delivered_at_unix,
            last_error: wire.last_error,
        };

        record.validate().map_err(de::Error::custom)?;
        Ok(record)
    }
}

fn is_valid_id(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
}

#[cfg(test)]
mod tests {
    use super::{
        OutboundDeliveryAttempt, OutboundDeliveryId, OutboundDeliveryRecord, OutboundDeliveryStatus,
    };
    use crate::runtime::{
        message::{Message, MessageAuthor, MessageContent, MessageId},
        session::{SessionId, SessionScope},
    };

    #[test]
    fn outbound_delivery_transitions_through_delivery_attempts() {
        let mut delivery = delivery_fixture("out_1", 10);

        assert_eq!(delivery.status(), OutboundDeliveryStatus::Pending);
        assert_eq!(delivery.delivery_attempts(), 0);

        delivery.begin_delivery(11).expect("delivery should start");
        assert_eq!(delivery.status(), OutboundDeliveryStatus::Delivering);
        assert_eq!(delivery.delivery_attempts(), 1);

        delivery
            .mark_failed(12, "transport failed")
            .expect("delivery should fail");
        assert_eq!(delivery.status(), OutboundDeliveryStatus::Failed);
        assert_eq!(delivery.last_error(), Some("transport failed"));

        delivery
            .begin_delivery(13)
            .expect("failed delivery can retry");
        assert_eq!(delivery.status(), OutboundDeliveryStatus::Delivering);
        assert_eq!(delivery.delivery_attempts(), 2);
        assert_eq!(delivery.last_error(), None);

        delivery
            .mark_delivered(14)
            .expect("delivery should complete");
        assert_eq!(delivery.status(), OutboundDeliveryStatus::Delivered);
        assert_eq!(delivery.delivered_at_unix(), Some(14));
    }

    #[test]
    fn outbound_delivery_preserves_uncertain_outcomes_without_retrying() {
        let mut delivery = delivery_fixture("out_1", 10);
        delivery.begin_delivery(11).expect("delivery should start");
        delivery
            .mark_uncertain(12, "provider acceptance is unknown")
            .expect("uncertain outcome should persist");

        assert_eq!(delivery.status(), OutboundDeliveryStatus::Uncertain);
        assert_eq!(
            delivery.last_error(),
            Some("provider acceptance is unknown")
        );
        assert_eq!(delivery.delivery_attempts(), 1);

        let err = delivery
            .begin_delivery(13)
            .expect_err("uncertain delivery must not retry automatically");
        assert!(err.contains("cannot start from Uncertain"));

        let encoded = serde_json::to_string(&delivery).expect("delivery should serialize");
        let decoded: OutboundDeliveryRecord =
            serde_json::from_str(&encoded).expect("uncertain delivery should decode");
        assert_eq!(decoded, delivery);
    }

    #[test]
    fn outbound_delivery_resolves_uncertain_outcomes_explicitly() {
        let mut delivered = delivery_fixture("out_delivered", 10);
        delivered.begin_delivery(11).expect("delivery should start");
        delivered
            .mark_uncertain(12, "provider acceptance is unknown")
            .expect("delivery should become uncertain");
        delivered
            .resolve_uncertain_as_delivered(12)
            .expect("confirmed acceptance should resolve the delivery");

        assert_eq!(delivered.status(), OutboundDeliveryStatus::Delivered);
        assert_eq!(delivered.delivery_attempts(), 1);
        assert_eq!(delivered.delivered_at_unix(), Some(12));
        assert_eq!(delivered.last_error(), None);

        let mut failed = delivery_fixture("out_failed", 20);
        failed.begin_delivery(21).expect("delivery should start");
        failed
            .mark_uncertain(22, "provider acceptance is unknown")
            .expect("delivery should become uncertain");
        failed
            .resolve_uncertain_as_failed(22, "provider confirmed non-acceptance")
            .expect("confirmed non-acceptance should resolve the delivery");

        assert_eq!(failed.status(), OutboundDeliveryStatus::Failed);
        assert_eq!(failed.delivery_attempts(), 1);
        assert_eq!(failed.delivered_at_unix(), None);
        assert_eq!(
            failed.last_error(),
            Some("provider confirmed non-acceptance")
        );
    }

    #[test]
    fn outbound_delivery_rejects_invalid_uncertain_resolutions_without_mutating() {
        let mut pending = delivery_fixture("out_pending", 10);
        let pending_before = pending.clone();
        let err = pending
            .resolve_uncertain_as_delivered(11)
            .expect_err("pending delivery cannot be reconciled");
        assert!(err.contains("from Pending"));
        assert_eq!(pending, pending_before);

        let err = pending
            .resolve_uncertain_as_failed(11, "provider confirmed non-acceptance")
            .expect_err("pending delivery cannot be reconciled as failed");
        assert!(err.contains("from Pending"));
        assert_eq!(pending, pending_before);

        let mut uncertain = delivery_fixture("out_uncertain", 10);
        uncertain.begin_delivery(11).expect("delivery should start");
        uncertain
            .mark_uncertain(12, "provider acceptance is unknown")
            .expect("delivery should become uncertain");

        let before_backwards = uncertain.clone();
        let err = uncertain
            .resolve_uncertain_as_delivered(11)
            .expect_err("resolution timestamp cannot move backwards");
        assert!(err.contains("before updated_at_unix"));
        assert_eq!(uncertain, before_backwards);

        let before_failed_backwards = uncertain.clone();
        let err = uncertain
            .resolve_uncertain_as_failed(11, "provider confirmed non-acceptance")
            .expect_err("failed resolution timestamp cannot move backwards");
        assert!(err.contains("before updated_at_unix"));
        assert_eq!(uncertain, before_failed_backwards);

        let before_direct_failure = uncertain.clone();
        let err = uncertain
            .mark_failed(13, "provider confirmed non-acceptance")
            .expect_err("ordinary worker failure cannot resolve uncertainty");
        assert!(err.contains("cannot fail from Uncertain"));
        assert_eq!(uncertain, before_direct_failure);

        let before_empty_reason = uncertain.clone();
        let err = uncertain
            .resolve_uncertain_as_failed(13, "  ")
            .expect_err("failed resolution requires evidence");
        assert!(err.contains("resolution failure is empty"));
        assert_eq!(uncertain, before_empty_reason);
    }

    #[test]
    fn outbound_attempt_requires_a_claimed_record_and_matching_scope() {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let mut delivery = delivery_fixture("out_1", 10);

        let err = OutboundDeliveryAttempt::from_record(&delivery, scope.clone())
            .expect_err("pending deliveries cannot become adapter attempts");
        assert!(err.contains("from Pending"));

        delivery.begin_delivery(11).expect("delivery should claim");
        let attempt = OutboundDeliveryAttempt::from_record(&delivery, scope)
            .expect("claimed delivery should become an attempt");
        assert_eq!(attempt.delivery_id(), delivery.id());
        assert_eq!(attempt.idempotency_key(), delivery.id().as_str());
        assert_eq!(attempt.message(), delivery.message());
        assert_eq!(attempt.attempt_number(), 1);

        let other_scope = SessionScope::new("lark", "chat:oc_other").expect("valid scope");
        let err = OutboundDeliveryAttempt::from_record(&delivery, other_scope)
            .expect_err("scope must match the persisted session");
        assert!(err.contains("session scope"));
    }

    #[test]
    fn outbound_delivery_rejects_invalid_state_transitions() {
        let mut delivery = delivery_fixture("out_1", 10);

        let err = delivery
            .mark_delivered(11)
            .expect_err("pending delivery should not complete");
        assert!(err.contains("cannot complete from Pending"));

        let err = delivery
            .begin_delivery(9)
            .expect_err("delivery cannot start before creation");
        assert!(err.contains("before created_at_unix"));

        delivery.begin_delivery(11).expect("delivery should start");
        let before_empty_uncertain = delivery.clone();
        let err = delivery
            .mark_uncertain(12, "  ")
            .expect_err("uncertain outcome requires a non-empty error");
        assert!(err.contains("uncertain outcome is empty"));
        assert_eq!(delivery, before_empty_uncertain);

        delivery
            .mark_delivered(12)
            .expect("delivery should complete");

        let err = delivery
            .begin_delivery(13)
            .expect_err("delivered messages cannot retry");
        assert!(err.contains("cannot start from Delivered"));
    }

    #[test]
    fn outbound_delivery_attempt_overflow_leaves_record_unchanged() {
        let mut delivery = delivery_fixture("out_1", 10);
        delivery.begin_delivery(11).expect("delivery should start");
        delivery
            .mark_failed(12, "transport failed")
            .expect("delivery should fail");
        delivery.delivery_attempts = u32::MAX;
        let before = delivery.clone();

        let err = delivery
            .begin_delivery(13)
            .expect_err("attempt overflow should fail");

        assert!(err.contains("attempts overflowed"));
        assert_eq!(delivery, before);
    }

    #[test]
    fn outbound_delivery_rejects_backwards_transition_timestamps() {
        let mut delivered = delivery_fixture("out_1", 10);
        delivered
            .begin_delivery(100)
            .expect("delivery should start");
        let before_delivered = delivered.clone();

        let err = delivered
            .mark_delivered(99)
            .expect_err("delivery should not complete before the attempt started");

        assert!(err.contains("before updated_at_unix"));
        assert_eq!(delivered, before_delivered);

        let mut failed = delivery_fixture("out_2", 10);
        failed.begin_delivery(100).expect("delivery should start");
        let before_failed = failed.clone();

        let err = failed
            .mark_failed(99, "transport failed")
            .expect_err("delivery should not fail before the attempt started");

        assert!(err.contains("before updated_at_unix"));
        assert_eq!(failed, before_failed);

        let mut uncertain = delivery_fixture("out_uncertain", 10);
        uncertain
            .begin_delivery(100)
            .expect("delivery should start");
        let before_uncertain = uncertain.clone();

        let err = uncertain
            .mark_uncertain(99, "provider acceptance is unknown")
            .expect_err("uncertain outcome cannot predate the attempt");

        assert!(err.contains("before updated_at_unix"));
        assert_eq!(uncertain, before_uncertain);

        let mut retried = delivery_fixture("out_3", 10);
        retried.begin_delivery(100).expect("delivery should start");
        retried
            .mark_failed(101, "transport failed")
            .expect("delivery should fail");
        let before_retry = retried.clone();

        let err = retried
            .begin_delivery(100)
            .expect_err("retry should not start before the previous failure update");

        assert!(err.contains("before updated_at_unix"));
        assert_eq!(retried, before_retry);
    }

    #[test]
    fn outbound_delivery_round_trips_as_json() {
        let mut delivery = delivery_fixture("out_1", 10);
        delivery.begin_delivery(11).expect("delivery should start");
        delivery
            .mark_failed(12, "temporary failure")
            .expect("delivery should fail");

        let encoded = serde_json::to_string(&delivery).expect("delivery should serialize");
        let decoded: OutboundDeliveryRecord =
            serde_json::from_str(&encoded).expect("delivery should decode");

        assert_eq!(decoded, delivery);
    }

    #[test]
    fn outbound_delivery_rejects_invalid_json() {
        let delivery = delivery_fixture("out_1", 10);
        let mut value = serde_json::to_value(&delivery).expect("delivery should encode");
        value["status"] = serde_json::Value::String("delivered".to_owned());

        let err = serde_json::from_value::<OutboundDeliveryRecord>(value)
            .expect_err("delivered status must have delivered_at_unix");

        assert!(err.to_string().contains("delivered_at_unix"));

        let mut delivered = delivery_fixture("out_2", 10);
        delivered
            .begin_delivery(100)
            .expect("delivery should start");
        delivered
            .mark_delivered(100)
            .expect("delivery should complete");
        let mut value = serde_json::to_value(&delivered).expect("delivery should encode");
        value["delivered_at_unix"] = serde_json::Value::Number(99.into());

        let err = serde_json::from_value::<OutboundDeliveryRecord>(value)
            .expect_err("delivered_at_unix must not predate updated_at_unix");

        assert!(err.to_string().contains("must match updated_at_unix"));

        let mut value =
            serde_json::to_value(delivery_fixture("out_3", 10)).expect("delivery should encode");
        value["status"] = serde_json::Value::String("uncertain".to_owned());

        let err = serde_json::from_value::<OutboundDeliveryRecord>(value)
            .expect_err("uncertain status must have a last error");
        assert!(err.to_string().contains("last_error"));
    }

    #[test]
    fn outbound_delivery_requires_matching_non_user_message() {
        let session_id = session_id();
        let other_session_id =
            SessionId::for_scope(&SessionScope::new("lark", "chat:oc_other").expect("valid scope"));
        let user_message = Message::user_text("msg_1", Some(session_id.clone()), "hello", 10)
            .expect("valid user message");
        let agent_message = Message::new(
            MessageId::new("msg_2").expect("valid message id"),
            Some(other_session_id),
            MessageAuthor::Agent,
            MessageContent::text("hello").expect("valid text"),
            10,
        );

        let err = OutboundDeliveryRecord::new(
            OutboundDeliveryId::new("out_user").expect("valid id"),
            session_id.clone(),
            user_message,
            10,
        )
        .expect_err("user-authored outbound messages should be rejected");
        assert!(err.contains("user-authored"));

        let err = OutboundDeliveryRecord::new(
            OutboundDeliveryId::new("out_session").expect("valid id"),
            session_id,
            agent_message,
            10,
        )
        .expect_err("message session should match outbox session");
        assert!(err.contains("message session"));
    }

    #[test]
    fn rejects_invalid_outbound_delivery_ids() {
        assert!(OutboundDeliveryId::new("").is_err());
        assert!(OutboundDeliveryId::new("has space").is_err());
        assert_eq!(
            OutboundDeliveryId::new("outbound:msg_1")
                .expect("valid id")
                .as_str(),
            "outbound:msg_1"
        );
    }

    fn delivery_fixture(id: &str, created_at_unix: u64) -> OutboundDeliveryRecord {
        let session_id = session_id();
        let message = Message::new(
            MessageId::new(format!("msg_{id}")).expect("valid message id"),
            Some(session_id.clone()),
            MessageAuthor::Agent,
            MessageContent::text("hello").expect("valid text"),
            created_at_unix,
        );

        OutboundDeliveryRecord::new(
            OutboundDeliveryId::new(id).expect("valid outbound id"),
            session_id,
            message,
            created_at_unix,
        )
        .expect("valid delivery")
    }

    fn session_id() -> SessionId {
        SessionId::for_scope(&SessionScope::new("lark", "chat:oc_123").expect("valid scope"))
    }
}
