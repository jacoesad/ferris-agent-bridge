use super::{OutboundDeliveryRecord, OutboundDeliveryStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundRetryPolicy {
    max_attempts: u32,
    initial_retry_delay_secs: u64,
    max_retry_delay_secs: u64,
}

impl OutboundRetryPolicy {
    pub fn new(
        max_attempts: u32,
        initial_retry_delay_secs: u64,
        max_retry_delay_secs: u64,
    ) -> Result<Self, String> {
        if max_attempts == 0 {
            return Err("outbound retry policy max_attempts must be greater than zero".to_owned());
        }

        if initial_retry_delay_secs == 0 {
            return Err(
                "outbound retry policy initial_retry_delay_secs must be greater than zero"
                    .to_owned(),
            );
        }

        if max_retry_delay_secs < initial_retry_delay_secs {
            return Err(
                "outbound retry policy max_retry_delay_secs must not be less than initial_retry_delay_secs"
                    .to_owned(),
            );
        }

        Ok(Self {
            max_attempts,
            initial_retry_delay_secs,
            max_retry_delay_secs,
        })
    }

    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    pub fn initial_retry_delay_secs(&self) -> u64 {
        self.initial_retry_delay_secs
    }

    pub fn max_retry_delay_secs(&self) -> u64 {
        self.max_retry_delay_secs
    }

    pub fn next_attempt_at_unix(&self, delivery: &OutboundDeliveryRecord) -> Option<u64> {
        match delivery.status() {
            OutboundDeliveryStatus::Pending => Some(delivery.updated_at_unix()),
            OutboundDeliveryStatus::Failed if delivery.delivery_attempts() < self.max_attempts => {
                Some(
                    delivery
                        .updated_at_unix()
                        .saturating_add(self.retry_delay_secs(delivery.delivery_attempts())),
                )
            }
            OutboundDeliveryStatus::Delivering
            | OutboundDeliveryStatus::Delivered
            | OutboundDeliveryStatus::Uncertain
            | OutboundDeliveryStatus::Failed => None,
        }
    }

    pub fn is_due(&self, delivery: &OutboundDeliveryRecord, now_unix: u64) -> bool {
        self.next_attempt_at_unix(delivery)
            .is_some_and(|next_attempt_at_unix| next_attempt_at_unix <= now_unix)
    }

    fn retry_delay_secs(&self, completed_attempts: u32) -> u64 {
        let exponent = completed_attempts.saturating_sub(1);
        let multiplier = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);

        self.initial_retry_delay_secs
            .saturating_mul(multiplier)
            .min(self.max_retry_delay_secs)
    }
}

impl Default for OutboundRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_retry_delay_secs: 1,
            max_retry_delay_secs: 60,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::OutboundRetryPolicy;
    use crate::runtime::{
        message::{Message, MessageAuthor, MessageContent, MessageId},
        outbox::{OutboundDeliveryId, OutboundDeliveryRecord},
        session::{SessionId, SessionScope},
    };

    #[test]
    fn retry_policy_rejects_invalid_bounds() {
        assert!(OutboundRetryPolicy::new(0, 1, 10).is_err());
        assert!(OutboundRetryPolicy::new(1, 0, 10).is_err());
        assert!(OutboundRetryPolicy::new(1, 10, 9).is_err());

        let policy = OutboundRetryPolicy::new(3, 2, 20).expect("valid retry policy");
        assert_eq!(policy.max_attempts(), 3);
        assert_eq!(policy.initial_retry_delay_secs(), 2);
        assert_eq!(policy.max_retry_delay_secs(), 20);
    }

    #[test]
    fn retry_policy_applies_bounded_exponential_backoff() {
        let policy = OutboundRetryPolicy::new(4, 10, 40).expect("valid retry policy");
        let mut delivery = delivery_fixture(10);

        assert_eq!(policy.next_attempt_at_unix(&delivery), Some(10));
        assert!(!policy.is_due(&delivery, 9));
        assert!(policy.is_due(&delivery, 10));

        delivery
            .begin_delivery(11)
            .expect("first attempt should start");
        assert_eq!(policy.next_attempt_at_unix(&delivery), None);
        delivery
            .mark_failed(12, "first failure")
            .expect("first attempt should fail");
        assert_eq!(policy.next_attempt_at_unix(&delivery), Some(22));

        delivery
            .begin_delivery(22)
            .expect("second attempt should start");
        delivery
            .mark_failed(23, "second failure")
            .expect("second attempt should fail");
        assert_eq!(policy.next_attempt_at_unix(&delivery), Some(43));

        delivery
            .begin_delivery(43)
            .expect("third attempt should start");
        delivery
            .mark_failed(44, "third failure")
            .expect("third attempt should fail");
        assert_eq!(policy.next_attempt_at_unix(&delivery), Some(84));

        delivery
            .begin_delivery(84)
            .expect("fourth attempt should start");
        delivery
            .mark_failed(85, "fourth failure")
            .expect("fourth attempt should fail");
        assert_eq!(policy.next_attempt_at_unix(&delivery), None);
        assert!(!policy.is_due(&delivery, u64::MAX));
    }

    #[test]
    fn retry_policy_saturates_next_attempt_time() {
        let policy = OutboundRetryPolicy::new(2, 10, 40).expect("valid retry policy");
        let mut delivery = delivery_fixture(u64::MAX - 6);
        delivery
            .begin_delivery(u64::MAX - 5)
            .expect("first attempt should start");
        delivery
            .mark_failed(u64::MAX - 4, "temporary failure")
            .expect("first attempt should fail");

        assert_eq!(policy.next_attempt_at_unix(&delivery), Some(u64::MAX));
        assert!(!policy.is_due(&delivery, u64::MAX - 1));
        assert!(policy.is_due(&delivery, u64::MAX));
    }

    #[test]
    fn retry_policy_accepts_only_explicitly_failed_uncertain_deliveries() {
        let policy = OutboundRetryPolicy::new(2, 10, 40).expect("valid retry policy");
        let mut delivery = delivery_fixture(10);
        delivery
            .begin_delivery(11)
            .expect("first attempt should start");
        delivery
            .mark_uncertain(12, "provider acceptance is unknown")
            .expect("delivery should become uncertain");
        assert_eq!(policy.next_attempt_at_unix(&delivery), None);

        delivery
            .resolve_uncertain_as_failed(13, "provider confirmed non-acceptance")
            .expect("confirmed non-acceptance should permit retry policy");

        assert_eq!(delivery.delivery_attempts(), 1);
        assert_eq!(policy.next_attempt_at_unix(&delivery), Some(23));
        assert!(!policy.is_due(&delivery, 22));
        assert!(policy.is_due(&delivery, 23));

        delivery
            .begin_delivery(23)
            .expect("second attempt should start");
        delivery
            .mark_uncertain(24, "provider acceptance is unknown")
            .expect("second attempt should become uncertain");
        delivery
            .resolve_uncertain_as_failed(24, "provider confirmed non-acceptance")
            .expect("confirmed non-acceptance should resolve the second attempt");
        assert_eq!(delivery.delivery_attempts(), 2);
        assert_eq!(policy.next_attempt_at_unix(&delivery), None);
        assert!(!policy.is_due(&delivery, u64::MAX));

        let mut delivered = delivery_fixture(30);
        delivered
            .begin_delivery(31)
            .expect("delivery attempt should start");
        delivered
            .mark_uncertain(32, "provider acceptance is unknown")
            .expect("delivery should become uncertain");
        delivered
            .resolve_uncertain_as_delivered(32)
            .expect("confirmed acceptance should resolve the delivery");
        assert_eq!(policy.next_attempt_at_unix(&delivered), None);
        assert!(!policy.is_due(&delivered, u64::MAX));
    }

    fn delivery_fixture(created_at_unix: u64) -> OutboundDeliveryRecord {
        let scope = SessionScope::new("lark", "chat:oc_123").expect("valid scope");
        let session_id = SessionId::for_scope(&scope);
        let message = Message::new(
            MessageId::new("msg_1").expect("valid message id"),
            Some(session_id.clone()),
            MessageAuthor::Agent,
            MessageContent::text("reply").expect("valid text"),
            created_at_unix,
        );

        OutboundDeliveryRecord::new(
            OutboundDeliveryId::new("out_1").expect("valid delivery id"),
            session_id,
            message,
            created_at_unix,
        )
        .expect("valid outbound delivery")
    }
}
