use super::OutboundDeliveryId;

pub(crate) const STARTUP_RECOVERY_DIAGNOSTIC: &str =
    "delivery outcome is unknown after startup recovery";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundDeliveryStartupRecoveryReport {
    reconciliation_required_delivery_ids: Vec<OutboundDeliveryId>,
}

impl OutboundDeliveryStartupRecoveryReport {
    pub(crate) fn new(reconciliation_required_delivery_ids: Vec<OutboundDeliveryId>) -> Self {
        Self {
            reconciliation_required_delivery_ids,
        }
    }

    pub fn reconciliation_required_delivery_ids(&self) -> &[OutboundDeliveryId] {
        &self.reconciliation_required_delivery_ids
    }

    pub fn is_empty(&self) -> bool {
        self.reconciliation_required_delivery_ids.is_empty()
    }
}
