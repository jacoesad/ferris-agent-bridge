use std::fmt;

use serde::{Deserialize, Deserializer};

#[derive(Default)]
pub(super) enum WireField<T> {
    #[default]
    Missing,
    Null,
    Value(T),
}

impl<T> WireField<T> {
    pub(super) fn is_present(&self) -> bool {
        !matches!(self, Self::Missing)
    }

    pub(super) fn into_required<M>(self, message: M) -> Result<T, String>
    where
        M: fmt::Display,
    {
        match self {
            Self::Value(value) => Ok(value),
            Self::Missing | Self::Null => Err(message.to_string()),
        }
    }
}

pub(super) fn deserialize_wire_field<'de, D, T>(deserializer: D) -> Result<WireField<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(match Option::<T>::deserialize(deserializer)? {
        Some(value) => WireField::Value(value),
        None => WireField::Null,
    })
}
