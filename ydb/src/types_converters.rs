use crate::errors::YdbError;
use crate::types::{Bytes, Value, ValueOptional};
use crate::{ValueList, ValueStruct};
use itertools::Itertools;
use std::any::type_name;
use std::collections::HashMap;
use std::time::SystemTime;

macro_rules! simple_convert {
    ($native_type:ty, $ydb_value_kind_first:path $(,$ydb_value_kind:path)* $(,)?) => {
        impl From<$native_type> for Value {
            fn from(value: $native_type)->Self {
                $ydb_value_kind_first(value)
            }
        }

        impl TryFrom<Value> for $native_type {
            type Error = YdbError;

            fn try_from(value: Value) -> Result<Self, Self::Error> {
                match value {
                    $ydb_value_kind_first(val) => Ok(val.into()),
                    $($ydb_value_kind(val) => Ok(val.into()),)*
                    value => Err(YdbError::Convert(format!(
                        "failed to convert from {} to {}",
                        value.kind_static(),
                        type_name::<Self>(),
                    ))),
                }
            }
        }

        impl TryFrom<Value> for Option<$native_type> {
            type Error = YdbError;

            fn try_from(value: Value) -> Result<Self, Self::Error> {
                match value {
                    Value::Optional(opt_val) => {
                        <$native_type as TryFrom<Value>>::try_from(opt_val.t)?;

                        match opt_val.value {
                            Some(val) => {
                                let res_val: $native_type = val.try_into()?;
                                Ok(Some(res_val))
                            }
                            None => Ok(None),
                        }
                    }
                    value => Ok(Some(value.try_into()?)),
                }
            }
        }

    };
}

simple_convert!(bool, Value::Bool);
simple_convert!(i8, Value::Int8);
simple_convert!(u8, Value::Uint8);
simple_convert!(i16, Value::Int16, Value::Int8, Value::Uint8);
simple_convert!(u16, Value::Uint16, Value::Uint8);
simple_convert!(
    i32,
    Value::Int32,
    Value::Int16,
    Value::Uint16,
    Value::Int8,
    Value::Uint8,
);
simple_convert!(u32, Value::Uint32, Value::Uint16, Value::Uint8);
simple_convert!(
    i64,
    Value::Int64,
    Value::Int32,
    Value::Uint32,
    Value::Int16,
    Value::Uint16,
    Value::Int8,
    Value::Uint8,
);
simple_convert!(
    u64,
    Value::Uint64,
    Value::Uint32,
    Value::Uint16,
    Value::Uint8,
);
simple_convert!(String, Value::Text, Value::Json, Value::JsonDocument,);
simple_convert!(
    Bytes,
    Value::Bytes,
    Value::Text,
    Value::Json,
    Value::JsonDocument,
    Value::Yson
);
simple_convert!(f32, Value::Float);
simple_convert!(f64, Value::Double, Value::Float);
simple_convert!(SystemTime, Value::Timestamp, Value::Date, Value::DateTime);
simple_convert!(decimal_rs::Decimal, Value::Decimal);

// Impl additional Value From
impl From<&str> for Value {
    fn from(v: &str) -> Self {
        v.to_string().into()
    }
}

// ===== Chrono Type Conversions =====

/// Converts YDB timestamp or datetime value to Chrono's DateTime<Utc>
///
/// This implementation provides a more semantically appropriate conversion for timestamp
/// values compared to the standard SystemTime conversion.
///
/// # Examples
///
/// ```
/// use ydb::Value;
/// use std::time::SystemTime;
/// use chrono::{DateTime, Utc};
///
/// // Create a timestamp value
/// let now = SystemTime::now();
/// let timestamp_value = Value::Timestamp(now);
///
/// // Convert to chrono DateTime<Utc>
/// let datetime: DateTime<Utc> = timestamp_value.try_into().unwrap();
/// ```
///
/// # Errors
///
/// Returns an error if the value type is not Timestamp or DateTime, or if
/// the conversion from SystemTime to DateTime<Utc> fails.
impl TryFrom<Value> for chrono::DateTime<chrono::Utc> {
    type Error = YdbError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Timestamp(ts) => {
                // Convert SystemTime to chrono::DateTime<Utc>
                let st: SystemTime = ts;
                let duration = st.duration_since(SystemTime::UNIX_EPOCH).map_err(|e| {
                    YdbError::Convert(format!("Failed to convert timestamp: {}", e))
                })?;

                // Create DateTime from duration since epoch
                let seconds = duration.as_secs() as i64;
                let nanos = duration.subsec_nanos();

                // Use from_timestamp_opt which returns Option<DateTime<Utc>>
                Ok(
                    chrono::DateTime::<chrono::Utc>::from_timestamp_opt(seconds, nanos)
                        .unwrap_or_else(|| {
                            chrono::DateTime::<chrono::Utc>::from_timestamp_opt(0, 0).unwrap()
                        }),
                )
            }
            Value::DateTime(dt) => {
                // Also support DateTime conversion
                let st: SystemTime = dt;
                let duration = st
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map_err(|e| YdbError::Convert(format!("Failed to convert datetime: {}", e)))?;

                // Create DateTime from duration since epoch
                let seconds = duration.as_secs() as i64;
                let nanos = duration.subsec_nanos();

                Ok(
                    chrono::DateTime::<chrono::Utc>::from_timestamp_opt(seconds, nanos)
                        .unwrap_or_else(|| {
                            chrono::DateTime::<chrono::Utc>::from_timestamp_opt(0, 0).unwrap()
                        }),
                )
            }
            _ => Err(YdbError::Convert(format!(
                "Failed to convert from {} to chrono::DateTime<Utc>",
                value.kind_static()
            ))),
        }
    }
}

/// Converts YDB datetime or timestamp value to Chrono's NaiveDateTime
///
/// NaiveDateTime represents a datetime without timezone information, making it
/// appropriate for YDB's DateTime type.
///
/// # Examples
///
/// ```
/// use ydb::Value;
/// use std::time::SystemTime;
/// use chrono::NaiveDateTime;
///
/// // Create a datetime value
/// let now = SystemTime::now();
/// let datetime_value = Value::DateTime(now);
///
/// // Convert to chrono NaiveDateTime
/// let naive_dt: NaiveDateTime = datetime_value.try_into().unwrap();
/// ```
///
/// # Errors
///
/// Returns an error if the value type is not DateTime or Timestamp, or if
/// the conversion fails.
impl TryFrom<Value> for chrono::NaiveDateTime {
    type Error = YdbError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::DateTime(dt) => {
                // Convert SystemTime to chrono::NaiveDateTime
                let st: SystemTime = dt;
                let duration = st
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map_err(|e| YdbError::Convert(format!("Failed to convert datetime: {}", e)))?;

                // Create NaiveDateTime from seconds and nanoseconds
                let seconds = duration.as_secs() as i64;
                let nanos = duration.subsec_nanos();

                chrono::NaiveDateTime::from_timestamp_opt(seconds, nanos)
                    .ok_or_else(|| YdbError::Convert("Invalid timestamp for NaiveDateTime".into()))
            }
            Value::Timestamp(ts) => {
                // Also support Timestamp conversion
                let st: SystemTime = ts;
                let duration = st.duration_since(SystemTime::UNIX_EPOCH).map_err(|e| {
                    YdbError::Convert(format!("Failed to convert timestamp: {}", e))
                })?;

                let seconds = duration.as_secs() as i64;
                let nanos = duration.subsec_nanos();

                chrono::NaiveDateTime::from_timestamp_opt(seconds, nanos)
                    .ok_or_else(|| YdbError::Convert("Invalid timestamp for NaiveDateTime".into()))
            }
            _ => Err(YdbError::Convert(format!(
                "Failed to convert from {} to chrono::NaiveDateTime",
                value.kind_static()
            ))),
        }
    }
}

/// Converts YDB date value to Chrono's NaiveDate
///
/// NaiveDate represents a date without timezone information, matching
/// the semantics of YDB's Date type.
///
/// # Examples
///
/// ```
/// use ydb::Value;
/// use std::time::SystemTime;
/// use chrono::NaiveDate;
///
/// // Create a date value
/// let now = SystemTime::now();
/// let date_value = Value::Date(now);
///
/// // Convert to chrono NaiveDate
/// let date: NaiveDate = date_value.try_into().unwrap();
/// ```
///
/// # Errors
///
/// Returns an error if the value type is not Date, or if the conversion fails.
impl TryFrom<Value> for chrono::NaiveDate {
    type Error = YdbError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Date(d) => {
                // Convert SystemTime to chrono::NaiveDate
                let st: SystemTime = d;
                let duration = st
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map_err(|e| YdbError::Convert(format!("Failed to convert date: {}", e)))?;

                // Convert to NaiveDateTime first (at UTC) then extract the date part
                let seconds = duration.as_secs() as i64;
                let datetime = chrono::NaiveDateTime::from_timestamp_opt(
                    seconds, 0, // Ignore nanoseconds for date
                )
                .ok_or_else(|| YdbError::Convert("Invalid timestamp for NaiveDate".into()))?;

                Ok(datetime.date())
            }
            _ => Err(YdbError::Convert(format!(
                "Failed to convert from {} to chrono::NaiveDate",
                value.kind_static()
            ))),
        }
    }
}

// Conversion from chrono types to Value

/// Converts Chrono's DateTime<Utc> to a YDB Timestamp value
///
/// # Examples
///
/// ```
/// use ydb::Value;
/// use chrono::{DateTime, Utc, TimeZone};
///
/// // Create a chrono DateTime
/// let dt = Utc.with_ymd_and_hms_opt(2024, 4, 15, 12, 0, 0).unwrap();
///
/// // Convert to YDB Value
/// let timestamp_value: Value = dt.into();
///
/// // The result will be Value::Timestamp
/// ```
impl From<chrono::DateTime<chrono::Utc>> for Value {
    fn from(dt: chrono::DateTime<chrono::Utc>) -> Self {
        // Convert chrono::DateTime<Utc> to SystemTime and then to Value::Timestamp
        let timestamp = dt.timestamp();
        let nanos = dt.timestamp_subsec_nanos();

        // Handle negative timestamps
        let system_time = if timestamp >= 0 {
            SystemTime::UNIX_EPOCH
                .checked_add(std::time::Duration::new(timestamp as u64, nanos))
                .unwrap_or(SystemTime::UNIX_EPOCH)
        } else {
            SystemTime::UNIX_EPOCH
                .checked_sub(std::time::Duration::new((-timestamp) as u64, nanos))
                .unwrap_or(SystemTime::UNIX_EPOCH)
        };

        Value::Timestamp(system_time)
    }
}

/// Converts Chrono's NaiveDateTime to a YDB DateTime value
///
/// # Examples
///
/// ```
/// use ydb::Value;
/// use chrono::NaiveDateTime;
///
/// // Create a chrono NaiveDateTime
/// let naive_dt = NaiveDateTime::new(
///     chrono::NaiveDate::from_ymd_opt(2024, 4, 15).unwrap(),
///     chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
/// );
///
/// // Convert to YDB Value
/// let datetime_value: Value = naive_dt.into();
///
/// // The result will be Value::DateTime
/// ```
impl From<chrono::NaiveDateTime> for Value {
    fn from(dt: chrono::NaiveDateTime) -> Self {
        // Convert NaiveDateTime to SystemTime and then to Value::DateTime
        let timestamp = dt.timestamp();
        let nanos = dt.timestamp_subsec_nanos();

        // Handle negative timestamps
        let system_time = if timestamp >= 0 {
            SystemTime::UNIX_EPOCH
                .checked_add(std::time::Duration::new(timestamp as u64, nanos))
                .unwrap_or(SystemTime::UNIX_EPOCH)
        } else {
            SystemTime::UNIX_EPOCH
                .checked_sub(std::time::Duration::new((-timestamp) as u64, nanos))
                .unwrap_or(SystemTime::UNIX_EPOCH)
        };

        Value::DateTime(system_time)
    }
}

/// Converts Chrono's NaiveDate to a YDB Date value
///
/// # Examples
///
/// ```
/// use ydb::Value;
/// use chrono::NaiveDate;
///
/// // Create a chrono NaiveDate
/// let date = NaiveDate::from_ymd_opt(2024, 4, 15).unwrap();
///
/// // Convert to YDB Value
/// let date_value: Value = date.into();
///
/// // The result will be Value::Date
/// ```
impl From<chrono::NaiveDate> for Value {
    fn from(d: chrono::NaiveDate) -> Self {
        // Convert NaiveDate to midnight, then to SystemTime, then to Value::Date
        let naive_dt = d.and_hms_opt(0, 0, 0).unwrap();
        let timestamp = naive_dt.timestamp();

        // Handle negative timestamps (dates before 1970)
        let system_time = if timestamp >= 0 {
            SystemTime::UNIX_EPOCH
                .checked_add(std::time::Duration::new(timestamp as u64, 0))
                .unwrap_or(SystemTime::UNIX_EPOCH)
        } else {
            SystemTime::UNIX_EPOCH
                .checked_sub(std::time::Duration::new((-timestamp) as u64, 0))
                .unwrap_or(SystemTime::UNIX_EPOCH)
        };

        Value::Date(system_time)
    }
}

// Support for optional chrono types

/// Converts YDB optional timestamp/datetime to Option<DateTime<Utc>>
///
/// # Examples
///
/// ```
/// use ydb::Value;
/// use std::time::SystemTime;
/// use chrono::{DateTime, Utc};
///
/// // Create an optional timestamp value
/// let now = SystemTime::now();
/// let opt_value = Value::Optional(Box::new(ydb::ValueOptional {
///     t: Value::Timestamp(SystemTime::UNIX_EPOCH),
///     value: Some(Value::Timestamp(now)),
/// }));
///
/// // Convert to Option<DateTime<Utc>>
/// let opt_dt: Option<DateTime<Utc>> = opt_value.try_into().unwrap();
/// ```
impl TryFrom<Value> for Option<chrono::DateTime<chrono::Utc>> {
    type Error = YdbError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Optional(opt_val) => {
                // Check if the optional value has compatible type
                // We'll do a safe check without failing on type mismatch
                if let Err(_) = chrono::DateTime::<chrono::Utc>::try_from(opt_val.t.clone()) {
                    return Err(YdbError::Convert(format!(
                        "Optional value has incompatible type for chrono::DateTime<Utc>"
                    )));
                }

                match opt_val.value {
                    Some(val) => {
                        let dt = chrono::DateTime::<chrono::Utc>::try_from(val)?;
                        Ok(Some(dt))
                    }
                    None => Ok(None),
                }
            }
            // If not optional, try to convert directly
            value => Ok(Some(value.try_into()?)),
        }
    }
}

/// Converts YDB optional datetime/timestamp to Option<NaiveDateTime>
///
/// # Examples
///
/// ```
/// use ydb::Value;
/// use std::time::SystemTime;
/// use chrono::NaiveDateTime;
///
/// // Create an optional datetime value (None case)
/// let opt_value = Value::Optional(Box::new(ydb::ValueOptional {
///     t: Value::DateTime(SystemTime::UNIX_EPOCH),
///     value: None,
/// }));
///
/// // Convert to Option<NaiveDateTime>
/// let opt_dt: Option<NaiveDateTime> = opt_value.try_into().unwrap();
/// assert!(opt_dt.is_none());
/// ```
impl TryFrom<Value> for Option<chrono::NaiveDateTime> {
    type Error = YdbError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Optional(opt_val) => {
                // Check if the optional value has compatible type
                // We'll do a safe check without failing on type mismatch
                if let Err(_) = chrono::NaiveDateTime::try_from(opt_val.t.clone()) {
                    return Err(YdbError::Convert(format!(
                        "Optional value has incompatible type for chrono::NaiveDateTime"
                    )));
                }

                match opt_val.value {
                    Some(val) => {
                        let dt = chrono::NaiveDateTime::try_from(val)?;
                        Ok(Some(dt))
                    }
                    None => Ok(None),
                }
            }
            // If not optional, try to convert directly
            value => Ok(Some(value.try_into()?)),
        }
    }
}

/// Converts YDB optional date to Option<NaiveDate>
///
/// # Examples
///
/// ```
/// use ydb::Value;
/// use std::time::SystemTime;
/// use chrono::NaiveDate;
///
/// // Create an optional date value
/// let now = SystemTime::now();
/// let opt_value = Value::Optional(Box::new(ydb::ValueOptional {
///     t: Value::Date(SystemTime::UNIX_EPOCH),
///     value: Some(Value::Date(now)),
/// }));
///
/// // Convert to Option<NaiveDate>
/// let opt_date: Option<NaiveDate> = opt_value.try_into().unwrap();
/// ```
impl TryFrom<Value> for Option<chrono::NaiveDate> {
    type Error = YdbError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Optional(opt_val) => {
                // Check if the optional value has compatible type
                // We'll do a safe check without failing on type mismatch
                if let Err(_) = chrono::NaiveDate::try_from(opt_val.t.clone()) {
                    return Err(YdbError::Convert(format!(
                        "Optional value has incompatible type for chrono::NaiveDate"
                    )));
                }

                match opt_val.value {
                    Some(val) => {
                        let d = chrono::NaiveDate::try_from(val)?;
                        Ok(Some(d))
                    }
                    None => Ok(None),
                }
            }
            // If not optional, try to convert directly
            value => Ok(Some(value.try_into()?)),
        }
    }
}

impl<T: Into<Value> + Default> From<Option<T>> for Value {
    fn from(from_value: Option<T>) -> Self {
        let t = T::default().into();
        let value = from_value.map(|val| val.into());

        Value::Optional(Box::new(ValueOptional { t, value }))
    }
}

impl<T: Into<Value> + Default> FromIterator<T> for Value {
    fn from_iter<T2: IntoIterator<Item = T>>(iter: T2) -> Self {
        let t: Value = T::default().into();
        let values: Vec<Value> = iter.into_iter().map(|item| item.into()).collect();
        Value::List(Box::new(ValueList { t, values }))
    }
}

impl<T> TryFrom<Value> for Vec<T>
where
    T: TryFrom<Value, Error = YdbError>,
{
    type Error = YdbError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let value = match value {
            Value::List(inner) => inner,
            value => {
                return Err(YdbError::from_str(format!(
                    "can't convert from {} to Vec",
                    value.kind_static()
                )));
            }
        };

        // check list type compatible - for prevent false positive convert empty list
        let list_item_type = value.t.kind_static();
        if TryInto::<T>::try_into(value.t).is_err() {
            let vec_item_type = type_name::<i32>();
            return Err(YdbError::from_str(format!(
                "can't convert list item type '{}' to vec item type '{}'",
                list_item_type, vec_item_type
            )));
        };

        let res: Vec<T> = value
            .values
            .into_iter()
            .map(|item| item.try_into())
            .try_collect()?;
        Ok(res)
    }
}

// From hashmap to Value::Struct
impl From<HashMap<String, Value>> for Value {
    fn from(from_val: HashMap<String, Value>) -> Self {
        let mut value_struct = ValueStruct::with_capacity(from_val.len());
        from_val
            .into_iter()
            .for_each(|(key, val)| value_struct.insert(key, val));
        Value::Struct(value_struct)
    }
}

// From Value::Struct to Hashmap
impl TryFrom<Value> for HashMap<String, Value> {
    type Error = YdbError;

    fn try_from(from_value: Value) -> Result<Self, Self::Error> {
        let kind_name = from_value.kind_static();
        let value_struct = match from_value {
            Value::Struct(value_struct) => value_struct,
            _ => {
                return Err(YdbError::from_str(format!(
                    "failed convert {} to HashMap",
                    kind_name
                )))
            }
        };
        Ok(value_struct.into())
    }
}
