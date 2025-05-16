use serde::{
    de::{self, MapAccess},
    Deserializer,
};

use crate::{Row, Value, YdbError};

pub struct RowDeserializer {
    row: Row,
}

impl RowDeserializer {
    pub fn from_row(row: Row) -> Self {
        Self { row }
    }

    /// Get the first value from Row if it's the only one
    fn get_single_value(&mut self) -> Result<Value, YdbError> {
        let columns = self.row.len();

        if columns == 0 {
            return Err(YdbError::Custom("Row is empty".into()));
        }

        if columns > 1 {
            return Err(YdbError::Custom(format!(
                "Expected Row with single value, but found {} columns",
                columns
            )));
        }

        self.row.remove_field(0)
    }
}

impl<'de> Deserializer<'de> for RowDeserializer {
    type Error = YdbError;

    fn deserialize_any<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // If there is only one value in the Row, deserialize it directly
        if self.row.len() == 1 {
            let value = self.row.remove_field(0)?;
            return ValueDeserializer { value }.deserialize_any(visitor);
        }

        // Otherwise deserialize as a map (structure) using all fields
        visitor.visit_map(RowMapAccess::new(self.row, &[]))
    }

    fn deserialize_bool<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let value = self.get_single_value()?;
        ValueDeserializer { value }.deserialize_bool(visitor)
    }

    fn deserialize_i8<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let value = self.get_single_value()?;
        ValueDeserializer { value }.deserialize_i8(visitor)
    }

    fn deserialize_i16<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let value = self.get_single_value()?;
        ValueDeserializer { value }.deserialize_i16(visitor)
    }

    fn deserialize_i32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let value = self.get_single_value()?;
        ValueDeserializer { value }.deserialize_i32(visitor)
    }

    fn deserialize_i64<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let value = self.get_single_value()?;
        ValueDeserializer { value }.deserialize_i64(visitor)
    }

    fn deserialize_u8<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let value = self.get_single_value()?;
        ValueDeserializer { value }.deserialize_u8(visitor)
    }

    fn deserialize_u16<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let value = self.get_single_value()?;
        ValueDeserializer { value }.deserialize_u16(visitor)
    }

    fn deserialize_u32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let value = self.get_single_value()?;
        ValueDeserializer { value }.deserialize_u32(visitor)
    }

    fn deserialize_u64<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let value = self.get_single_value()?;
        ValueDeserializer { value }.deserialize_u64(visitor)
    }

    fn deserialize_f32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let value = self.get_single_value()?;
        ValueDeserializer { value }.deserialize_f32(visitor)
    }

    fn deserialize_f64<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let value = self.get_single_value()?;
        ValueDeserializer { value }.deserialize_f64(visitor)
    }

    fn deserialize_char<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let value = self.get_single_value()?;
        ValueDeserializer { value }.deserialize_char(visitor)
    }

    fn deserialize_str<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let value = self.get_single_value()?;
        ValueDeserializer { value }.deserialize_str(visitor)
    }

    fn deserialize_string<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let value = self.get_single_value()?;
        ValueDeserializer { value }.deserialize_string(visitor)
    }

    fn deserialize_bytes<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let value = self.get_single_value()?;
        ValueDeserializer { value }.deserialize_bytes(visitor)
    }

    fn deserialize_byte_buf<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let value = self.get_single_value()?;
        ValueDeserializer { value }.deserialize_byte_buf(visitor)
    }

    fn deserialize_option<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // If Row is empty, return none
        if self.row.len() == 0 {
            return visitor.visit_none();
        }

        // Otherwise get the value
        let value = self.get_single_value()?;

        if let Value::Optional(v) = value {
            match v.value {
                Some(value) => visitor.visit_some(ValueDeserializer { value }),
                None => visitor.visit_none(),
            }
        } else {
            Err(YdbError::Convert("Expected Optional".to_string()))
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // Check that Row has only one value and it's a list
        if self.row.len() != 1 {
            return Err(YdbError::Convert(
                "Expected a Row with a single List value for seq deserialization".into(),
            ));
        }

        let value = self.row.remove_field(0)?;

        // Delegate deserialization to ValueDeserializer
        ValueDeserializer { value }.deserialize_seq(visitor)
    }

    fn deserialize_tuple<V>(mut self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // For tuples, use sequence deserialization
        let value = self.get_single_value()?;

        if let Value::List(items) = value {
            let items_len = items.values.len();
            if items_len != len {
                return Err(YdbError::Convert(format!(
                    "Expected tuple of length {}, got list of length {}",
                    len, items_len
                )));
            }

            struct TupleAccess {
                items: Vec<Value>,
                index: usize,
            }

            impl<'de> de::SeqAccess<'de> for TupleAccess {
                type Error = YdbError;

                fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
                where
                    T: de::DeserializeSeed<'de>,
                {
                    if self.index < self.items.len() {
                        let value = std::mem::replace(&mut self.items[self.index], Value::Null);
                        self.index += 1;
                        seed.deserialize(ValueDeserializer { value }).map(Some)
                    } else {
                        Ok(None)
                    }
                }
            }

            visitor.visit_seq(TupleAccess {
                items: items.values,
                index: 0,
            })
        } else {
            Err(YdbError::Convert("Expected List value for tuple".into()))
        }
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // Delegate to regular tuple deserialization
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // For maps, use all fields in Row
        visitor.visit_map(RowMapAccess::new(self.row, &[]))
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // If fields are not specified or empty, use all fields in Row
        visitor.visit_map(RowMapAccess::new(self.row, fields))
    }

    fn deserialize_enum<V>(
        mut self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // For enums, get the value from Row
        let value = self.get_single_value()?;

        // Delegate deserialization to ValueDeserializer
        ValueDeserializer { value }.deserialize_enum(name, variants, visitor)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // Identifiers are usually represented as strings
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // For ignored_any use unit
        visitor.visit_unit()
    }

    fn deserialize_i128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let _ = visitor;
        Err(serde::de::Error::custom("i128 is not supported"))
    }

    fn deserialize_u128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let _ = visitor;
        Err(serde::de::Error::custom("u128 is not supported"))
    }

    fn is_human_readable(&self) -> bool {
        true
    }
}

impl serde::de::Error for crate::YdbError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        YdbError::Custom(msg.to_string())
    }
}

struct RowMapAccess {
    row: Row,
    fields: &'static [&'static str],
    all_columns: Vec<String>,
    index: usize,
}

impl RowMapAccess {
    fn new(row: Row, fields: &'static [&'static str]) -> Self {
        let all_columns = row.get_columns_names();

        RowMapAccess {
            row,
            fields,
            all_columns,
            index: 0,
        }
    }
}

impl<'de> MapAccess<'de> for RowMapAccess {
    type Error = crate::YdbError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: de::DeserializeSeed<'de>,
    {
        if !self.fields.is_empty() {
            // Use specified fields
            if self.index < self.fields.len() {
                let key = self.fields[self.index];
                self.index += 1;
                seed.deserialize(de::value::StrDeserializer::<YdbError>::new(key))
                    .map(Some)
            } else {
                Ok(None)
            }
        } else {
            // Use all columns from Row
            if self.index < self.all_columns.len() {
                let key = &self.all_columns[self.index];
                self.index += 1;
                seed.deserialize(de::value::StrDeserializer::<YdbError>::new(key.as_str()))
                    .map(Some)
            } else {
                Ok(None)
            }
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        let field_name = if !self.fields.is_empty() {
            self.fields[self.index - 1]
        } else {
            &self.all_columns[self.index - 1]
        };

        let value = self.row.remove_field_by_name(field_name)?;
        seed.deserialize(ValueDeserializer { value })
    }
}

struct ValueDeserializer {
    value: Value,
}

impl<'de> Deserializer<'de> for ValueDeserializer {
    type Error = crate::YdbError;

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        if let Value::Int64(i) = self.value {
            visitor.visit_i64(i)
        } else {
            Err(crate::YdbError::Convert("Expected Int64".into()))
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        if let Value::Text(s) = self.value {
            visitor.visit_str(&s)
        } else {
            Err(crate::YdbError::Convert(format!(
                "Expected Text, found: {:?}",
                self.value
            )))
        }
    }

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.value {
            Value::Bool(b) => visitor.visit_bool(b),
            Value::Int8(i) => visitor.visit_i8(i),
            Value::Int16(i) => visitor.visit_i16(i),
            Value::Int32(i) => visitor.visit_i32(i),
            Value::Int64(i) => visitor.visit_i64(i),
            Value::Uint8(i) => visitor.visit_u8(i),
            Value::Uint16(i) => visitor.visit_u16(i),
            Value::Uint32(i) => visitor.visit_u32(i),
            Value::Uint64(i) => visitor.visit_u64(i),
            Value::Float(f) => visitor.visit_f32(f),
            Value::Double(d) => visitor.visit_f64(d),
            Value::Text(s) => visitor.visit_string(s),
            Value::Bytes(b) => visitor.visit_byte_buf(Vec::from(b)),
            _ => Err(crate::YdbError::Convert(format!(
                "Unsupported Value type: {:?}",
                self.value
            ))),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Bool(b) = self.value {
            visitor.visit_bool(b)
        } else {
            Err(crate::YdbError::Convert("Expected Bool".into()))
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Int8(i) = self.value {
            visitor.visit_i8(i)
        } else {
            Err(crate::YdbError::Convert("Expected Int8".into()))
        }
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Int16(i) = self.value {
            visitor.visit_i16(i)
        } else {
            Err(crate::YdbError::Convert("Expected Int16".into()))
        }
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Int32(i) = self.value {
            visitor.visit_i32(i)
        } else {
            Err(crate::YdbError::Convert("Expected Int32".into()))
        }
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Uint8(i) = self.value {
            visitor.visit_u8(i)
        } else {
            Err(crate::YdbError::Convert("Expected Uint8".into()))
        }
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Uint16(i) = self.value {
            visitor.visit_u16(i)
        } else {
            Err(crate::YdbError::Convert("Expected Uint16".into()))
        }
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Uint32(i) = self.value {
            visitor.visit_u32(i)
        } else {
            Err(crate::YdbError::Convert(format!(
                "Expected Uint32, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Uint64(i) = self.value {
            visitor.visit_u64(i)
        } else {
            Err(crate::YdbError::Convert(format!(
                "Expected Uint64, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Float(f) = self.value {
            visitor.visit_f32(f)
        } else {
            Err(crate::YdbError::Convert(format!(
                "Expected Float, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Double(d) = self.value {
            visitor.visit_f64(d)
        } else {
            Err(crate::YdbError::Convert(format!(
                "Expected Double, got {:?}",
                self.value
            )))
        }
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Text(s) = &self.value {
            // Try to get the first character of the string
            let mut chars = s.chars();
            if let Some(c) = chars.next() {
                if chars.next().is_none() {
                    // String contains exactly one character
                    return visitor.visit_char(c);
                }
            }
            Err(crate::YdbError::Convert(
                "Expected a single character Text".into(),
            ))
        } else {
            Err(crate::YdbError::Convert("Expected Text for char".into()))
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Text(s) = self.value {
            visitor.visit_string(s)
        } else if let Value::Bytes(b) = self.value {
            let s = String::from_utf8(Vec::from(b)).map_err(|_| {
                crate::YdbError::Convert("Expected valid UTF-8 bytes for string".into())
            })?;
            visitor.visit_string(s)
        } else {
            Err(crate::YdbError::Convert(format!(
                "Expected Text, found: {:?}",
                self.value
            )))
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Bytes(b) = self.value {
            visitor.visit_bytes(&b)
        } else {
            Err(crate::YdbError::Convert("Expected Bytes".into()))
        }
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Bytes(b) = self.value {
            visitor.visit_byte_buf(Vec::from(b))
        } else {
            Err(crate::YdbError::Convert("Expected Bytes".into()))
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Value::Optional(v) = self.value {
            match v.value {
                Some(value) => visitor.visit_some(ValueDeserializer { value }),
                None => visitor.visit_none(),
            }
        } else {
            Err(YdbError::Convert("Expected Optional".to_string()))
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match &self.value {
            Value::List(items) => {
                struct ListAccess<'a> {
                    items: &'a [Value],
                    index: usize,
                }

                impl<'de, 'a> de::SeqAccess<'de> for ListAccess<'a> {
                    type Error = YdbError;

                    fn next_element_seed<T>(
                        &mut self,
                        seed: T,
                    ) -> Result<Option<T::Value>, Self::Error>
                    where
                        T: de::DeserializeSeed<'de>,
                    {
                        if self.index < self.items.len() {
                            let value = &self.items[self.index];
                            self.index += 1;
                            seed.deserialize(ValueDeserializer {
                                value: value.clone(),
                            })
                            .map(Some)
                        } else {
                            Ok(None)
                        }
                    }
                }

                visitor.visit_seq(ListAccess {
                    items: &items.values,
                    index: 0,
                })
            }
            _ => Err(YdbError::Convert("Expected List value".into())),
        }
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match &self.value {
            Value::List(items) => {
                let items_len = items.values.len();
                if items_len != len {
                    return Err(YdbError::Convert(format!(
                        "Expected tuple of length {}, got list of length {}",
                        len, items_len
                    )));
                }

                struct TupleAccess {
                    items: Vec<Value>,
                    index: usize,
                }

                impl<'de> de::SeqAccess<'de> for TupleAccess {
                    type Error = YdbError;

                    fn next_element_seed<T>(
                        &mut self,
                        seed: T,
                    ) -> Result<Option<T::Value>, Self::Error>
                    where
                        T: de::DeserializeSeed<'de>,
                    {
                        if self.index < self.items.len() {
                            let value = std::mem::replace(&mut self.items[self.index], Value::Null);
                            self.index += 1;
                            seed.deserialize(ValueDeserializer { value }).map(Some)
                        } else {
                            Ok(None)
                        }
                    }
                }

                visitor.visit_seq(TupleAccess {
                    items: items.values.clone(),
                    index: 0,
                })
            }
            _ => Err(YdbError::Convert("Expected List value for tuple".into())),
        }
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // Delegate to regular tuple deserialization
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // In YDB, Dict is not directly supported, return an error
        Err(YdbError::Convert(
            "Map deserialization is not directly supported for Values".into(),
        ))
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // Structures in YDB are usually represented as Row, not as Value
        Err(YdbError::Convert(
            "Struct deserialization is not directly supported for Values".into(),
        ))
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // For string representation of enum
        struct EnumStringAccess<'a> {
            variant: &'a str,
        }

        // For string representation of enum
        struct EnumBytesAccess<'a> {
            variant: &'a [u8],
        }



        impl<'de, 'a> de::EnumAccess<'de> for EnumStringAccess<'a> {
            type Error = YdbError;
            type Variant = UnitVariantAccess;

            fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
            where
                V: de::DeserializeSeed<'de>,
            {
                let variant = self.variant;
                let val = seed.deserialize(de::value::StrDeserializer::<YdbError>::new(variant))?;
                Ok((val, UnitVariantAccess))
            }
        }

        impl<'de, 'a> de::EnumAccess<'de> for EnumBytesAccess<'a> {
            type Error = YdbError;
            type Variant = UnitVariantAccess;

            fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
            where
                V: de::DeserializeSeed<'de>,
            {
                let variant = self.variant;
                let val =
                    seed.deserialize(de::value::BytesDeserializer::<YdbError>::new(variant))?;
                Ok((val, UnitVariantAccess))
            }
        }

        struct UnitVariantAccess;

        impl<'de> de::VariantAccess<'de> for UnitVariantAccess {
            type Error = YdbError;

            fn unit_variant(self) -> Result<(), Self::Error> {
                Ok(())
            }

            fn newtype_variant_seed<T>(self, _seed: T) -> Result<T::Value, Self::Error>
            where
                T: de::DeserializeSeed<'de>,
            {
                Err(YdbError::Convert("Newtype variants not supported".into()))
            }

            fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
            where
                V: de::Visitor<'de>,
            {
                Err(YdbError::Convert("Tuple variants not supported".into()))
            }

            fn struct_variant<V>(
                self,
                _fields: &'static [&'static str],
                _visitor: V,
            ) -> Result<V::Value, Self::Error>
            where
                V: de::Visitor<'de>,
            {
                Err(YdbError::Convert("Struct variants not supported".into()))
            }
        }
        // For enumerations in YDB, strings or integers are most commonly used
        match &self.value {
            Value::Text(s) => visitor.visit_enum(EnumStringAccess { variant: &s }),
            Value::Bytes(b) => visitor.visit_enum(EnumBytesAccess { variant: &b }),
            // Используем нативные методы визитора для разных числовых типов
            Value::Int8(i) => visitor.visit_i8(*i),
            Value::Int16(i) => visitor.visit_i16(*i),
            Value::Int32(i) => visitor.visit_i32(*i),
            Value::Int64(i) => visitor.visit_i64(*i),
            Value::Uint8(i) => visitor.visit_u8(*i),
            Value::Uint16(i) => visitor.visit_u16(*i),
            Value::Uint32(i) => visitor.visit_u32(*i),
            Value::Uint64(i) => visitor.visit_u64(*i),
            _ => Err(YdbError::Convert(format!(
                "Expected String or Integer for enum, got {:?}",
                self.value
            ))),
        }
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // Identifiers are usually represented as strings
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // For ignored_any use unit
        visitor.visit_unit()
    }
}
