use std::collections::{BTreeMap, HashMap};
use std::ops::Not;
use std::sync::{Arc, Mutex, OnceLock};

use modular_agent_kit::{AgentError, AgentValue};
use arrow_array::builder::{FixedSizeListBuilder, Float16Builder, Float32Builder, Float64Builder};
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float16Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, NullArray, RecordBatch, StringArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode};
use half::f16;
use im::vector;
use lancedb::{Connection, connect};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DbSchema {
    /// A sequence of fields that describe the schema.
    pub fields: DbFields,
    /// A map of key-value pairs containing additional meta data.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct DbFields(Arc<[DbFieldRef]>);

pub type DbFieldRef = Arc<DbField>;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DbField {
    name: String,
    data_type: DbDataType,
    #[serde(default, skip_serializing_if = "<&bool>::not")]
    nullable: bool,
    /// A map of key-value pairs containing additional custom meta data.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum DbDataType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Timestamp(TimeUnit, Option<Arc<str>>),
    Date32,
    Date64,
    Time32(TimeUnit),
    Time64(TimeUnit),
    Duration(TimeUnit),
    Interval(IntervalUnit),
    Binary,
    FixedSizeBinary(i32),
    LargeBinary,
    // BinaryView,
    Utf8,
    LargeUtf8,
    // Utf8View,
    List(DbFieldRef),
    // ListView(DbFieldRef),
    FixedSizeList(DbFieldRef, i32),
    LargeList(DbFieldRef),
    // LargeListView(DbFieldRef),
    Struct(DbFields),
    Union(DbUnionFields, UnionMode),
    Dictionary(Box<DbDataType>, Box<DbDataType>),
    Decimal32(u8, i8),
    Decimal64(u8, i8),
    Decimal128(u8, i8),
    Decimal256(u8, i8),
    Map(DbFieldRef, bool),
    // RunEndEncoded(DbFieldRef, DbFieldRef),
}

#[derive(Clone, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct DbUnionFields(Arc<[(i8, DbFieldRef)]>);

impl std::fmt::Debug for DbUnionFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.as_ref().fmt(f)
    }
}

impl From<DbSchema> for Schema {
    fn from(db_schema: DbSchema) -> Self {
        let fields = db_schema
            .fields
            .0
            .iter()
            .map(|field_ref| from_db_field(field_ref))
            .collect::<Vec<_>>();
        Self::new(fields).with_metadata(db_schema.metadata.clone())
    }
}

fn from_db_field(db_field: &DbFieldRef) -> Field {
    Field::new(
        &db_field.name,
        from_db_data_type(&db_field.data_type),
        db_field.nullable,
    )
    .with_metadata(db_field.metadata.clone())
}

fn from_db_data_type(db_data_type: &DbDataType) -> DataType {
    match db_data_type {
        DbDataType::Null => DataType::Null,
        DbDataType::Boolean => DataType::Boolean,
        DbDataType::Int8 => DataType::Int8,
        DbDataType::Int16 => DataType::Int16,
        DbDataType::Int32 => DataType::Int32,
        DbDataType::Int64 => DataType::Int64,
        DbDataType::UInt8 => DataType::UInt8,
        DbDataType::UInt16 => DataType::UInt16,
        DbDataType::UInt32 => DataType::UInt32,
        DbDataType::UInt64 => DataType::UInt64,
        DbDataType::Float16 => DataType::Float16,
        DbDataType::Float32 => DataType::Float32,
        DbDataType::Float64 => DataType::Float64,
        DbDataType::Timestamp(unit, tz) => DataType::Timestamp(unit.clone(), tz.clone()),
        DbDataType::Date32 => DataType::Date32,
        DbDataType::Date64 => DataType::Date64,
        DbDataType::Time32(unit) => DataType::Time32(unit.clone()),
        DbDataType::Time64(unit) => DataType::Time64(unit.clone()),
        DbDataType::Duration(unit) => DataType::Duration(unit.clone()),
        DbDataType::Interval(unit) => DataType::Interval(unit.clone()),
        DbDataType::Binary => DataType::Binary,
        DbDataType::FixedSizeBinary(size) => DataType::FixedSizeBinary(*size),
        DbDataType::LargeBinary => DataType::LargeBinary,
        DbDataType::Utf8 => DataType::Utf8,
        DbDataType::LargeUtf8 => DataType::LargeUtf8,
        DbDataType::List(field_ref) => DataType::List(Arc::new(from_db_field(field_ref))),
        DbDataType::FixedSizeList(field_ref, size) => {
            DataType::FixedSizeList(Arc::new(from_db_field(field_ref)), *size)
        }
        DbDataType::LargeList(field_ref) => DataType::LargeList(Arc::new(from_db_field(field_ref))),
        DbDataType::Struct(db_fields) => {
            let fields = db_fields
                .0
                .iter()
                .map(|field_ref| from_db_field(field_ref))
                .collect::<Vec<_>>();
            DataType::Struct(fields.into())
        }
        DbDataType::Union(db_union_fields, mode) => {
            let fields: UnionFields = db_union_fields
                .0
                .iter()
                .map(|(id, field_ref)| (*id, Arc::new(from_db_field(field_ref))))
                .collect();
            DataType::Union(fields, mode.clone())
        }
        DbDataType::Dictionary(key_type, value_type) => DataType::Dictionary(
            Box::new(from_db_data_type(key_type)),
            Box::new(from_db_data_type(value_type)),
        ),
        DbDataType::Decimal32(precision, scale) => DataType::Decimal32(*precision, *scale),
        DbDataType::Decimal64(precision, scale) => DataType::Decimal64(*precision, *scale),
        DbDataType::Decimal128(precision, scale) => DataType::Decimal128(*precision, *scale),
        DbDataType::Decimal256(precision, scale) => DataType::Decimal256(*precision, *scale),
        DbDataType::Map(field_ref, keys_sorted) => {
            DataType::Map(Arc::new(from_db_field(field_ref)), *keys_sorted)
        }
    }
}

pub fn agent_value_to_record_batch(
    schema: Arc<Schema>,
    value: AgentValue,
) -> Result<RecordBatch, AgentError> {
    // Check if the value is an array, and if not, convert it to a single-element array
    let value_arr = if value.is_array() {
        value.into_array().unwrap()
    } else {
        vector![value]
    };

    // Extract data for each field and create a RecordBatch.
    let mut records = Vec::new();
    for field in schema.fields() {
        let field_name = field.name();
        let nullable = field.is_nullable();
        let mut arr = Vec::new();
        for v in &value_arr {
            let Some(obj) = v.as_object() else {
                return Err(AgentError::InvalidValue(
                    "Expected AgentValue to be an object".to_string(),
                ));
            };
            let v = obj.get(field_name);
            if v.is_none() && !nullable {
                return Err(AgentError::InvalidValue(format!(
                    "Missing non-nullable field '{}' in AgentValue",
                    field_name
                )));
            }
            arr.push(v.cloned());
        }
        records.push(agent_array_to_arrow_array(field, arr)?);
    }

    RecordBatch::try_new(schema, records)
        .map_err(|e| AgentError::InvalidValue(format!("Failed to create RecordBatch: {}", e)))
}

fn agent_array_to_arrow_array(
    field: &Field,
    values: Vec<Option<AgentValue>>,
) -> Result<ArrayRef, AgentError> {
    let nullable = field.is_nullable();
    match field.data_type() {
        DataType::Null => {
            let arr = NullArray::new(values.len());
            Ok(Arc::new(arr))
        }
        DataType::Boolean => {
            let arr = if nullable {
                let vs: Vec<Option<bool>> = values
                    .iter()
                    .map(|opt_v| opt_v.clone().and_then(|v| v.to_boolean()))
                    .collect();
                BooleanArray::from(vs)
            } else {
                let vs: Vec<bool> = values
                    .into_iter()
                    .map(|opt_v| {
                        opt_v
                            .map(|v| v.to_boolean().unwrap_or_default())
                            .unwrap_or_default()
                    })
                    .collect();
                BooleanArray::from(vs)
            };
            Ok(Arc::new(arr))
        }
        DataType::Int8 => {
            let arr = if nullable {
                let vs: Vec<Option<i8>> = values
                    .iter()
                    .map(|opt_v| opt_v.clone().and_then(|v| v.to_integer().map(|i| i as i8)))
                    .collect();
                Int8Array::from(vs)
            } else {
                let vs: Vec<i8> = values
                    .into_iter()
                    .map(|opt_v| {
                        opt_v
                            .map(|v| v.to_integer().map(|i| i as i8).unwrap_or_default())
                            .unwrap_or_default()
                    })
                    .collect();
                Int8Array::from(vs)
            };
            Ok(Arc::new(arr))
        }
        DataType::Int16 => {
            let arr = if nullable {
                let vs: Vec<Option<i16>> = values
                    .iter()
                    .map(|opt_v| opt_v.clone().and_then(|v| v.to_integer().map(|i| i as i16)))
                    .collect();
                Int16Array::from(vs)
            } else {
                let vs: Vec<i16> = values
                    .into_iter()
                    .map(|opt_v| {
                        opt_v
                            .map(|v| v.to_integer().map(|i| i as i16).unwrap_or_default())
                            .unwrap_or_default()
                    })
                    .collect();
                Int16Array::from(vs)
            };
            Ok(Arc::new(arr))
        }
        DataType::Int32 => {
            let arr = if nullable {
                let vs: Vec<Option<i32>> = values
                    .iter()
                    .map(|opt_v| opt_v.clone().and_then(|v| v.to_integer().map(|i| i as i32)))
                    .collect();
                Int32Array::from(vs)
            } else {
                let vs: Vec<i32> = values
                    .into_iter()
                    .map(|opt_v| {
                        opt_v
                            .map(|v| v.to_integer().map(|i| i as i32).unwrap_or_default())
                            .unwrap_or_default()
                    })
                    .collect();
                Int32Array::from(vs)
            };
            Ok(Arc::new(arr))
        }
        DataType::Int64 => {
            let arr = if nullable {
                let vs: Vec<Option<i64>> = values
                    .iter()
                    .map(|opt_v| opt_v.clone().and_then(|v| v.to_integer().map(|i| i as i64)))
                    .collect();
                Int64Array::from(vs)
            } else {
                let vs: Vec<i64> = values
                    .into_iter()
                    .map(|opt_v| {
                        opt_v
                            .map(|v| v.to_integer().map(|i| i as i64).unwrap_or_default())
                            .unwrap_or_default()
                    })
                    .collect();
                Int64Array::from(vs)
            };
            Ok(Arc::new(arr))
        }
        DataType::UInt8 => {
            let arr = if nullable {
                let vs: Vec<Option<u8>> = values
                    .iter()
                    .map(|opt_v| opt_v.clone().and_then(|v| v.to_integer().map(|i| i as u8)))
                    .collect();
                UInt8Array::from(vs)
            } else {
                let vs: Vec<u8> = values
                    .into_iter()
                    .map(|opt_v| {
                        opt_v
                            .map(|v| v.to_integer().map(|i| i as u8).unwrap_or_default())
                            .unwrap_or_default()
                    })
                    .collect();
                UInt8Array::from(vs)
            };
            Ok(Arc::new(arr))
        }
        DataType::UInt16 => {
            let arr = if nullable {
                let vs: Vec<Option<u16>> = values
                    .iter()
                    .map(|opt_v| opt_v.clone().and_then(|v| v.to_integer().map(|i| i as u16)))
                    .collect();
                UInt16Array::from(vs)
            } else {
                let vs: Vec<u16> = values
                    .into_iter()
                    .map(|opt_v| {
                        opt_v
                            .map(|v| v.to_integer().map(|i| i as u16).unwrap_or_default())
                            .unwrap_or_default()
                    })
                    .collect();
                UInt16Array::from(vs)
            };
            Ok(Arc::new(arr))
        }
        DataType::UInt32 => {
            let arr = if nullable {
                let vs: Vec<Option<u32>> = values
                    .iter()
                    .map(|opt_v| opt_v.clone().and_then(|v| v.to_integer().map(|i| i as u32)))
                    .collect();
                UInt32Array::from(vs)
            } else {
                let vs: Vec<u32> = values
                    .into_iter()
                    .map(|opt_v| {
                        opt_v
                            .map(|v| v.to_integer().map(|i| i as u32).unwrap_or_default())
                            .unwrap_or_default()
                    })
                    .collect();
                UInt32Array::from(vs)
            };
            Ok(Arc::new(arr))
        }
        DataType::UInt64 => {
            let arr = if nullable {
                let vs: Vec<Option<u64>> = values
                    .iter()
                    .map(|opt_v| opt_v.clone().and_then(|v| v.to_integer().map(|i| i as u64)))
                    .collect();
                UInt64Array::from(vs)
            } else {
                let vs: Vec<u64> = values
                    .into_iter()
                    .map(|opt_v| {
                        opt_v
                            .map(|v| v.to_integer().map(|i| i as u64).unwrap_or_default())
                            .unwrap_or_default()
                    })
                    .collect();
                UInt64Array::from(vs)
            };
            Ok(Arc::new(arr))
        }
        DataType::Float16 => {
            let arr = if nullable {
                let vs: Vec<Option<f16>> = values
                    .iter()
                    .map(|opt_v| {
                        opt_v
                            .clone()
                            .and_then(|v| v.to_number().map(|f| f16::from_f64(f)))
                    })
                    .collect();
                Float16Array::from(vs)
            } else {
                let vs: Vec<f16> = values
                    .into_iter()
                    .map(|opt_v| {
                        opt_v
                            .map(|v| v.to_number().map(|f| f16::from_f64(f)).unwrap_or_default())
                            .unwrap_or_default()
                    })
                    .collect();
                Float16Array::from(vs)
            };
            Ok(Arc::new(arr))
        }
        DataType::Float32 => {
            let arr = if nullable {
                let vs: Vec<Option<f32>> = values
                    .iter()
                    .map(|opt_v| opt_v.clone().and_then(|v| v.to_number().map(|f| f as f32)))
                    .collect();
                Float32Array::from(vs)
            } else {
                let vs: Vec<f32> = values
                    .into_iter()
                    .map(|opt_v| {
                        opt_v
                            .map(|v| v.to_number().map(|f| f as f32).unwrap_or_default())
                            .unwrap_or_default()
                    })
                    .collect();
                Float32Array::from(vs)
            };
            Ok(Arc::new(arr))
        }
        DataType::Float64 => {
            let arr = if nullable {
                let vs: Vec<Option<f64>> = values
                    .iter()
                    .map(|opt_v| opt_v.clone().and_then(|v| v.to_number().map(|f| f as f64)))
                    .collect();
                Float64Array::from(vs)
            } else {
                let vs: Vec<f64> = values
                    .into_iter()
                    .map(|opt_v| {
                        opt_v
                            .map(|v| v.to_number().map(|f| f as f64).unwrap_or_default())
                            .unwrap_or_default()
                    })
                    .collect();
                Float64Array::from(vs)
            };
            Ok(Arc::new(arr))
        }
        DataType::Utf8 => {
            let arr = if nullable {
                let vs: Vec<Option<String>> = values
                    .iter()
                    .map(|opt_v| opt_v.clone().and_then(|v| v.to_string()))
                    .collect();
                StringArray::from(vs)
            } else {
                let vs: Vec<String> = values
                    .into_iter()
                    .map(|opt_v| {
                        opt_v
                            .map(|v| v.to_string().unwrap_or_default())
                            .unwrap_or_default()
                    })
                    .collect();
                StringArray::from(vs)
            };
            Ok(Arc::new(arr))
        }
        DataType::FixedSizeList(field_ref, size) => {
            match field_ref.data_type() {
                DataType::Float16 => {
                    let values_builder = Float16Builder::new();
                    let mut builder = FixedSizeListBuilder::new(values_builder, *size);
                    for value in values {
                        if let Some(v) = value {
                            let Some(arr) = v.into_array() else {
                                return Err(AgentError::InvalidValue(format!(
                                    "Expected AgentValue to be an array for field '{}'",
                                    field.name()
                                )));
                            };
                            for item in arr.iter() {
                                if let Some(f) = item.to_number() {
                                    builder.values().append_value(f16::from_f64(f));
                                } else {
                                    builder.values().append_null();
                                }
                            }
                            builder.append(true);
                        } else {
                            // Append null for the entire FixedSizeList
                            for _ in 0..*size {
                                builder.values().append_null();
                            }
                            builder.append(false);
                        }
                    }
                    let arr = builder.finish();
                    Ok(Arc::new(arr))
                }
                DataType::Float32 => {
                    let values_builder = Float32Builder::new();
                    let mut builder = FixedSizeListBuilder::new(values_builder, *size);
                    for value in values {
                        if let Some(v) = value {
                            match v {
                                AgentValue::Tensor(tensor) => {
                                    builder.values().append_slice(tensor.as_slice());
                                }
                                AgentValue::Array(arr) => {
                                    for item in arr.iter() {
                                        if let Some(f) = item.to_number() {
                                            builder.values().append_value(f as f32);
                                        } else {
                                            builder.values().append_null();
                                        }
                                    }
                                }
                                _ => {
                                    return Err(AgentError::InvalidValue(format!(
                                        "Expected AgentValue to be a tensor or array for field '{}'",
                                        field.name()
                                    )));
                                }
                            }
                            builder.append(true);
                        } else {
                            // Append null for the entire FixedSizeList
                            for _ in 0..*size {
                                builder.values().append_null();
                            }
                            builder.append(false);
                        }
                    }
                    let arr = builder.finish();
                    Ok(Arc::new(arr))
                }
                DataType::Float64 => {
                    let values_builder = Float64Builder::new();
                    let mut builder = FixedSizeListBuilder::new(values_builder, *size);
                    for value in values {
                        if let Some(v) = value {
                            let Some(arr) = v.into_array() else {
                                return Err(AgentError::InvalidValue(format!(
                                    "Expected AgentValue to be an array for field '{}'",
                                    field.name()
                                )));
                            };
                            for item in arr.iter() {
                                if let Some(f) = item.to_number() {
                                    builder.values().append_value(f);
                                } else {
                                    builder.values().append_null();
                                }
                            }
                            builder.append(true);
                        } else {
                            // Append null for the entire FixedSizeList
                            for _ in 0..*size {
                                builder.values().append_null();
                            }
                            builder.append(false);
                        }
                    }
                    let arr = builder.finish();
                    Ok(Arc::new(arr))
                }
                _ => Err(AgentError::InvalidValue(format!(
                    "Unsupported data type for FixedSizeList field '{}'",
                    field.name()
                ))),
            }
        }
        _ => Err(AgentError::InvalidValue(format!(
            "Unsupported data type for field '{}'",
            field.name()
        ))),
    }
}

pub fn record_batches_to_agent_value(batches: Vec<RecordBatch>) -> Result<AgentValue, AgentError> {
    let mut total_records = Vec::new();
    for batch in &batches {
        let num_rows = batch.num_rows();
        let mut records = (0..num_rows)
            .map(|_| im::HashMap::new())
            .collect::<Vec<_>>();
        for (col_index, field) in batch.schema().fields().iter().enumerate() {
            let column = batch.column(col_index);
            match field.data_type() {
                DataType::Null => {
                    let null_array =
                        column.as_any().downcast_ref::<NullArray>().ok_or_else(|| {
                            AgentError::InvalidValue(format!(
                                "Failed to downcast column '{}' to NullArray",
                                field.name()
                            ))
                        })?;
                    for row_index in 0..null_array.len() {
                        records[row_index].insert(field.name().clone(), AgentValue::Unit);
                    }
                }
                DataType::Boolean => {
                    let boolean_array =
                        column
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .ok_or_else(|| {
                                AgentError::InvalidValue(format!(
                                    "Failed to downcast column '{}' to BooleanArray",
                                    field.name()
                                ))
                            })?;
                    for (row_index, a) in boolean_array.iter().enumerate() {
                        let v = if a.is_none() {
                            AgentValue::Unit
                        } else {
                            AgentValue::boolean(a.unwrap())
                        };
                        records[row_index].insert(field.name().clone(), v);
                    }
                }
                DataType::Int8 => {
                    let int_array =
                        column.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                            AgentError::InvalidValue(format!(
                                "Failed to downcast column '{}' to Int8Array",
                                field.name()
                            ))
                        })?;
                    for (row_index, a) in int_array.iter().enumerate() {
                        let v = if a.is_none() {
                            AgentValue::Unit
                        } else {
                            AgentValue::integer(a.unwrap() as i64)
                        };
                        records[row_index].insert(field.name().clone(), v);
                    }
                }
                DataType::Int16 => {
                    let int_array =
                        column
                            .as_any()
                            .downcast_ref::<Int16Array>()
                            .ok_or_else(|| {
                                AgentError::InvalidValue(format!(
                                    "Failed to downcast column '{}' to Int16Array",
                                    field.name()
                                ))
                            })?;
                    for (row_index, a) in int_array.iter().enumerate() {
                        let v = if a.is_none() {
                            AgentValue::Unit
                        } else {
                            AgentValue::integer(a.unwrap() as i64)
                        };
                        records[row_index].insert(field.name().clone(), v);
                    }
                }
                DataType::Int32 => {
                    let int_array =
                        column
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .ok_or_else(|| {
                                AgentError::InvalidValue(format!(
                                    "Failed to downcast column '{}' to Int32Array",
                                    field.name()
                                ))
                            })?;
                    for (row_index, a) in int_array.iter().enumerate() {
                        let v = if a.is_none() {
                            AgentValue::Unit
                        } else {
                            AgentValue::integer(a.unwrap() as i64)
                        };
                        records[row_index].insert(field.name().clone(), v);
                    }
                }
                DataType::Int64 => {
                    let int_array =
                        column
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| {
                                AgentError::InvalidValue(format!(
                                    "Failed to downcast column '{}' to Int64Array",
                                    field.name()
                                ))
                            })?;
                    for (row_index, a) in int_array.iter().enumerate() {
                        let v = if a.is_none() {
                            AgentValue::Unit
                        } else {
                            AgentValue::integer(a.unwrap())
                        };
                        records[row_index].insert(field.name().clone(), v);
                    }
                }
                DataType::UInt8 => {
                    let int_array =
                        column
                            .as_any()
                            .downcast_ref::<UInt8Array>()
                            .ok_or_else(|| {
                                AgentError::InvalidValue(format!(
                                    "Failed to downcast column '{}' to UInt8Array",
                                    field.name()
                                ))
                            })?;
                    for (row_index, a) in int_array.iter().enumerate() {
                        let v = if a.is_none() {
                            AgentValue::Unit
                        } else {
                            AgentValue::integer(a.unwrap() as i64)
                        };
                        records[row_index].insert(field.name().clone(), v);
                    }
                }
                DataType::UInt16 => {
                    let int_array =
                        column
                            .as_any()
                            .downcast_ref::<UInt16Array>()
                            .ok_or_else(|| {
                                AgentError::InvalidValue(format!(
                                    "Failed to downcast column '{}' to UInt16Array",
                                    field.name()
                                ))
                            })?;
                    for (row_index, a) in int_array.iter().enumerate() {
                        let v = if a.is_none() {
                            AgentValue::Unit
                        } else {
                            AgentValue::integer(a.unwrap() as i64)
                        };
                        records[row_index].insert(field.name().clone(), v);
                    }
                }
                DataType::UInt32 => {
                    let int_array =
                        column
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .ok_or_else(|| {
                                AgentError::InvalidValue(format!(
                                    "Failed to downcast column '{}' to UInt32Array",
                                    field.name()
                                ))
                            })?;
                    for (row_index, a) in int_array.iter().enumerate() {
                        let v = if a.is_none() {
                            AgentValue::Unit
                        } else {
                            AgentValue::integer(a.unwrap() as i64)
                        };
                        records[row_index].insert(field.name().clone(), v);
                    }
                }
                DataType::UInt64 => {
                    let int_array =
                        column
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .ok_or_else(|| {
                                AgentError::InvalidValue(format!(
                                    "Failed to downcast column '{}' to UInt64Array",
                                    field.name()
                                ))
                            })?;
                    for (row_index, a) in int_array.iter().enumerate() {
                        let v = if a.is_none() {
                            AgentValue::Unit
                        } else {
                            AgentValue::integer(a.unwrap() as i64)
                        };
                        records[row_index].insert(field.name().clone(), v);
                    }
                }
                DataType::Float16 => {
                    let float_array =
                        column
                            .as_any()
                            .downcast_ref::<Float16Array>()
                            .ok_or_else(|| {
                                AgentError::InvalidValue(format!(
                                    "Failed to downcast column '{}' to Float16Array",
                                    field.name()
                                ))
                            })?;
                    for (row_index, a) in float_array.iter().enumerate() {
                        let v = if a.is_none() {
                            AgentValue::Unit
                        } else {
                            AgentValue::number(f16::to_f64(a.unwrap()))
                        };
                        records[row_index].insert(field.name().clone(), v);
                    }
                }
                DataType::Float32 => {
                    let float_array =
                        column
                            .as_any()
                            .downcast_ref::<Float32Array>()
                            .ok_or_else(|| {
                                AgentError::InvalidValue(format!(
                                    "Failed to downcast column '{}' to Float32Array",
                                    field.name()
                                ))
                            })?;
                    for (row_index, a) in float_array.iter().enumerate() {
                        let v = if a.is_none() {
                            AgentValue::Unit
                        } else {
                            AgentValue::number(a.unwrap() as f64)
                        };
                        records[row_index].insert(field.name().clone(), v);
                    }
                }
                DataType::Float64 => {
                    let float_array =
                        column
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| {
                                AgentError::InvalidValue(format!(
                                    "Failed to downcast column '{}' to Float64Array",
                                    field.name()
                                ))
                            })?;
                    for (row_index, a) in float_array.iter().enumerate() {
                        let v = if a.is_none() {
                            AgentValue::Unit
                        } else {
                            AgentValue::number(a.unwrap())
                        };
                        records[row_index].insert(field.name().clone(), v);
                    }
                }
                DataType::Utf8 => {
                    let string_array =
                        column
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                AgentError::InvalidValue(format!(
                                    "Failed to downcast column '{}' to StringArray",
                                    field.name()
                                ))
                            })?;
                    // for (row_index, obj) in records.iter_mut().enumerate() {
                    for (row_index, a) in string_array.iter().enumerate() {
                        let v = if a.is_none() {
                            AgentValue::Unit
                        } else {
                            AgentValue::string(a.unwrap())
                        };
                        records[row_index].insert(field.name().clone(), v);
                    }
                }
                DataType::FixedSizeList(field_ref, size) => match field_ref.data_type() {
                    DataType::Float16 => {
                        let list_array = column
                            .as_any()
                            .downcast_ref::<FixedSizeListArray>()
                            .ok_or_else(|| {
                                AgentError::InvalidValue(format!(
                                    "Failed to downcast column '{}' to FixedSizeListArray",
                                    field.name()
                                ))
                            })?;
                        let values_array = list_array.values();
                        let float_array = values_array
                            .as_any()
                            .downcast_ref::<Float16Array>()
                            .ok_or_else(|| {
                                AgentError::InvalidValue(format!(
                                    "Failed to downcast values of column '{}' to Float16Array",
                                    field.name()
                                ))
                            })?;
                        for row_index in 0..list_array.len() {
                            if list_array.is_null(row_index) {
                                records[row_index].insert(field.name().clone(), AgentValue::Unit);
                            } else {
                                let start = row_index * (*size as usize);
                                let end = start + (*size as usize);
                                let mut arr = Vec::new();
                                for i in start..end {
                                    if float_array.is_null(i) {
                                        arr.push(AgentValue::Unit);
                                    } else {
                                        arr.push(AgentValue::number(f16::to_f64(
                                            float_array.value(i),
                                        )));
                                    }
                                }
                                records[row_index]
                                    .insert(field.name().clone(), AgentValue::array(arr.into()));
                            }
                        }
                    }
                    DataType::Float32 => {
                        let list_array = column
                            .as_any()
                            .downcast_ref::<FixedSizeListArray>()
                            .ok_or_else(|| {
                                AgentError::InvalidValue(format!(
                                    "Failed to downcast column '{}' to FixedSizeListArray",
                                    field.name()
                                ))
                            })?;
                        let values_array = list_array.values();
                        let float_array = values_array
                            .as_any()
                            .downcast_ref::<Float32Array>()
                            .ok_or_else(|| {
                                AgentError::InvalidValue(format!(
                                    "Failed to downcast values of column '{}' to Float32Array",
                                    field.name()
                                ))
                            })?;
                        for row_index in 0..list_array.len() {
                            if list_array.is_null(row_index) {
                                records[row_index].insert(field.name().clone(), AgentValue::Unit);
                            } else {
                                let start = row_index * (*size as usize);
                                let end = start + (*size as usize);
                                let mut arr = Vec::new();
                                for i in start..end {
                                    if float_array.is_null(i) {
                                        arr.push(0.);
                                    } else {
                                        arr.push(float_array.value(i));
                                    }
                                }
                                records[row_index]
                                    .insert(field.name().clone(), AgentValue::tensor(arr)); // Using tensor for Float32 arrays
                            }
                        }
                    }
                    DataType::Float64 => {
                        let list_array = column
                            .as_any()
                            .downcast_ref::<FixedSizeListArray>()
                            .ok_or_else(|| {
                                AgentError::InvalidValue(format!(
                                    "Failed to downcast column '{}' to FixedSizeListArray",
                                    field.name()
                                ))
                            })?;
                        let values_array = list_array.values();
                        let float_array = values_array
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| {
                                AgentError::InvalidValue(format!(
                                    "Failed to downcast values of column '{}' to Float64Array",
                                    field.name()
                                ))
                            })?;
                        for row_index in 0..list_array.len() {
                            if list_array.is_null(row_index) {
                                records[row_index].insert(field.name().clone(), AgentValue::Unit);
                            } else {
                                let start = row_index * (*size as usize);
                                let end = start + (*size as usize);
                                let mut arr = Vec::new();
                                for i in start..end {
                                    if float_array.is_null(i) {
                                        arr.push(AgentValue::Unit);
                                    } else {
                                        arr.push(AgentValue::number(float_array.value(i)));
                                    }
                                }
                                records[row_index]
                                    .insert(field.name().clone(), AgentValue::array(arr.into()));
                            }
                        }
                    }
                    _ => {
                        return Err(AgentError::InvalidValue(format!(
                            "Unsupported data type for FixedSizeList field '{}'",
                            field.name()
                        )));
                    }
                },
                _ => {
                    return Err(AgentError::InvalidValue(format!(
                        "Unsupported data type for field '{}'",
                        field.name()
                    )));
                }
            };
        }
        total_records.extend(records);
    }
    Ok(AgentValue::array(
        total_records
            .into_iter()
            .map(|obj| AgentValue::object(obj))
            .collect::<Vec<_>>()
            .into(),
    ))
}

static DB_MAP: OnceLock<Mutex<BTreeMap<String, Connection>>> = OnceLock::new();

pub async fn get_db_connection(uri: &str) -> Result<Connection, AgentError> {
    let db_map = DB_MAP.get_or_init(|| Mutex::new(BTreeMap::new()));
    {
        let map_guard = db_map.lock().unwrap();
        if let Some(db) = map_guard.get(uri) {
            return Ok(db.clone());
        }
    }

    let db = connect(uri)
        .execute()
        .await
        .map_err(|e| AgentError::IoError(format!("LanceDB Connection Error: {}", e)))?;

    let mut map_guard = db_map.lock().unwrap();
    map_guard.insert(uri.to_string(), db.clone());

    Ok(db)
}
