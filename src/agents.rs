use std::sync::Arc;

use agent_stream_kit::{
    ASKit, Agent, AgentContext, AgentData, AgentError, AgentOutput, AgentSpec, AgentValue, AsAgent,
    askit_agent, async_trait,
};
use arrow_array::RecordBatchIterator;
use arrow_schema::Schema;
use futures_util::TryStreamExt;
use im::hashmap;
use lancedb::database::CreateTableMode;
use lancedb::index::Index;
use lancedb::query::{ExecutableQuery, QueryBase, Select};

use crate::db::{
    DbSchema, agent_value_to_record_batch, get_db_connection, record_batches_to_agent_value,
};

static CATEGORY: &str = "DB/LanceDB";

static PIN_VALUE: &str = "value";
static PIN_TABLE: &str = "table";
static PIN_UNIT: &str = "unit";

static CONFIG_DB: &str = "db";
static CONFIG_COLUMN: &str = "column";
static CONFIG_SCHEMA: &str = "schema";
static CONFIG_TABLE: &str = "table";

#[askit_agent(
    title = "Create Table",
    category = CATEGORY,
    inputs = [PIN_VALUE],
    outputs = [PIN_UNIT],
    string_config(name = CONFIG_DB),
    string_config(name = CONFIG_TABLE),
    object_config(name = CONFIG_SCHEMA),
    boolean_config(name = "overwrite"),
)]
struct LanceDbCreateTableAgent {
    data: AgentData,
}

#[async_trait]
impl AsAgent for LanceDbCreateTableAgent {
    fn new(askit: ASKit, id: String, spec: AgentSpec) -> Result<Self, AgentError> {
        Ok(Self {
            data: AgentData::new(askit, id, spec),
        })
    }

    async fn process(
        &mut self,
        ctx: AgentContext,
        _pin: String,
        value: AgentValue,
    ) -> Result<(), AgentError> {
        let config = self.configs()?;
        let db_uri = config.get_string_or_default(CONFIG_DB);
        // if db_uri.is_empty() {
        //     if value.is_object() {
        //         if let Some(db) = value.get_str(CONFIG_DB) {
        //             db_uri = db.to_string();
        //         }
        //     }
        // }
        if db_uri.is_empty() {
            return Err(AgentError::InvalidValue(
                "Database uri is required".to_string(),
            ));
        }

        let table_name = config.get_string(CONFIG_TABLE)?;
        // if table_name.is_empty() {
        //     if value.is_object() {
        //         if let Some(name) = value.get_str(CONFIG_TABLE) {
        //             table_name = name.to_string();
        //         }
        //     }
        // }
        if table_name.is_empty() {
            return Err(AgentError::InvalidValue(
                "Table name is required".to_string(),
            ));
        }

        let mut schema = config.get_object_or_default(CONFIG_SCHEMA);
        if schema.is_empty() {
            if value.is_object() {
                if let Some(s) = value.get_object(CONFIG_SCHEMA) {
                    schema = s.clone();
                }
            }
        }
        if schema.is_empty() {
            return Err(AgentError::InvalidValue("Schema is required".to_string()));
        }
        let schema_value = serde_json::to_value(&schema)
            .map_err(|e| AgentError::InvalidValue(format!("Schema serialization error: {}", e)))?;
        let schema: DbSchema = serde_json::from_value(schema_value).map_err(|e| {
            AgentError::InvalidValue(format!("Schema deserialization error: {}", e))
        })?;
        let schema: Schema = schema.into();

        let overwrite = config.get_bool_or_default("overwrite");

        let db = get_db_connection(&db_uri).await?;

        db.create_empty_table(&table_name, Arc::new(schema))
            .mode(if overwrite {
                CreateTableMode::Overwrite
            } else {
                CreateTableMode::Create
            })
            .execute()
            .await
            .map_err(|e| AgentError::IoError(format!("LanceDB Create Table Error: {}", e)))?;

        self.output(ctx, PIN_UNIT, AgentValue::unit()).await
    }
}

#[askit_agent(
    title = "Create Index",
    category = CATEGORY,
    inputs = [PIN_VALUE],
    outputs = [PIN_UNIT],
    string_config(name = CONFIG_DB),
    string_config(name = CONFIG_TABLE),
    string_config(name = CONFIG_COLUMN),
)]
struct LanceDbCreateIndexAgent {
    data: AgentData,
}

#[async_trait]
impl AsAgent for LanceDbCreateIndexAgent {
    fn new(askit: ASKit, id: String, spec: AgentSpec) -> Result<Self, AgentError> {
        Ok(Self {
            data: AgentData::new(askit, id, spec),
        })
    }

    async fn process(
        &mut self,
        ctx: AgentContext,
        _pin: String,
        value: AgentValue,
    ) -> Result<(), AgentError> {
        let config = self.configs()?;
        let db_uri = config.get_string_or_default(CONFIG_DB);
        // if db_uri.is_empty() {
        //     if value.is_object() {
        //         if let Some(db) = value.get_str(CONFIG_DB) {
        //             db_uri = db.to_string();
        //         }
        //     }
        // }
        if db_uri.is_empty() {
            return Err(AgentError::InvalidValue(
                "Database uri is required".to_string(),
            ));
        }

        let table_name = config.get_string(CONFIG_TABLE)?;
        // if table_name.is_empty() {
        //     if value.is_object() {
        //         if let Some(name) = value.get_str(CONFIG_TABLE) {
        //             table_name = name.to_string();
        //         }
        //     }
        // }
        if table_name.is_empty() {
            return Err(AgentError::InvalidValue(
                "Table name is required".to_string(),
            ));
        }

        let mut columns = config
            .get_string_or_default(CONFIG_COLUMN)
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<_>>();
        if columns.is_empty() {
            if let Ok(arr) = config.get_array(CONFIG_COLUMN) {
                columns = arr
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect::<Vec<_>>();
            }
        }
        if columns.is_empty() {
            if value.is_object() {
                if let Some(s) = value.get_str(CONFIG_COLUMN) {
                    columns = s
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .collect::<Vec<_>>();
                } else if let Some(arr) = value.get_array(CONFIG_COLUMN) {
                    columns = arr
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect::<Vec<_>>();
                }
            }
        }
        if columns.is_empty() {
            return Err(AgentError::InvalidValue("Column is required".to_string()));
        }

        let db = get_db_connection(&db_uri).await?;

        let table = db
            .open_table(&table_name)
            .execute()
            .await
            .map_err(|e| AgentError::IoError(format!("LanceDB Get Table Error: {}", e)))?;

        table
            .create_index(&columns, Index::Auto)
            .execute()
            .await
            .map_err(|e| AgentError::IoError(format!("LanceDB Create Index Error: {}", e)))?;

        self.output(ctx, PIN_UNIT, AgentValue::unit()).await
    }
}

#[askit_agent(
    title = "Drop Table",
    category = CATEGORY,
    inputs = [PIN_VALUE],
    outputs = [PIN_UNIT],
    string_config(name = CONFIG_DB),
    string_config(name = CONFIG_TABLE),
)]
struct LanceDbDropTableAgent {
    data: AgentData,
}

#[async_trait]
impl AsAgent for LanceDbDropTableAgent {
    fn new(askit: ASKit, id: String, spec: AgentSpec) -> Result<Self, AgentError> {
        Ok(Self {
            data: AgentData::new(askit, id, spec),
        })
    }

    async fn process(
        &mut self,
        ctx: AgentContext,
        _pin: String,
        value: AgentValue,
    ) -> Result<(), AgentError> {
        let config = self.configs()?;
        let mut db_uri = config.get_string_or_default(CONFIG_DB);
        if db_uri.is_empty() {
            if value.is_object() {
                if let Some(db) = value.get_str(CONFIG_DB) {
                    db_uri = db.to_string();
                }
            }
        }
        if db_uri.is_empty() {
            return Err(AgentError::InvalidValue(
                "Database uri is required".to_string(),
            ));
        }

        let mut table_name = config.get_string(CONFIG_TABLE)?;
        if table_name.is_empty() {
            if value.is_object() {
                if let Some(name) = value.get_str(CONFIG_TABLE) {
                    table_name = name.to_string();
                }
            }
        }
        if table_name.is_empty() {
            return Err(AgentError::InvalidValue(
                "Table name is required".to_string(),
            ));
        }

        let db = get_db_connection(&db_uri).await?;

        db.drop_table(&table_name, &[])
            .await
            .map_err(|e| AgentError::IoError(format!("LanceDB Drop Table Error: {}", e)))?;

        self.output(ctx, PIN_UNIT, AgentValue::unit()).await
    }
}

#[askit_agent(
    title = "Add Records",
    category = CATEGORY,
    inputs = [PIN_VALUE],
    outputs = [PIN_UNIT],
    string_config(name = CONFIG_DB),
    string_config(name = CONFIG_TABLE),
)]
struct LanceDbAddRecordsAgent {
    data: AgentData,
}

#[async_trait]
impl AsAgent for LanceDbAddRecordsAgent {
    fn new(askit: ASKit, id: String, spec: AgentSpec) -> Result<Self, AgentError> {
        Ok(Self {
            data: AgentData::new(askit, id, spec),
        })
    }

    async fn process(
        &mut self,
        ctx: AgentContext,
        _pin: String,
        value: AgentValue,
    ) -> Result<(), AgentError> {
        let config = self.configs()?;
        let db_uri = config.get_string_or_default(CONFIG_DB);
        // if db_uri.is_empty() {
        //     if value.is_object() {
        //         if let Some(db) = value.get_str(CONFIG_DB) {
        //             db_uri = db.to_string();
        //         }
        //     }
        // }
        if db_uri.is_empty() {
            return Err(AgentError::InvalidValue(
                "Database uri is required".to_string(),
            ));
        }

        let table_name = config.get_string(CONFIG_TABLE)?;
        // if table_name.is_empty() {
        //     if value.is_object() {
        //         if let Some(name) = value.get_str(CONFIG_TABLE) {
        //             table_name = name.to_string();
        //         }
        //     }
        // }
        if table_name.is_empty() {
            return Err(AgentError::InvalidValue(
                "Table name is required".to_string(),
            ));
        }

        let db = get_db_connection(&db_uri).await?;
        let table = db
            .open_table(&table_name)
            .execute()
            .await
            .map_err(|e| AgentError::IoError(format!("LanceDB Get Table Error: {}", e)))?;
        let schema = table
            .schema()
            .await
            .map_err(|e| AgentError::IoError(format!("LanceDB Get Table Schema Error: {}", e)))?;
        let batch = agent_value_to_record_batch(schema.clone(), value)?;
        table
            .add(RecordBatchIterator::new(
                vec![Ok(batch)].into_iter(),
                schema.clone(),
            ))
            .execute()
            .await
            .map_err(|e| AgentError::IoError(format!("LanceDB Add Records Error: {}", e)))?;

        self.output(ctx, PIN_UNIT, AgentValue::unit()).await
    }
}

#[askit_agent(
    title = "Optimize",
    category = CATEGORY,
    inputs = [PIN_UNIT],
    outputs = [PIN_VALUE],
    string_config(name = CONFIG_DB),
    string_config(name = CONFIG_TABLE),
)]
struct LanceDbOptimizeAgent {
    data: AgentData,
}

#[async_trait]
impl AsAgent for LanceDbOptimizeAgent {
    fn new(askit: ASKit, id: String, spec: AgentSpec) -> Result<Self, AgentError> {
        Ok(Self {
            data: AgentData::new(askit, id, spec),
        })
    }

    async fn process(
        &mut self,
        ctx: AgentContext,
        _pin: String,
        _value: AgentValue,
    ) -> Result<(), AgentError> {
        let config = self.configs()?;
        let db_uri = config.get_string_or_default(CONFIG_DB);
        if db_uri.is_empty() {
            return Err(AgentError::InvalidValue(
                "Database uri is required".to_string(),
            ));
        }

        let table_name = config.get_string(CONFIG_TABLE)?;
        if table_name.is_empty() {
            return Err(AgentError::InvalidValue(
                "Table name is required".to_string(),
            ));
        }

        let db = get_db_connection(&db_uri).await?;

        let table = db
            .open_table(&table_name)
            .execute()
            .await
            .map_err(|e| AgentError::IoError(format!("LanceDB Get Table Error: {}", e)))?;

        let stats = table
            .optimize(lancedb::table::OptimizeAction::All)
            .await
            .map_err(|e| AgentError::IoError(format!("LanceDB Optimize Table Error: {}", e)))?;

        let output = AgentValue::object(hashmap! {
            "compaction".to_string() => AgentValue::string(format!("{:?}", stats.compaction)),
            "prune".to_string() => AgentValue::string(format!("{:?}", stats.prune)),
        });
        self.output(ctx, PIN_VALUE, output).await
    }
}

#[askit_agent(
    title = "Query",
    category = CATEGORY,
    inputs = [PIN_VALUE],
    outputs = [PIN_TABLE],
    string_config(name = CONFIG_DB),
    string_config(name = CONFIG_TABLE),
)]
struct LanceDbQueryAgent {
    data: AgentData,
}

#[async_trait]
impl AsAgent for LanceDbQueryAgent {
    fn new(askit: ASKit, id: String, spec: AgentSpec) -> Result<Self, AgentError> {
        Ok(Self {
            data: AgentData::new(askit, id, spec),
        })
    }

    async fn process(
        &mut self,
        ctx: AgentContext,
        _pin: String,
        value: AgentValue,
    ) -> Result<(), AgentError> {
        let config = self.configs()?;

        let db_uri = config.get_string_or_default(CONFIG_DB);
        // if db_uri.is_empty() {
        //     if value.is_object() {
        //         if let Some(db) = value.get_str(CONFIG_DB) {
        //             db_uri = db.to_string();
        //         }
        //     }
        // }
        if db_uri.is_empty() {
            return Err(AgentError::InvalidValue(
                "Database uri is required".to_string(),
            ));
        }

        let table_name = config.get_string(CONFIG_TABLE)?;
        // if table_name.is_empty() {
        //     if value.is_object() {
        //         if let Some(name) = value.get_str(CONFIG_TABLE) {
        //             table_name = name.to_string();
        //         }
        //     }
        // }
        if table_name.is_empty() {
            return Err(AgentError::InvalidValue(
                "Table name is required".to_string(),
            ));
        }

        let db = get_db_connection(&db_uri).await?;
        let table = db
            .open_table(&table_name)
            .execute()
            .await
            .map_err(|e| AgentError::IoError(format!("LanceDB Get Table Error: {}", e)))?;

        let mut query = table.query();

        if let Some(select) = value.get_array("select") {
            let select_cols = select
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>();
            if !select_cols.is_empty() {
                query = query.select(Select::Columns(select_cols));
            }
        }

        if let Some(only_if) = value.get_str("only_if") {
            query = query.only_if(only_if.to_string());
        }

        if let Some(limit) = value.get_i64("limit") {
            query = query.limit(limit as usize);
        }

        let result = query
            .execute()
            .await
            .map_err(|e| AgentError::IoError(format!("LanceDB Query Table Records Error: {}", e)))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| {
                AgentError::IoError(format!("LanceDB Collect Query Results Error: {}", e))
            })?;

        let value = record_batches_to_agent_value(result)?;

        self.output(ctx, PIN_TABLE, value).await
    }
}

#[askit_agent(
    title = "Vector Search",
    category = CATEGORY,
    inputs = [PIN_VALUE],
    outputs = [PIN_TABLE],
    string_config(name = CONFIG_DB),
    string_config(name = CONFIG_TABLE),
)]
struct LanceDbVectorSearchAgent {
    data: AgentData,
}

#[async_trait]
impl AsAgent for LanceDbVectorSearchAgent {
    fn new(askit: ASKit, id: String, spec: AgentSpec) -> Result<Self, AgentError> {
        Ok(Self {
            data: AgentData::new(askit, id, spec),
        })
    }

    async fn process(
        &mut self,
        ctx: AgentContext,
        _pin: String,
        value: AgentValue,
    ) -> Result<(), AgentError> {
        let config = self.configs()?;
        let db_uri = config.get_string_or_default(CONFIG_DB);
        if db_uri.is_empty() {
            return Err(AgentError::InvalidValue(
                "Database uri is required".to_string(),
            ));
        }

        let table_name = config.get_string(CONFIG_TABLE)?;
        if table_name.is_empty() {
            return Err(AgentError::InvalidValue(
                "Table name is required".to_string(),
            ));
        }

        let query_vector: &[f32] = {
            let Some(v) = value.as_tensor() else {
                return Err(AgentError::InvalidValue("tensor is required".to_string()));
            };
            v.as_slice()
        };

        let db = get_db_connection(&db_uri).await?;

        let table = db
            .open_table(&table_name)
            .execute()
            .await
            .map_err(|e| AgentError::IoError(format!("LanceDB Get Table Error: {}", e)))?;

        let mut results = table
            .vector_search(query_vector)
            .map_err(|e| AgentError::IoError(format!("LanceDB Vector Search Error: {}", e)))?
            .distance_type(lancedb::DistanceType::L2)
            .execute()
            .await
            .map_err(|e| AgentError::IoError(format!("LanceDB Execute Error: {}", e)))?;

        let mut batches = Vec::new();
        while let Some(batch) = results.try_next().await.map_err(|e| {
            AgentError::IoError(format!(
                "LanceDB Collect Vector Search Results Error: {}",
                e
            ))
        })? {
            batches.push(batch);
        }

        let value = record_batches_to_agent_value(batches)?;
        self.output(ctx, PIN_TABLE, value).await
    }
}
