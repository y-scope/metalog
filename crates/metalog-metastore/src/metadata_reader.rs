use sqlx::MySqlPool;

/// Read-only queries against the column and sketch registries.
///
/// Used by the MetadataService gRPC handler for schema introspection.
pub struct MetadataReader {
    db: MySqlPool,
}

/// Dimension column metadata from `_dim_registry`.
#[derive(Debug, Clone)]
pub struct DimensionInfo {
    pub name: String,
    pub base_type: String,
    pub alias_column: Option<String>,
    pub width: Option<i32>,
}

/// Aggregation column metadata from `_agg_registry`.
#[derive(Debug, Clone)]
pub struct AggInfo {
    pub name: String,
    pub qualifier: String,
    pub aggregation_type: String,
    pub value_type: String,
    pub alias_column: Option<String>,
}

/// Sketch metadata from `_sketch_registry`.
#[derive(Debug, Clone)]
pub struct SketchInfo {
    pub name: String,
}

impl MetadataReader {
    pub fn new(db: MySqlPool) -> Self {
        Self { db }
    }

    /// Lists all table names from `_table`.
    pub async fn list_tables(&self) -> Result<Vec<String>, sqlx::Error> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT table_name FROM _table WHERE active = TRUE ORDER BY table_name")
                .fetch_all(&self.db)
                .await?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    /// Lists dimensions for a table from `_dim_registry`.
    pub async fn list_dimensions(
        &self,
        table_name: &str,
    ) -> Result<Vec<DimensionInfo>, sqlx::Error> {
        let rows: Vec<(String, String, Option<String>, Option<i32>)> = sqlx::query_as(
            "SELECT dim_key, base_type, alias_column, width FROM _dim_registry WHERE table_name = \
             ? AND state = 'ACTIVE' ORDER BY dim_key",
        )
        .bind(table_name)
        .fetch_all(&self.db)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(name, base_type, alias_column, width)| DimensionInfo {
                name,
                base_type,
                alias_column,
                width,
            })
            .collect())
    }

    /// Lists aggregations for a table from `_agg_registry`.
    pub async fn list_aggs(&self, table_name: &str) -> Result<Vec<AggInfo>, sqlx::Error> {
        let rows: Vec<(String, String, String, String, Option<String>)> = sqlx::query_as(
            "SELECT agg_key, COALESCE(agg_value, ''), aggregation_type, value_type, alias_column \
             FROM _agg_registry WHERE table_name = ? AND state = 'ACTIVE' ORDER BY agg_key",
        )
        .bind(table_name)
        .fetch_all(&self.db)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(name, qualifier, aggregation_type, value_type, alias_column)| AggInfo {
                    name,
                    qualifier,
                    aggregation_type,
                    value_type,
                    alias_column,
                },
            )
            .collect())
    }

    /// Lists sketches for a table from `_sketch_registry`.
    pub async fn list_sketches(&self, table_name: &str) -> Result<Vec<SketchInfo>, sqlx::Error> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT sketch_key FROM _sketch_registry WHERE table_name = ? AND state = 'ACTIVE' \
             AND sketch_key IS NOT NULL ORDER BY sketch_key",
        )
        .bind(table_name)
        .fetch_all(&self.db)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(name,)| SketchInfo { name })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dimension_info_fields() {
        let info = DimensionInfo {
            name: "hostname".into(),
            base_type: "str".into(),
            alias_column: Some("host".into()),
            width: Some(256),
        };
        assert_eq!(info.name, "hostname");
        assert_eq!(info.base_type, "str");
    }

    #[test]
    fn agg_info_fields() {
        let info = AggInfo {
            name: "level".into(),
            qualifier: "error".into(),
            aggregation_type: "EQ".into(),
            value_type: "INT".into(),
            alias_column: None,
        };
        assert_eq!(info.aggregation_type, "EQ");
    }
}
