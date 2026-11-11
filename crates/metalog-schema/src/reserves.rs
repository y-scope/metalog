use metalog_types::{column::MIN_VARCHAR_WIDTH, config::ColumnReserves};

pub(crate) struct ReservedColumn {
    pub name: String,
    pub kind: String,
    pub width: Option<u16>,
    pub aggregate: bool,
    pub sql_type: String,
}

pub(crate) fn columns(config: &ColumnReserves) -> Vec<ReservedColumn> {
    let mut columns = Vec::new();
    let mut slot = 0;
    for group in &config.dimensions {
        let width = matches!(group.base_type.as_str(), "str" | "str_utf8")
            .then_some(group.width.unwrap_or(MIN_VARCHAR_WIDTH as u16));
        for _ in 0..group.count {
            slot += 1;
            columns.push(ReservedColumn {
                name: format!("dim_f{slot:02}"),
                kind: group.base_type.clone(),
                width,
                aggregate: false,
                sql_type: crate::column_registry::dim_sql_type(
                    &group.base_type,
                    i32::from(width.unwrap_or(0)),
                ),
            });
        }
    }
    slot = 0;
    for group in &config.aggregates {
        for _ in 0..group.count {
            slot += 1;
            let float = group.value_type == "float";
            columns.push(ReservedColumn {
                name: format!("agg_f{slot:02}"),
                kind: if float { "FLOAT" } else { "INT" }.into(),
                width: None,
                aggregate: true,
                sql_type: if float { "DOUBLE" } else { "BIGINT" }.into(),
            });
        }
    }
    columns
}
