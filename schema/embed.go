package schema

import _ "embed"

// SchemaSQL contains the full DDL for system tables (_table, _table_config,
// _table_assignment, _node_registry, _dim_registry, _agg_registry,
// _sketch_registry, _task_queue) and the _clp_template table.
//
//go:embed schema.sql
var SchemaSQL string
