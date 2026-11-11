# Column reserves in Rust

New tables reserve **64 nullable columns by default**. A new dimension or aggregate
claims an existing compatible slot in its registry, without running `ALTER TABLE`
on the metadata table. Existing tables without this setting retain dynamic column
allocation; registration does not migrate them.

| Pool | Type | Count | String capacity |
|---|---|---:|---:|
| Dimensions | `str` (ASCII) | 16 | 512 characters |
| Dimensions | `str_utf8` | 8 | 512 characters |
| Dimensions | `int` (`BIGINT`) | 8 | — |
| Dimensions | `float` (`DOUBLE`) | 4 | — |
| Dimensions | `bool` | 4 | — |
| Aggregates | `int` (`BIGINT`) | 16 | — |
| Aggregates | `float` (`DOUBLE`) | 8 | — |

The existing sketch slots remain separate. The default 64 unused dimension and
aggregate columns add eight bytes to the NULL bitmap of a row in InnoDB's DYNAMIC
format. Populated values consume their normal storage. Reserves do not create
secondary indexes or replace the virtual path-hash columns.

Aggregate reserves are supported by the aggregate registry; they do not enable
aggregation ingestion, which remains outside the supported Rust server scope.

## Create a table

Omitting `column_reserves` selects the defaults when the physical table is first
created. The resolved settings are saved with that table, so a later default
change cannot reinterpret its schema.

```bash
target/release/metalog admin register-table --addr localhost:9090 --table web_logs
```

Customize counts and capacities through the existing `config_json` field of the
gRPC `RegisterTable` request or the CLI's `--config-json` option:

```bash
target/release/metalog admin register-table --addr localhost:9090 --table web_logs_custom \
  --config-json '{"column_reserves":{"dimensions":[{"type":"str","count":24,"width":512},{"type":"str_utf8","count":8,"width":1024},{"type":"int","count":8},{"type":"float","count":4},{"type":"bool","count":4}],"aggregates":[{"type":"int","count":16},{"type":"float","count":8}]}}'
```

Omitted `dimensions` or `aggregates` arrays use their respective defaults. An
explicit empty array reserves no slots in that pool. A supplied array replaces
the entire default pool. Custom string groups default to width 256 when `width`
is omitted; width is measured in characters and must be 256–16383. Multiple groups
of the same string type can reserve different capacities; allocation chooses the
smallest available column that fits. Other types do not accept a width.

Each group needs a positive count. Totals cannot exceed 99 dimensions and 99
aggregates. Configuration validation limits declared reserve capacity to 48,000
bytes, including the maximum UTF-8 byte width, to leave room for the base schema.
Database row-size limits still apply and can reject oversized configurations.

To opt out and retain dynamic allocation for a **new** table:

```bash
target/release/metalog admin register-table --addr localhost:9090 --table dynamic_logs \
  --config-json '{"column_reserves":null}'
```

The dynamic mode may rebuild MariaDB tables when allocating or widening columns.
`ALGORITHM=INPLACE` is not a guarantee against a table rebuild.

## Capacity and lifecycle

`column_reserves` is immutable after creation. Registering the same table again
can change other settings, such as `indexes`, but cannot add, remove, or resize
its reserves. Existing populated tables need an explicit migration to adopt
reserves. There is no automatic capacity-expansion migration.

Allocation is serialized across nodes using a database-scoped advisory lock and
performed on that lock's connection. Only `AVAILABLE` slots can be claimed.
Queries see only `ACTIVE` field mappings. `INVALIDATED` columns are not recycled:
old rows may still contain their previous field's values.

When no compatible slot remains, ingestion of records needing that slot fails
with a capacity error. A string that exceeds an already assigned column's
capacity also fails. Reserved tables never fall back to adding or widening
columns during ingestion. Other compatible fields can continue ingesting.
Allocation logs a warning when two or fewer compatible slots remain.

Inspect remaining slots with:

```sql
SELECT base_type, width, state, COUNT(*) AS slots
FROM _dim_registry WHERE table_name = 'web_logs'
GROUP BY base_type, width, state;

SELECT value_type, state, COUNT(*) AS slots
FROM _agg_registry WHERE table_name = 'web_logs'
GROUP BY value_type, state;
```

Configured dimension indexes are still reconciled separately in the background.
Building an index scans existing data and consumes database resources; reserving
columns does not make index creation instantaneous.

Provisioning creates the physical columns before publishing the registry slots
and coordinator assignment. Interrupted provisioning can be retried with the same
configuration without resetting claimed slots.
