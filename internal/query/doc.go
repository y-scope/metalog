// Package query implements the split-file query engine for CLP metadata.
//
// The [SplitQueryEngine] translates user queries (with SQL WHERE filters,
// ordering, and keyset pagination) into optimized queries against the physical
// schema. It resolves user-facing column names to dim_fNN/agg_fNN slots via
// the column registry, validates filter expressions using Vitess SQL parsing,
// and supports cursor-based pagination for large result sets.
//
// Additional facilities include an LRU [Cache] for resolved column mappings
// and a [SketchEvaluator] for approximate query planning using HyperLogLog
// sketches.
package query
