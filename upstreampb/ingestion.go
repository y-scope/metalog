package upstreampb

import "github.com/y-scope/metalog/gen/proto/ingestionpb"

// Ingestion record types — used by MessageTransformer implementations.

type MetadataRecord = ingestionpb.MetadataRecord

// File-level fields.

type FileFields = ingestionpb.FileFields
type IrFileInfo = ingestionpb.IrFileInfo
type ArchiveFileInfo = ingestionpb.ArchiveFileInfo

// Dimension types.

type DimEntry = ingestionpb.DimEntry
type DimensionValue = ingestionpb.DimensionValue
type DimensionValue_Str = ingestionpb.DimensionValue_Str
type DimensionValue_StrUtf8 = ingestionpb.DimensionValue_StrUtf8
type DimensionValue_IntVal = ingestionpb.DimensionValue_IntVal
type DimensionValue_BoolVal = ingestionpb.DimensionValue_BoolVal
type DimensionValue_FloatVal = ingestionpb.DimensionValue_FloatVal
type StringDimension = ingestionpb.StringDimension

// Aggregation types.

type IngestAggEntry = ingestionpb.IngestAggEntry
type IngestAggType = ingestionpb.IngestAggType

// IngestAggType enum values.
const (
	IngestAggType_EQ  = ingestionpb.IngestAggType_EQ
	IngestAggType_GTE = ingestionpb.IngestAggType_GTE
	IngestAggType_GT  = ingestionpb.IngestAggType_GT
	IngestAggType_LTE = ingestionpb.IngestAggType_LTE
	IngestAggType_LT  = ingestionpb.IngestAggType_LT
	IngestAggType_SUM = ingestionpb.IngestAggType_SUM
	IngestAggType_AVG = ingestionpb.IngestAggType_AVG
	IngestAggType_MIN = ingestionpb.IngestAggType_MIN
	IngestAggType_MAX = ingestionpb.IngestAggType_MAX
)

// Self-describing key-value escape hatch.

type SelfDescribingEntry = ingestionpb.SelfDescribingEntry
