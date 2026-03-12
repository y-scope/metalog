package upstreampb

import "github.com/y-scope/metalog/gen/proto/ingestionpb"

// Ingestion record types — used by MessageTransformer implementations.

type MetadataRecord = ingestionpb.MetadataRecord
type IngestRequest = ingestionpb.IngestRequest
type IngestResponse = ingestionpb.IngestResponse

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

type AggEntry = ingestionpb.AggEntry
type AggType = ingestionpb.AggType

// AggType enum values.
const (
	AggType_EQ  = ingestionpb.AggType_EQ
	AggType_GTE = ingestionpb.AggType_GTE
	AggType_GT  = ingestionpb.AggType_GT
	AggType_LTE = ingestionpb.AggType_LTE
	AggType_LT  = ingestionpb.AggType_LT
	AggType_SUM = ingestionpb.AggType_SUM
	AggType_AVG = ingestionpb.AggType_AVG
	AggType_MIN = ingestionpb.AggType_MIN
	AggType_MAX = ingestionpb.AggType_MAX
)

// Self-describing key-value escape hatch.

type SelfDescribingEntry = ingestionpb.SelfDescribingEntry
