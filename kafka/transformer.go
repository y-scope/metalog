package kafka

import (
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/y-scope/metalog/coordinator/ingestion"
	pb "github.com/y-scope/metalog/gen/proto/ingestionpb"
	"github.com/y-scope/metalog/metastore"
)

var (
	transformerMu       sync.RWMutex
	transformerRegistry = map[string]func() MessageTransformer{}
)

// RegisterTransformer registers a named message transformer factory.
func RegisterTransformer(name string, factory func() MessageTransformer) {
	transformerMu.Lock()
	defer transformerMu.Unlock()
	transformerRegistry[name] = factory
}

// NewTransformer creates a transformer by name. Returns an error for unregistered names.
func NewTransformer(name string) (MessageTransformer, error) {
	transformerMu.RLock()
	factory, ok := transformerRegistry[name]
	transformerMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown message transformer %q", name)
	}
	return factory(), nil
}

func init() {
	RegisterTransformer("", func() MessageTransformer { return &AutoDetectTransformer{} })
	RegisterTransformer("auto", func() MessageTransformer { return &AutoDetectTransformer{} })
	RegisterTransformer("proto", func() MessageTransformer { return &ProtoTransformer{} })
}

// MessageTransformer transforms raw Kafka message bytes into a FileRecord
// ready for ingestion. Implementations handle deserialization (proto, JSON,
// platform-specific formats) and validation internally.
type MessageTransformer interface {
	Transform(payload []byte) (*metastore.FileRecord, error)
}

// ProtoTransformer deserializes protobuf MetadataRecord payloads.
type ProtoTransformer struct{}

func (t *ProtoTransformer) Transform(payload []byte) (*metastore.FileRecord, error) {
	var record pb.MetadataRecord
	if err := proto.Unmarshal(payload, &record); err != nil {
		return nil, fmt.Errorf("protobuf unmarshal: %w", err)
	}
	return ingestion.ConvertRecord(&record)
}

// AutoDetectTransformer auto-detects JSON vs protobuf payloads.
// JSON payloads start with '{'; everything else is treated as protobuf.
type AutoDetectTransformer struct{}

func (t *AutoDetectTransformer) Transform(payload []byte) (*metastore.FileRecord, error) {
	var record *pb.MetadataRecord
	var err error
	if len(payload) > 0 && payload[0] == '{' {
		record, err = unmarshalJSONToProto(payload)
	} else {
		var r pb.MetadataRecord
		err = proto.Unmarshal(payload, &r)
		record = &r
	}
	if err != nil {
		return nil, err
	}
	return ingestion.ConvertRecord(record)
}
