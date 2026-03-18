package kafka

import (
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	pb "github.com/y-scope/metalog/gen/proto/ingestionpb"
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

// NewTransformer creates a transformer by name. Falls back to AutoDetectTransformer.
func NewTransformer(name string) MessageTransformer {
	transformerMu.RLock()
	factory, ok := transformerRegistry[name]
	transformerMu.RUnlock()
	if ok {
		return factory()
	}
	return &AutoDetectTransformer{}
}

func init() {
	RegisterTransformer("", func() MessageTransformer { return &AutoDetectTransformer{} })
	RegisterTransformer("auto", func() MessageTransformer { return &AutoDetectTransformer{} })
	RegisterTransformer("proto", func() MessageTransformer { return &ProtoTransformer{} })
}

// MessageTransformer transforms raw Kafka message bytes into a MetadataRecord.
type MessageTransformer interface {
	Transform(payload []byte) (*pb.MetadataRecord, error)
}

// ProtoTransformer deserializes protobuf MetadataRecord payloads.
type ProtoTransformer struct{}

func (t *ProtoTransformer) Transform(payload []byte) (*pb.MetadataRecord, error) {
	var record pb.MetadataRecord
	if err := proto.Unmarshal(payload, &record); err != nil {
		return nil, fmt.Errorf("protobuf unmarshal: %w", err)
	}
	return &record, nil
}

// AutoDetectTransformer auto-detects JSON vs protobuf payloads.
// JSON payloads start with '{'; everything else is treated as protobuf.
type AutoDetectTransformer struct{}

func (t *AutoDetectTransformer) Transform(payload []byte) (*pb.MetadataRecord, error) {
	if len(payload) > 0 && payload[0] == '{' {
		return unmarshalJSONToProto(payload)
	}
	var record pb.MetadataRecord
	if err := proto.Unmarshal(payload, &record); err != nil {
		return nil, fmt.Errorf("protobuf unmarshal: %w", err)
	}
	return &record, nil
}
