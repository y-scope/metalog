// Package upstreampb provides the stable, public protobuf type surface for
// downstream repositories that depend on metalog.
//
// Rather than importing generated code paths directly (gen/proto/…), downstream
// consumers should import this package. It re-exports the subset of types
// needed to build [kafka.MessageTransformer] implementations and interact with
// the ingestion API.
//
// All type aliases here are binary-compatible with the underlying generated
// types — no conversion is needed when passing values across package boundaries.
package upstreampb
