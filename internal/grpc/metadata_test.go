package grpc

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metapb "github.com/y-scope/metalog/gen/proto/metadatapb"
	"github.com/y-scope/metalog/internal/metastore"
)

func newTestMetadataHandler(t *testing.T) (*MetadataHandler, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	querier := metastore.NewMetadataReader(db, zap.NewNop())
	return NewMetadataHandler(querier, zap.NewNop()), mock
}

// --- ListTables ---

func TestListTables_ReturnsTableNames(t *testing.T) {
	h, mock := newTestMetadataHandler(t)
	rows := sqlmock.NewRows([]string{"table_name"}).
		AddRow("logs").
		AddRow("metrics")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	resp, err := h.ListTables(context.Background(), &metapb.ListTablesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Tables) != 2 {
		t.Errorf("tables count = %d, want 2", len(resp.Tables))
	}
	if resp.Tables[0] != "logs" || resp.Tables[1] != "metrics" {
		t.Errorf("tables = %v, want [logs metrics]", resp.Tables)
	}
}

func TestListTables_Empty(t *testing.T) {
	h, mock := newTestMetadataHandler(t)
	rows := sqlmock.NewRows([]string{"table_name"})
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	resp, err := h.ListTables(context.Background(), &metapb.ListTablesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Tables) != 0 {
		t.Errorf("tables count = %d, want 0", len(resp.Tables))
	}
}

// --- ListDimensions ---

func TestListDimensions_EmptyTable(t *testing.T) {
	h, _ := newTestMetadataHandler(t)
	_, err := h.ListDimensions(context.Background(), &metapb.ListDimensionsRequest{})

	assertGRPCCode(t, err, codes.InvalidArgument)
	assertContains(t, status.Convert(err).Message(), "table is required")
}

func TestListDimensions_ReturnsDims(t *testing.T) {
	h, mock := newTestMetadataHandler(t)
	rows := sqlmock.NewRows([]string{"column_name", "dim_key", "base_type", "width", "alias_column"}).
		AddRow("dim_f01", "region", "STRING", 64, "").
		AddRow("dim_f02", "service.name", "STRING", 128, "svc")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	resp, err := h.ListDimensions(context.Background(), &metapb.ListDimensionsRequest{Table: "logs"})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Dimensions) != 2 {
		t.Fatalf("dims count = %d, want 2", len(resp.Dimensions))
	}
	if resp.Dimensions[0].Name != "region" {
		t.Errorf("dim[0].Name = %q, want region", resp.Dimensions[0].Name)
	}
	if resp.Dimensions[1].AliasColumn != "svc" {
		t.Errorf("dim[1].AliasColumn = %q, want svc", resp.Dimensions[1].AliasColumn)
	}
}

// --- ListAggs ---

func TestListAggs_EmptyTable(t *testing.T) {
	h, _ := newTestMetadataHandler(t)
	_, err := h.ListAggs(context.Background(), &metapb.ListAggsRequest{})

	assertGRPCCode(t, err, codes.InvalidArgument)
	assertContains(t, status.Convert(err).Message(), "table is required")
}

func TestListAggs_ReturnsAggs(t *testing.T) {
	h, mock := newTestMetadataHandler(t)
	rows := sqlmock.NewRows([]string{"column_name", "agg_key", "agg_value", "aggregation_type", "value_type", "alias_column"}).
		AddRow("agg_f01", "status_code", "", "EQ", "INT", "").
		AddRow("agg_f02", "latency", "p99", "GTE", "FLOAT", "lat_p99")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	resp, err := h.ListAggs(context.Background(), &metapb.ListAggsRequest{Table: "logs"})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Aggs) != 2 {
		t.Fatalf("aggs count = %d, want 2", len(resp.Aggs))
	}
	if resp.Aggs[0].Name != "status_code" {
		t.Errorf("agg[0].Name = %q, want status_code", resp.Aggs[0].Name)
	}
	if resp.Aggs[1].AliasColumn != "lat_p99" {
		t.Errorf("agg[1].AliasColumn = %q, want lat_p99", resp.Aggs[1].AliasColumn)
	}
}

// --- ListSketches ---

func TestListSketches_EmptyTable(t *testing.T) {
	h, _ := newTestMetadataHandler(t)
	_, err := h.ListSketches(context.Background(), &metapb.ListSketchesRequest{})

	assertGRPCCode(t, err, codes.InvalidArgument)
	assertContains(t, status.Convert(err).Message(), "table is required")
}

func TestListSketches_ReturnsSketches(t *testing.T) {
	h, mock := newTestMetadataHandler(t)
	rows := sqlmock.NewRows([]string{"sketch_name"}).
		AddRow("host").
		AddRow("path")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	resp, err := h.ListSketches(context.Background(), &metapb.ListSketchesRequest{Table: "logs"})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Sketches) != 2 {
		t.Fatalf("sketches count = %d, want 2", len(resp.Sketches))
	}
	if resp.Sketches[0].Name != "host" {
		t.Errorf("sketch[0].Name = %q, want host", resp.Sketches[0].Name)
	}
}
