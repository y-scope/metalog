package grpc

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/y-scope/metalog/gen/proto/coordinatorpb"
)

func newTestAdminHandler(t *testing.T) (*AdminHandler, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return &AdminHandler{db: db, log: zap.NewNop()}, mock
}

// --- RegisterTable validation tests ---

func TestRegisterTable_EmptyTableName(t *testing.T) {
	h := &AdminHandler{}
	_, err := h.RegisterTable(context.Background(), &pb.RegisterTableRequest{})

	assertGRPCCode(t, err, codes.InvalidArgument)
	assertContains(t, status.Convert(err).Message(), "table_name is required")
}

// --- SetColumnAlias validation tests ---

func TestSetColumnAlias_EmptyTableName(t *testing.T) {
	h := &AdminHandler{}
	req := &pb.SetColumnAliasRequest{ColumnName: "dim_f01"}
	_, err := h.SetColumnAlias(context.Background(), req)

	assertGRPCCode(t, err, codes.InvalidArgument)
	assertContains(t, status.Convert(err).Message(), "table_name is required")
}

func TestSetColumnAlias_EmptyColumnName(t *testing.T) {
	h := &AdminHandler{}
	req := &pb.SetColumnAliasRequest{TableName: "test_table"}
	_, err := h.SetColumnAlias(context.Background(), req)

	assertGRPCCode(t, err, codes.InvalidArgument)
	assertContains(t, status.Convert(err).Message(), "column_name is required")
}

func TestSetColumnAlias_InvalidColumnPrefix(t *testing.T) {
	h := &AdminHandler{}
	req := &pb.SetColumnAliasRequest{
		TableName:  "test_table",
		ColumnName: "some_random_col",
	}
	_, err := h.SetColumnAlias(context.Background(), req)

	assertGRPCCode(t, err, codes.InvalidArgument)
	assertContains(t, status.Convert(err).Message(), "must start with")
}

func TestSetColumnAlias_AliasTooLong(t *testing.T) {
	h := &AdminHandler{}
	longAlias := "a"
	for len(longAlias) <= maxAliasLength {
		longAlias += "abcdefghij"
	}
	req := &pb.SetColumnAliasRequest{
		TableName:   "test_table",
		ColumnName:  "dim_f01",
		AliasColumn: longAlias,
	}
	_, err := h.SetColumnAlias(context.Background(), req)

	assertGRPCCode(t, err, codes.InvalidArgument)
	assertContains(t, status.Convert(err).Message(), "exceeds max length")
}

func TestSetColumnAlias_InvalidAliasPattern(t *testing.T) {
	invalid := []string{
		"123starts_with_digit",
		"has space",
		"has@special",
		"has!bang",
		"has#hash",
	}
	for _, alias := range invalid {
		t.Run(alias, func(t *testing.T) {
			h := &AdminHandler{}
			req := &pb.SetColumnAliasRequest{
				TableName:   "test_table",
				ColumnName:  "dim_f01",
				AliasColumn: alias,
			}
			_, err := h.SetColumnAlias(context.Background(), req)
			assertGRPCCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestSetColumnAlias_ValidAliasPatterns(t *testing.T) {
	valid := []string{
		"region",
		"service_name",
		"k8s.pod.name",
		"my-alias",
		"path/to/thing",
		"_private",
		"CamelCase",
	}
	for _, alias := range valid {
		t.Run(alias, func(t *testing.T) {
			h, mock := newTestAdminHandler(t)
			mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
			req := &pb.SetColumnAliasRequest{
				TableName:   "test_table",
				ColumnName:  "dim_f01",
				AliasColumn: alias,
			}
			_, err := h.SetColumnAlias(context.Background(), req)
			if err != nil {
				t.Errorf("SetColumnAlias(alias=%q) unexpected error: %v", alias, err)
			}
		})
	}
}

func TestSetColumnAlias_EmptyAliasClearsAlias(t *testing.T) {
	h, mock := newTestAdminHandler(t)
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))

	req := &pb.SetColumnAliasRequest{
		TableName:  "test_table",
		ColumnName: "agg_f01",
	}
	_, err := h.SetColumnAlias(context.Background(), req)

	if err != nil {
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.InvalidArgument {
			t.Errorf("empty alias rejected as InvalidArgument: %s", st.Message())
		}
	}
}

func TestSetColumnAlias_AggPrefix(t *testing.T) {
	h, mock := newTestAdminHandler(t)
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))

	req := &pb.SetColumnAliasRequest{
		TableName:  "test_table",
		ColumnName: "agg_f05",
	}
	_, err := h.SetColumnAlias(context.Background(), req)

	if err != nil {
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.InvalidArgument {
			t.Errorf("agg_f prefix rejected as InvalidArgument: %s", st.Message())
		}
	}
}

// --- InvalidateColumn validation tests ---

func TestInvalidateColumn_EmptyTableName(t *testing.T) {
	h := &AdminHandler{}
	req := &pb.InvalidateColumnRequest{ColumnName: "dim_f01"}
	_, err := h.InvalidateColumn(context.Background(), req)

	assertGRPCCode(t, err, codes.InvalidArgument)
	assertContains(t, status.Convert(err).Message(), "table_name is required")
}

func TestInvalidateColumn_EmptyColumnName(t *testing.T) {
	h := &AdminHandler{}
	req := &pb.InvalidateColumnRequest{TableName: "test_table"}
	_, err := h.InvalidateColumn(context.Background(), req)

	assertGRPCCode(t, err, codes.InvalidArgument)
	assertContains(t, status.Convert(err).Message(), "column_name is required")
}

func TestInvalidateColumn_InvalidPrefix(t *testing.T) {
	h := &AdminHandler{}
	req := &pb.InvalidateColumnRequest{
		TableName:  "test_table",
		ColumnName: "some_random_col",
	}
	_, err := h.InvalidateColumn(context.Background(), req)

	assertGRPCCode(t, err, codes.InvalidArgument)
	assertContains(t, status.Convert(err).Message(), "must start with")
}

func TestInvalidateColumn_NotFound(t *testing.T) {
	h, mock := newTestAdminHandler(t)
	// SELECT returns no rows
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"dim_key"}))
	req := &pb.InvalidateColumnRequest{
		TableName:  "test_table",
		ColumnName: "dim_f01",
	}
	_, err := h.InvalidateColumn(context.Background(), req)

	assertGRPCCode(t, err, codes.NotFound)
}

func TestInvalidateColumn_Success_Dim(t *testing.T) {
	h, mock := newTestAdminHandler(t)
	// SELECT returns the current key
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"dim_key"}).AddRow("k8s.pod.name"))
	// UPDATE sets state=INVALIDATED
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))

	req := &pb.InvalidateColumnRequest{
		TableName:  "test_table",
		ColumnName: "dim_f03",
	}
	resp, err := h.InvalidateColumn(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ColumnName != "dim_f03" {
		t.Errorf("expected column_name=dim_f03, got %s", resp.ColumnName)
	}
	if resp.PreviousKey != "k8s.pod.name" {
		t.Errorf("expected previous_key=k8s.pod.name, got %s", resp.PreviousKey)
	}
}

func TestInvalidateColumn_Success_Agg(t *testing.T) {
	h, mock := newTestAdminHandler(t)
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"agg_key"}).AddRow("level"))
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))

	req := &pb.InvalidateColumnRequest{
		TableName:  "test_table",
		ColumnName: "agg_f02",
	}
	resp, err := h.InvalidateColumn(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ColumnName != "agg_f02" {
		t.Errorf("expected column_name=agg_f02, got %s", resp.ColumnName)
	}
	if resp.PreviousKey != "level" {
		t.Errorf("expected previous_key=level, got %s", resp.PreviousKey)
	}
}

// --- test helpers ---

func assertGRPCCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %s, got nil", code)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T: %v", err, err)
	}
	if st.Code() != code {
		t.Errorf("expected code %s, got %s: %s", code, st.Code(), st.Message())
	}
}

func assertContains(t *testing.T, s, substr string) {
	t.Helper()
	if !strings.Contains(s, substr) {
		t.Errorf("expected %q to contain %q", s, substr)
	}
}
