package grpcserver

import (
	"context"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"google.golang.org/grpc/codes"

	pb "github.com/y-scope/metalog/gen/proto/coordinatorpb"
)

func TestRegisterTable_DBError(t *testing.T) {
	h, mock := newTestAdminHandler(t)
	mock.ExpectQuery("SELECT").WillReturnError(fmt.Errorf("db connection failed"))

	_, err := h.RegisterTable(context.Background(), &pb.RegisterTableRequest{
		TableName: "test_table",
	})
	assertGRPCCode(t, err, codes.Internal)
}

func TestRegisterKafkaSource_ListError(t *testing.T) {
	h, mock := newTestAdminHandler(t)

	// ListSources fails
	mock.ExpectQuery("SELECT").WillReturnError(fmt.Errorf("db error"))

	req := &pb.RegisterKafkaSourceRequest{
		TableName:        "test_table",
		SourceName:       "src1",
		Topic:            "test-topic",
		BootstrapServers: "broker:9092",
		ConsumerGroupId:  "group-1",
	}
	_, err := h.RegisterKafkaSource(context.Background(), req)
	assertGRPCCode(t, err, codes.Internal)
}

func TestRegisterKafkaSource_DuplicateConsumerGroup(t *testing.T) {
	h, mock := newTestAdminHandler(t)

	// ListSources returns an existing source with the same consumer_group_id
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"table_name", "source_name", "topic", "bootstrap_servers", "record_transformer", "consumer_group_id", "required_env"}).
			AddRow("test_table", "existing_src", "topic1", "broker:9092", "proto", "group-1", ""))

	req := &pb.RegisterKafkaSourceRequest{
		TableName:        "test_table",
		SourceName:       "new_src",
		Topic:            "test-topic",
		BootstrapServers: "broker:9092",
		ConsumerGroupId:  "group-1", // same as existing
	}
	_, err := h.RegisterKafkaSource(context.Background(), req)
	assertGRPCCode(t, err, codes.AlreadyExists)
}

func TestRegisterKafkaSource_Success(t *testing.T) {
	h, mock := newTestAdminHandler(t)

	// ListSources returns empty
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"table_name", "source_name", "topic", "bootstrap_servers", "record_transformer", "consumer_group_id", "required_env"}))

	// Register uses a transaction with source INSERT + seed assignment INSERT
	mock.ExpectBegin()
	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	req := &pb.RegisterKafkaSourceRequest{
		TableName:        "test_table",
		SourceName:       "src1",
		Topic:            "test-topic",
		BootstrapServers: "broker:9092",
		ConsumerGroupId:  "group-1",
	}
	resp, err := h.RegisterKafkaSource(context.Background(), req)
	if err != nil {
		t.Fatalf("RegisterKafkaSource error: %v", err)
	}
	if resp.TableName != "test_table" {
		t.Errorf("TableName = %q", resp.TableName)
	}
}

func TestDeleteKafkaSource_Success(t *testing.T) {
	h, mock := newTestAdminHandler(t)
	mock.ExpectExec("DELETE").WillReturnResult(sqlmock.NewResult(0, 1))

	resp, err := h.DeleteKafkaSource(context.Background(), &pb.DeleteKafkaSourceRequest{
		TableName:  "test_table",
		SourceName: "src1",
	})
	if err != nil {
		t.Fatalf("DeleteKafkaSource error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestDeleteKafkaSource_DBError(t *testing.T) {
	h, mock := newTestAdminHandler(t)
	mock.ExpectExec("DELETE").WillReturnError(fmt.Errorf("db error"))

	_, err := h.DeleteKafkaSource(context.Background(), &pb.DeleteKafkaSourceRequest{
		TableName:  "test_table",
		SourceName: "src1",
	})
	assertGRPCCode(t, err, codes.Internal)
}

func TestRegisterKafkaSource_RegisterError(t *testing.T) {
	h, mock := newTestAdminHandler(t)

	// ListSources returns empty
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"table_name", "source_name", "topic", "bootstrap_servers", "record_transformer", "consumer_group_id", "required_env"}))

	// Register uses a transaction, fails on INSERT
	mock.ExpectBegin()
	mock.ExpectExec("INSERT").WillReturnError(fmt.Errorf("db error"))
	mock.ExpectRollback()

	req := &pb.RegisterKafkaSourceRequest{
		TableName:        "test_table",
		SourceName:       "src1",
		Topic:            "test-topic",
		BootstrapServers: "broker:9092",
		ConsumerGroupId:  "group-1",
	}
	_, err := h.RegisterKafkaSource(context.Background(), req)
	assertGRPCCode(t, err, codes.Internal)
}

func TestRegisterKafkaSource_DefaultTransformer(t *testing.T) {
	h, mock := newTestAdminHandler(t)

	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"table_name", "source_name", "topic", "bootstrap_servers", "record_transformer", "consumer_group_id", "required_env"}))
	mock.ExpectBegin()
	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1)) // seed assignment
	mock.ExpectCommit()

	req := &pb.RegisterKafkaSourceRequest{
		TableName:          "test_table",
		SourceName:         "src1",
		Topic:              "test-topic",
		BootstrapServers:   "broker:9092",
		ConsumerGroupId:    "group-1",
		RecordTransformer:  "", // empty -> defaults to "proto"
	}
	_, err := h.RegisterKafkaSource(context.Background(), req)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
}
