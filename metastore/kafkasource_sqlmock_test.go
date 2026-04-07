package metastore

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"go.uber.org/zap"
)

func TestKafkaSourceStore_Register(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup
	store := NewKafkaSourceStore(db, zap.NewNop())

	mock.ExpectBegin()
	mock.ExpectExec("INSERT IGNORE INTO _kafka_source").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT IGNORE INTO _kafka_assignment").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	created, err := store.Register(context.Background(), &KafkaSource{
		TableName: "t1", SourceName: "s1", Topic: "topic1",
		BootstrapServers: "localhost:9092",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Error("expected created=true")
	}
}

func TestKafkaSourceStore_Delete(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup
	store := NewKafkaSourceStore(db, zap.NewNop())

	mock.ExpectExec("DELETE FROM _kafka_source").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := store.Delete(context.Background(), "t1", "s1"); err != nil {
		t.Fatal(err)
	}
}

func TestKafkaSourceStore_ListSources(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup
	store := NewKafkaSourceStore(db, zap.NewNop())

	rows := sqlmock.NewRows([]string{"table_name", "source_name", "topic", "bootstrap_servers", "record_transformer", "consumer_group_id", "required_env"}).
		AddRow("t1", "s1", "topic1", "localhost:9092", "proto", nil, nil)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sources, err := store.ListSources(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 || sources[0].SourceName != "s1" {
		t.Errorf("got %+v", sources)
	}
}

func TestKafkaSourceStore_ListAllSources(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup
	store := NewKafkaSourceStore(db, zap.NewNop())

	rows := sqlmock.NewRows([]string{"table_name", "source_name", "topic", "bootstrap_servers", "record_transformer", "consumer_group_id", "required_env"}).
		AddRow("t1", "s1", "topic1", "host:9092", "proto", "grp", "REGION=us")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sources, err := store.ListAllSources(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 || sources[0].ConsumerGroupID != "grp" {
		t.Errorf("got %+v", sources)
	}
}

func TestNullIfEmpty(t *testing.T) {
	if nullIfEmpty("") != nil {
		t.Error("empty should return nil")
	}
	if nullIfEmpty("value") != "value" {
		t.Error("non-empty should return value")
	}
}
