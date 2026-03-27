package metastore

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestMetadataReader_ListTables(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup
	r := NewMetadataReader(db)

	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"table_name"}).AddRow("logs").AddRow("metrics"))

	tables, err := r.ListTables(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 2 || tables[0] != "logs" {
		t.Errorf("got %v", tables)
	}
}

func TestMetadataReader_ListDimensions(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup
	r := NewMetadataReader(db)

	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "dim_key", "base_type", "width", "alias_column"}).
			AddRow("dim_f01", "region", "VARCHAR", 64, ""))

	dims, err := r.ListDimensions(context.Background(), "logs")
	if err != nil {
		t.Fatal(err)
	}
	if len(dims) != 1 || dims[0].Name != "region" {
		t.Errorf("got %v", dims)
	}
}

func TestMetadataReader_ListAggs(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup
	r := NewMetadataReader(db)

	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "agg_key", "agg_value", "aggregation_type", "value_type", "alias_column"}).
			AddRow("agg_f01", "cpu", "", "SUM", "FLOAT", ""))

	aggs, err := r.ListAggs(context.Background(), "logs")
	if err != nil {
		t.Fatal(err)
	}
	if len(aggs) != 1 || aggs[0].Name != "cpu" {
		t.Errorf("got %v", aggs)
	}
}

func TestMetadataReader_ListSketches(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup
	r := NewMetadataReader(db)

	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"sketch_name"}).AddRow("s01").AddRow("s02"))

	sketches, err := r.ListSketches(context.Background(), "logs")
	if err != nil {
		t.Fatal(err)
	}
	if len(sketches) != 2 {
		t.Errorf("got %d sketches", len(sketches))
	}
}
