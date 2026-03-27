package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestDatabaseType_String(t *testing.T) {
	tests := []struct {
		want string
		dt   DatabaseType
	}{
		{want: "MySQL", dt: DatabaseTypeMySQL},
		{want: "MariaDB", dt: DatabaseTypeMariaDB},
		{want: "Aurora", dt: DatabaseTypeAurora},
		{want: "Unknown", dt: DatabaseTypeUnknown},
		{want: "Unknown", dt: DatabaseType(99)},
	}
	for _, tt := range tests {
		if got := tt.dt.String(); got != tt.want {
			t.Errorf("DatabaseType(%d).String() = %q, want %q", tt.dt, got, tt.want)
		}
	}
}

func TestDetectDatabaseType(t *testing.T) {
	tests := []struct {
		version string
		want    DatabaseType
	}{
		{"10.6.12-MariaDB", DatabaseTypeMariaDB},
		{"8.0.33", DatabaseTypeMySQL},
		{"5.7.mysql_aurora.2.10.2", DatabaseTypeAurora},
		{"5.7.25-TiDB-v6.5.0", DatabaseTypeMySQL}, // TiDB falls through to MySQL
	}
	for _, tt := range tests {
		t.Run(tt.want.String(), func(t *testing.T) {
			db, mock, _ := sqlmock.New()
			defer db.Close() //nolint:errcheck // test cleanup
			mock.ExpectQuery("SELECT VERSION").
				WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow(tt.version))
			dt, version, err := DetectDatabaseType(context.Background(), db)
			if err != nil {
				t.Fatal(err)
			}
			if dt != tt.want {
				t.Errorf("got %s, want %s", dt, tt.want)
			}
			if version != tt.version {
				t.Errorf("version = %q, want %q", version, tt.version)
			}
		})
	}
}

func TestDetectDatabaseType_Error(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close() //nolint:errcheck // test cleanup
	mock.ExpectQuery("SELECT VERSION").WillReturnError(fmt.Errorf("connection refused"))
	if _, _, err := DetectDatabaseType(context.Background(), db); err == nil {
		t.Fatal("expected error")
	}
}
