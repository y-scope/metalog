package db

import (
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
)

func TestIsMySQLError(t *testing.T) {
	tests := []struct {
		err     error
		checker func(error) bool
		name    string
		want    bool
	}{
		{name: "deadlock match", err: &mysql.MySQLError{Number: 1213}, checker: IsDeadlock, want: true},
		{name: "deadlock wrapped", err: fmt.Errorf("w: %w", &mysql.MySQLError{Number: 1213}), checker: IsDeadlock, want: true},
		{name: "deadlock wrong code", err: &mysql.MySQLError{Number: 1062}, checker: IsDeadlock, want: false},
		{name: "deadlock non-mysql", err: fmt.Errorf("err"), checker: IsDeadlock, want: false},
		{name: "dup key match", err: &mysql.MySQLError{Number: 1062}, checker: IsDuplicateKey, want: true},
		{name: "lock wait match", err: &mysql.MySQLError{Number: 1205}, checker: IsLockWaitTimeout, want: true},
		{name: "table exists match", err: &mysql.MySQLError{Number: 1050}, checker: IsTableExists, want: true},
		{name: "dup column match", err: &mysql.MySQLError{Number: 1060}, checker: IsDuplicateColumn, want: true},
		{name: "dup partition match", err: &mysql.MySQLError{Number: 1517}, checker: IsDuplicatePartition, want: true},
		{name: "dup partition wrong", err: &mysql.MySQLError{Number: 1050}, checker: IsDuplicatePartition, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.checker(tt.err); got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}
