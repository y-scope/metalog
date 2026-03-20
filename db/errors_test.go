package db

import (
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
)

func TestIsMySQLError(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		checker func(error) bool
		want    bool
	}{
		// IsDeadlock
		{"deadlock: correct code", &mysql.MySQLError{Number: 1213}, IsDeadlock, true},
		{"deadlock: wrong code", &mysql.MySQLError{Number: 1062}, IsDeadlock, false},
		{"deadlock: non-mysql", fmt.Errorf("some error"), IsDeadlock, false},
		{"deadlock: nil", nil, IsDeadlock, false},
		{"deadlock: wrapped", fmt.Errorf("wrapped: %w", &mysql.MySQLError{Number: 1213}), IsDeadlock, true},

		// IsDuplicateKey
		{"dup key: correct code", &mysql.MySQLError{Number: 1062}, IsDuplicateKey, true},
		{"dup key: wrapped", fmt.Errorf("wrap: %w", &mysql.MySQLError{Number: 1062}), IsDuplicateKey, true},

		// IsLockWaitTimeout
		{"lock wait: correct code", &mysql.MySQLError{Number: 1205}, IsLockWaitTimeout, true},

		// IsTableExists — verify adjacent codes don't collide
		{"table exists: 1050", &mysql.MySQLError{Number: 1050}, IsTableExists, true},
		{"table exists: 1051 is not 1050", &mysql.MySQLError{Number: 1051}, IsTableExists, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.checker(tt.err); got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}
