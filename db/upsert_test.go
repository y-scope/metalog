package db

import "testing"

func TestOnDuplicateKeyUpdateValues(t *testing.T) {
	for _, tt := range []struct {
		name string
		want string
		cols []string
	}{
		{name: "single", cols: []string{"a"}, want: "ON DUPLICATE KEY UPDATE a = VALUES(a)"},
		{name: "multi", cols: []string{"a", "b"}, want: "ON DUPLICATE KEY UPDATE a = VALUES(a), b = VALUES(b)"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := OnDuplicateKeyUpdateValues(tt.cols...); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestOnDuplicateKeyUpdateAlias(t *testing.T) {
	for _, tt := range []struct {
		name  string
		alias string
		want  string
		cols  []string
	}{
		{name: "single", alias: "new", cols: []string{"a"}, want: "AS new ON DUPLICATE KEY UPDATE a = new.a"},
		{name: "multi", alias: "new", cols: []string{"a", "b"}, want: "AS new ON DUPLICATE KEY UPDATE a = new.a, b = new.b"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := OnDuplicateKeyUpdateAlias(tt.alias, tt.cols...); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
