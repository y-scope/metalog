package health

import (
	"context"
	"database/sql"
	"fmt"
)

// DBChecker verifies database connectivity for the readiness probe.
type DBChecker struct {
	DB *sql.DB
}

// CheckReady pings the database to verify the connection pool is healthy.
func (c *DBChecker) CheckReady(ctx context.Context) error {
	if err := c.DB.PingContext(ctx); err != nil {
		return fmt.Errorf("database ping: %w", err)
	}
	return nil
}
