package config

import (
	"fmt"

	"github.com/go-sql-driver/mysql"
)

// DatabaseConfig holds connection pool and DSN settings.
type DatabaseConfig struct {
	Host        string `yaml:"host"`
	Port        int    `yaml:"port"`
	Database    string `yaml:"database"`
	User        string `yaml:"user"`
	Password    string `yaml:"password"`
	PoolSize    int    `yaml:"poolSize"`
	PoolMinIdle int    `yaml:"poolMinIdle"`
}

// DSN builds a go-sql-driver/mysql data source name string.
//
// Key settings:
//   - parseTime=true: scan DATE/DATETIME into time.Time
//   - interpolateParams=true: client-side parameter interpolation (avoids
//     COM_STMT_PREPARE round-trips, critical for batch performance)
//   - maxAllowedPacket=16MB: matches MariaDB default for large batch UPSERTs
func (c *DatabaseConfig) DSN() string {
	port := c.Port
	if port == 0 {
		port = 3306
	}
	cfg := mysql.NewConfig()
	cfg.User = c.User
	cfg.Passwd = c.Password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", c.Host, port)
	cfg.DBName = c.Database
	cfg.ParseTime = true
	cfg.InterpolateParams = true
	cfg.MaxAllowedPacket = 16 * 1024 * 1024
	return cfg.FormatDSN()
}
