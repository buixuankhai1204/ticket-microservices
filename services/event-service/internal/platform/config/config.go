// Package config loads the fully-resolved runtime configuration from the
// environment once, at startup. It has no business meaning and is used only from
// cmd/main.go.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port          int
	DatabaseURL   string
	DBMaxConns    int32
	ShutdownGrace time.Duration
}

// Load reads and validates configuration. DATABASE_URL is required; everything
// else has a bounded default.
func Load() (Config, error) {
	cfg := Config{
		Port:          8082, // must match kong/kong.yml (http://event-service:8082)
		DBMaxConns:    20,
		ShutdownGrace: 15 * time.Second,
	}

	if v := os.Getenv("PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("invalid PORT %q: %w", v, err)
		}
		cfg.Port = p
	}

	cfg.DatabaseURL = os.Getenv("DATABASE_URL")
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL must be set")
	}

	if v := os.Getenv("DB_MAX_CONNS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("invalid DB_MAX_CONNS %q", v)
		}
		cfg.DBMaxConns = int32(n)
	}

	return cfg, nil
}
