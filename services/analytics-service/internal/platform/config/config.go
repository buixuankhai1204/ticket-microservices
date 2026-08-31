package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port          int
	DatabaseURL   string
	DBMaxConns    int32
	ShutdownGrace time.Duration

	KafkaBrokers             []string
	KafkaUserEventsTopic     string
	KafkaConsumerMaxAttempts int
}

func Load() (Config, error) {
	cfg := Config{
		Port:                     8084,
		DBMaxConns:               20,
		ShutdownGrace:            15 * time.Second,
		KafkaUserEventsTopic:     "user.events",
		KafkaConsumerMaxAttempts: 5,
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

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		return Config{}, fmt.Errorf("KAFKA_BROKERS must be set")
	}
	for _, b := range strings.Split(brokers, ",") {
		if b = strings.TrimSpace(b); b != "" {
			cfg.KafkaBrokers = append(cfg.KafkaBrokers, b)
		}
	}
	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS contained no usable entries")
	}

	if v := os.Getenv("KAFKA_USER_EVENTS_TOPIC"); v != "" {
		cfg.KafkaUserEventsTopic = v
	}

	if v := os.Getenv("KAFKA_CONSUMER_MAX_ATTEMPTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return Config{}, fmt.Errorf("invalid KAFKA_CONSUMER_MAX_ATTEMPTS %q", v)
		}
		cfg.KafkaConsumerMaxAttempts = n
	}

	return cfg, nil
}
