package main

import (
	"os"
	"strconv"
	"strings"
)

type config struct {
	brokers  []string
	topic    string
	groupID  string
	httpPort string
	poolSize int
}

func loadConfig() config {
	return config{
		brokers:  strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
		topic:    getEnv("KAFKA_TOPIC", "keepie-jobs"),
		groupID:  getEnv("KAFKA_GROUP_ID", "keepie"),
		httpPort: getEnv("HTTP_PORT", "8080"),
		poolSize: getEnvInt("WORKER_POOL_SIZE", 10),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}
