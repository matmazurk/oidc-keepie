package main

import (
	"os"
	"strings"
)

type config struct {
	brokers  []string
	topic    string
	groupID  string
	httpPort string
}

func loadConfig() config {
	return config{
		brokers:  strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
		topic:    getEnv("KAFKA_TOPIC", "keepie-jobs"),
		groupID:  getEnv("KAFKA_GROUP_ID", "keepie"),
		httpPort: getEnv("HTTP_PORT", "8080"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
