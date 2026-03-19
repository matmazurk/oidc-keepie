package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

func main() {
	var (
		keepieURL    string
		webhookAddr  string
		webhookHost  string
		numRequests  int
		concurrency  int
		timeout      time.Duration
	)

	flag.StringVar(&keepieURL, "keepie-url", "http://localhost:8080", "keepie API base URL")
	flag.StringVar(&webhookAddr, "webhook-addr", ":9999", "address to listen on for webhook callbacks")
	flag.StringVar(&webhookHost, "webhook-host", "host.docker.internal:9999", "webhook host as seen from keepie container")
	flag.IntVar(&numRequests, "n", 100, "number of requests to send")
	flag.IntVar(&concurrency, "c", 10, "number of concurrent senders")
	flag.DurationVar(&timeout, "timeout", 30*time.Second, "time to wait for all webhooks after sending")
	flag.Parse()

	var received atomic.Int64

	mux := http.NewServeMux()
	mux.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	})

	listener, err := net.Listen("tcp", webhookAddr)
	if err != nil {
		log.Fatalf("listening on %s: %v", webhookAddr, err)
	}
	server := &http.Server{Handler: mux}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Fatalf("webhook server: %v", err)
		}
	}()
	log.Printf("webhook server listening on %s", webhookAddr)

	webhookURL := fmt.Sprintf("http://%s/", webhookHost)
	log.Printf("sending %d requests to %s (webhook: %s)", numRequests, keepieURL, webhookURL)

	start := time.Now()
	sem := make(chan struct{}, concurrency)
	var sent, failed atomic.Int64

	for i := range numRequests {
		sem <- struct{}{}
		go func(i int) {
			defer func() { <-sem }()
			if err := scheduleJob(keepieURL, webhookURL); err != nil {
				failed.Add(1)
				if failed.Load() <= 5 {
					log.Printf("request %d failed: %v", i, err)
				}
				return
			}
			sent.Add(1)
		}(i)
	}
	for range concurrency {
		sem <- struct{}{}
	}

	sendDuration := time.Since(start)
	log.Printf("sent %d/%d requests in %s (%d failed)",
		sent.Load(), numRequests, sendDuration, failed.Load())

	log.Printf("waiting up to %s for webhooks...", timeout)
	deadline := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r := received.Load()
			log.Printf("  received %d/%d webhooks", r, sent.Load())
			if r >= sent.Load() {
				printResults(sent.Load(), received.Load(), failed.Load(), sendDuration, time.Since(start))
				return
			}
		case <-deadline:
			printResults(sent.Load(), received.Load(), failed.Load(), sendDuration, time.Since(start))
			return
		}
	}
}

var client = &http.Client{
	Transport: &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     90 * time.Second,
	},
}

func scheduleJob(keepieURL, webhookURL string) error {
	body, _ := json.Marshal(map[string]string{"webhook_url": webhookURL})
	resp, err := client.Post(keepieURL+"/jobs", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// drain body to allow connection reuse
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}
	return nil
}

func printResults(sent, received, failed int64, sendDuration, totalDuration time.Duration) {
	log.Println("--- results ---")
	log.Printf("  requests sent:      %d", sent)
	log.Printf("  requests failed:    %d", failed)
	log.Printf("  webhooks received:  %d", received)
	log.Printf("  send duration:      %s", sendDuration)
	log.Printf("  total duration:     %s", totalDuration)
	if sent > 0 {
		log.Printf("  send rate:          %.1f req/s", float64(sent)/sendDuration.Seconds())
	}
}
