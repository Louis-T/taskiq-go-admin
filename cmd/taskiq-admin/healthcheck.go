package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

const healthTimeout = 3 * time.Second

// healthcheck probes the running server's /healthz, for Docker's HEALTHCHECK:
// distroless has no shell, curl or wget. It reads only the listen address
// (HOST, PORT), never the rest of the configuration, so it does not need the
// API token.
func healthcheck(getenv func(string) string) error {
	host, port := getenv("HOST"), getenv("PORT")
	if port == "" {
		port = "3000"
	}
	target := "http://" + net.JoinHostPort(probeHost(host), port) + "/healthz"
	client := &http.Client{Timeout: healthTimeout}
	resp, err := client.Get(target)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<10))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck: %s answered %s", target, resp.Status)
	}
	return nil
}

// probeHost turns a wildcard listen host into a loopback address to dial.
func probeHost(host string) string {
	switch host {
	case "", "0.0.0.0":
		return "127.0.0.1"
	case "::":
		return "::1"
	}
	return host
}
