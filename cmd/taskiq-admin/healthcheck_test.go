package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func envFor(srv *httptest.Server) func(string) string {
	host, port, _ := net.SplitHostPort(srv.Listener.Addr().String())
	return func(k string) string {
		return map[string]string{"HOST": host, "PORT": port}[k]
	}
}

func TestHealthcheck(t *testing.T) {
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer healthy.Close()
	if err := healthcheck(envFor(healthy)); err != nil {
		t.Fatalf("healthy server: %v", err)
	}

	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer broken.Close()
	if err := healthcheck(envFor(broken)); err == nil {
		t.Fatal("unhealthy server reported healthy")
	}

	env := envFor(broken)
	broken.Close()
	if err := healthcheck(env); err == nil {
		t.Fatal("stopped server reported healthy")
	}
}

func TestProbeHost(t *testing.T) {
	for in, want := range map[string]string{"": "127.0.0.1", "0.0.0.0": "127.0.0.1", "::": "::1", "10.0.0.5": "10.0.0.5"} {
		if got := probeHost(in); got != want {
			t.Errorf("probeHost(%q) = %q, want %q", in, got, want)
		}
	}
}
