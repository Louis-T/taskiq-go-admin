// Command taskiq-admin is a broker-agnostic admin panel for taskiq, fed by
// taskiq.middlewares.taskiq_admin_middleware.TaskiqAdminMiddleware.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"
	_ "time/tzdata" // viewer time zones without system tzdata (scratch image)

	"github.com/louis-t/taskiq-go-admin/internal/config"
	"github.com/louis-t/taskiq-go-admin/internal/events"
	"github.com/louis-t/taskiq-go-admin/internal/ingest"
	"github.com/louis-t/taskiq-go-admin/internal/maintenance"
	"github.com/louis-t/taskiq-go-admin/internal/store"
	"github.com/louis-t/taskiq-go-admin/internal/store/cache"
	"github.com/louis-t/taskiq-go-admin/internal/store/sqlite"
	"github.com/louis-t/taskiq-go-admin/internal/web"
)

// version is set at build time: -ldflags "-X main.version=v1.2.3".
var version = "dev"

// buildVersion falls back to the VCS revision embedded by the Go toolchain.
func buildVersion() string {
	if version != "dev" {
		return version
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return version
	}
	var rev, dirty string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "-dirty"
			}
		}
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	if rev == "" {
		return version
	}
	return "dev-" + rev + dirty
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		if err := healthcheck(os.Getenv); err != nil {
			fmt.Fprintln(os.Stderr, "taskiq-admin:", err)
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "taskiq-admin:", err)
		os.Exit(1)
	}
}

func openStore(ctx context.Context, cfg config.Config) (store.Store, error) {
	switch cfg.DBDriver {
	case "sqlite":
		return sqlite.Open(ctx, cfg.DBFilePath)
	}
	return nil, fmt.Errorf("unsupported database driver %q", cfg.DBDriver)
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer st.Close()
	startedAt := time.Now()

	hub := events.NewHub()
	ing := ingest.New(st, cfg.IngestMaxBatch, log)
	ing.OnCommit = hub.Publish
	go ing.Run()

	batch := maintenance.BatchConfig{Size: cfg.PurgeBatchSize, Pause: cfg.PurgeBatchPause}
	purger := maintenance.NewPurger(st, cfg.PurgeInterval, batch, log)
	purger.OnChange = hub.Publish

	srv, err := web.NewServer(web.Options{
		APIToken:       cfg.APIToken,
		MaxBodyBytes:   cfg.MaxBodyBytes,
		MaxValueBytes:  cfg.MaxValueBytes,
		PurgeInterval:  cfg.PurgeInterval,
		PurgeBatchSize: cfg.PurgeBatchSize,
		Version:        buildVersion(),
	}, cache.New(st), ing, purger, hub, log)
	if err != nil {
		return err
	}

	httpServer := &http.Server{
		Addr:              cfg.Addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		// Must outlive the client's keep-alive (aiohttp: 15s). Closing idle
		// connections first makes the middleware hit a half-closed socket:
		// "ServerDisconnectedError" and lost reports (taskiq-admin#16).
		IdleTimeout: 120 * time.Second,
	}
	httpServer.RegisterOnShutdown(hub.Close) // end the live-update streams

	var bg sync.WaitGroup
	bgCtx, cancelBg := context.WithCancel(context.Background())
	bg.Go(func() {
		// Tasks still "running" from before this start cannot be followed
		// anymore. Done in the background, in batches, so startup is instant.
		if maintenance.AbandonRunning(bgCtx, st, batch, startedAt, log) > 0 {
			hub.Publish()
		}
		purger.Run(bgCtx)
	})

	serveErr := make(chan error, 1)
	go func() {
		log.Info("taskiq-admin listening", "addr", cfg.Addr, "db", cfg.DBFilePath, "version", buildVersion())
		serveErr <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		cancelBg()
		bg.Wait()
		_ = ing.Close(context.Background())
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down")
	// Docker sends SIGKILL 10s after SIGTERM by default: finish before that.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	// Stop accepting requests and let in-flight ones finish (they wait for
	// their events to be committed), then flush whatever is still queued.
	if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Warn("http shutdown", "err", err)
	}
	if err := ing.Close(shutdownCtx); err != nil {
		log.Warn("pending task events were not written before shutdown", "err", err)
	}
	cancelBg()
	bg.Wait()
	return nil
}
