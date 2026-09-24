// Command server runs the calibration service.
//
// Configuration is via environment:
//
//	DATABASE_URL  PostgreSQL DSN (required)
//	PORT          listen port (default 8080)
package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"github.com/example/calibsvc/internal/httpapi"
	"github.com/example/calibsvc/internal/store"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		logger.Error("DATABASE_URL is required")
		os.Exit(1)
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		logger.Error("cannot open database handle", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	// The service only starts accepting traffic once the database is
	// reachable and the schema is in place, so "healthy" implies "ready".
	if err := waitForDB(db, 60*time.Second); err != nil {
		logger.Error("database not reachable", "error", err)
		os.Exit(1)
	}
	st := store.New(db)
	if err := st.Migrate(context.Background()); err != nil {
		logger.Error("schema migration failed", "error", err)
		os.Exit(1)
	}
	logger.Info("database ready")

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           httpapi.New(st).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening", "port", port)
		errCh <- srv.ListenAndServe()
	}()

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case <-sigCtx.Done():
		logger.Info("shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	}
}

func waitForDB(db *sql.DB, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var err error
	for {
		if err = db.Ping(); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(500 * time.Millisecond)
	}
}
