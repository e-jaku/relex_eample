package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"github.com/formulatehq/data-engineer/internal/config"
	"github.com/formulatehq/data-engineer/internal/csv"
	"github.com/formulatehq/data-engineer/internal/server"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

func main() {
	ctx := context.Background()
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()

	if err := run(ctx, &logger); err != nil {
		logger.Fatal().Err(err).Msg("Failed to run app")
	}
}

func run(ctx context.Context, logger *zerolog.Logger) error {
	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to load config")
	}

	done := make(chan struct{})
	quit := make(chan os.Signal, 1)

	csvParser := csv.NewCSVParser(20)

	svr := server.NewServer(cfg, func(m chi.Router) {
		m.Mount("/", server.NewCSVHandler(logger, csvParser).Router())
	})

	signal.Notify(quit, os.Interrupt)

	// ensure that the server shuts down gracefully
	go func() {
		<-quit

		logger.Info().Msg("Server is shutting down...")

		c, cancel := context.WithTimeout(ctx, cfg.ShutdownTimeout)
		defer cancel()

		svr.SetKeepAlivesEnabled(false)

		if err := svr.Shutdown(c); err != nil {
			logger.Fatal().Err(err).Msg("Failed to gracefully shutdown server")
		}

		close(done)
	}()

	logger.Info().Msgf("Starting server at %s", svr.Addr)

	if err := svr.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to start server at %s: %w", svr.Addr, err)
	}

	<-done

	logger.Info().Msg("Server stopped")

	return nil
}
