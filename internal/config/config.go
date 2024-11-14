package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
	"golang.org/x/xerrors"
)

type Config struct {
	Address         string        `envconfig:"SERVER_ADDRESS" default:":8080"`
	ReadTimeout     time.Duration `envconfig:"SERVER_READ_TIMEOUT" default:"5s"`
	WriteTimeout    time.Duration `envconfig:"SERVER_WRITE_TIMEOUT" default:"10s"`
	IdleTimeout     time.Duration `envconfig:"SERVER_IDLE_TIMEOUT" default:"15s"`
	ShutdownTimeout time.Duration `envconfig:"SERVER_SHUTDOWN_TIMEOUT" default:"30s"`
	RequestTimeout  time.Duration `envconfig:"SERVER_REQUEST_TIMEOUT" default:"45s"`
}

func LoadConfig() (*Config, error) {
	var config Config

	if err := envconfig.Process("", &config); err != nil {
		return nil, xerrors.Errorf("error loading config: %w", err)
	}

	return &config, nil
}
