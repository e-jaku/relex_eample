package config

type Config struct{}

func LoadConfig() (*Config, error) {
	return &Config{}, nil
}
