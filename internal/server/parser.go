package server

import (
	"context"
	"io"

	"github.com/formulatehq/data-engineer/internal/domain"
)

type Parser interface {
	ParseFile(ctx context.Context, file io.Reader) (*domain.Node, error)
}
