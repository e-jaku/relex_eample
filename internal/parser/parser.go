package parser

import (
	"context"
	"io"
)

type Parser interface {
	ParseFile(ctx context.Context, file io.Reader) (map[string]interface{}, error)
}
