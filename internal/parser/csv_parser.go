package parser

import (
	"context"
	"io"
)

type csvParser struct{}

func NewCsvParser() Parser {
	return &csvParser{}
}

func (p *csvParser) ParseFile(ctx context.Context, file io.Reader) (map[string]interface{}, error) {
	return nil, nil
}
