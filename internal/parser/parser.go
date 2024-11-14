package parser

import "io"

type Parser interface {
	ParseFile(file io.Reader) (map[string]interface{}, error)
}
