package server

import (
	"net/http"

	"github.com/formulatehq/data-engineer/internal/parser"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

type ParserHandler struct {
	logger *zerolog.Logger
	parser parser.Parser
}

func NewParserHandler(logger *zerolog.Logger, parser parser.Parser) *ParserHandler {
	return &ParserHandler{
		logger: logger,
		parser: parser,
	}
}

func (h *ParserHandler) Router() *chi.Mux {
	r := chi.NewRouter()

	r.Group(func(r chi.Router) {
		r.Post("/", h.handleParseFile)
	})

	return r
}

// TODO: add file request handler  functionality
func (h *ParserHandler) handleParseFile(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := h.logger.With().Str("handler", "handleParseFile").Logger()
	logger.Info().Msg("Parsing file...")

	h.parser.ParseFile(ctx, nil) // file will be passed here
}
