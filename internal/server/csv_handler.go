package server

import (
	"encoding/json"
	"errors"
	"net/http"

	validation_errors "github.com/formulatehq/data-engineer/internal/errors"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"golang.org/x/xerrors"
)

const (
	MAX_SIZE             = 1024 * 1024 * 10 // Limit both memory and file size to 10MB max
	ALLOWED_CONTENT_TYPE = "text/csv"
)

type CSVHandler struct {
	logger *zerolog.Logger
	parser Parser
}

func NewCSVHandler(logger *zerolog.Logger, parser Parser) *CSVHandler {
	return &CSVHandler{
		logger: logger,
		parser: parser,
	}
}

func (h *CSVHandler) Router() *chi.Mux {
	r := chi.NewRouter()

	r.Group(func(r chi.Router) {
		r.Post("/", h.handleParseCSV)
	})

	return r
}

func (h *CSVHandler) handleParseCSV(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := h.logger.With().Str("handler", "handleParseCSV").Logger()
	logger.Info().Msg("Parsing file...")

	if contentType := r.Header.Get("Content-Type"); contentType != ALLOWED_CONTENT_TYPE {
		h.sendErr(w, http.StatusUnsupportedMediaType, xerrors.Errorf("Unsupported media type: %s", contentType).Error())
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, MAX_SIZE)
	defer r.Body.Close()

	data, err := h.parser.ParseFile(ctx, r.Body)
	if err != nil {
		if errors.Is(err, validation_errors.ErrValidationError) {
			h.sendErr(w, http.StatusBadRequest, err.Error())
		} else {
			h.sendErr(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	h.sendJSON(w, http.StatusOK, data)
}

func (h *CSVHandler) sendJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(&data); err != nil {
		h.logger.Error().Err(err).Msg("Could not send json response")
	}
}

func (h *CSVHandler) sendErr(w http.ResponseWriter, status int, msg string) {
	type httpError struct {
		Message string `json:"message"`
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	h.logger.Error().Msgf("Request failed: %v", msg)
	if err := json.NewEncoder(w).Encode(&httpError{Message: msg}); err != nil {
		h.logger.Error().Err(err).Msg("Could not send json response")
	}
}
