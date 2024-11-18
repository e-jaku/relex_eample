package server

import (
	"encoding/json"
	"net/http"

	"github.com/formulatehq/data-engineer/internal/errors"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"golang.org/x/xerrors"
)

const MAX_SIZE = 1024 * 1024 * 10 // Limit both memory and file size to 10MB max

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

	if contentType := r.Header.Get("Content-Type"); contentType != "text/csv" {
		h.sendJSON(w, http.StatusUnsupportedMediaType, xerrors.Errorf("Unsupported media type: %s", contentType).Error())
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, MAX_SIZE)
	defer r.Body.Close()

	data, err := h.parser.ParseFile(ctx, r.Body)
	if err != nil {
		if errors.IsKnownUserError(err) {
			h.sendJSON(w, http.StatusBadRequest, err.Error())
		} else {
			h.sendJSON(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	h.sendJSON(w, http.StatusOK, data)
}

func (h *CSVHandler) sendJSON(w http.ResponseWriter, status int, data interface{}) {
	type httpError struct {
		Message string `json:"message"`
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if status > http.StatusCreated {
		h.logger.Error().Msgf("Request failed: %v", data)
		if err := json.NewEncoder(w).Encode(&httpError{Message: data.(string)}); err != nil {
			h.logger.Error().Err(err).Msg("Could not send json response")
		}

		return
	}

	if err := json.NewEncoder(w).Encode(&data); err != nil {
		h.logger.Error().Err(err).Msg("Could not send json response")
	}
}
