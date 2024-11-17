package errors

import "errors"

var ErrUnknownColumn = errors.New("unknown column")
var ErrMissingRequiredColumn = errors.New("missing required column")
var ErrMissingRequiredValue = errors.New("missing required column value")
var ErrMissingParentElement = errors.New("missing value for parent element")
var ErrReoccurringColumn = errors.New("column index already mapped")

var KnownUserErrors = []error{ErrUnknownColumn, ErrMissingRequiredColumn,
	ErrMissingParentElement, ErrMissingRequiredValue, ErrReoccurringColumn}

func IsKnownUserError(err error) bool {
	for _, known := range KnownUserErrors {
		if errors.Is(err, known) {
			return true
		}
	}

	return false
}
