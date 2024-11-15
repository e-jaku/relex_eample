package errors

import "errors"

var ErrUnknownColumn = errors.New("unknown column")
var ErrRequiredColumn = errors.New("required column")
var ErrMissingParentElement = errors.New("missing value for parent element")

var KnownUserErrors = []error{ErrUnknownColumn, ErrRequiredColumn, ErrMissingParentElement}

func IsKnownUserError(err error) bool {
	for _, known := range KnownUserErrors {
		if errors.Is(err, known) {
			return true
		}
	}

	return false
}
