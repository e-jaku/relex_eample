package errors

var ErrValidationError = NewValidationError("validation error")

type ValidationError struct {
	Message string
}

func NewValidationError(message string) error {
	return &ValidationError{
		Message: message,
	}
}

func (e *ValidationError) Error() string {
	return e.Message
}

func (e *ValidationError) Is(target error) bool {
	_, ok := target.(*ValidationError)
	return ok
}
