package ddbretry

import "fmt"

type InvalidRetryError struct {
	Retries int
}

func (e *InvalidRetryError) Error() string {
	return fmt.Sprintf("invalid value for retries: %d", e.Retries)
}

func NewInvalidRetryError(retries int) *InvalidRetryError {
	return &InvalidRetryError{
		Retries: retries,
	}
}

func IsInvalidRetryError(err error) bool {
	_, ok := err.(*InvalidRetryError)

	return ok
}
