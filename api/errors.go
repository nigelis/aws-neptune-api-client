package api

import (
	"strings"
)

// Error defines the common error fields from AWS Neptune.
type Error struct {
	StatusCode *int

	RequestID *string `json:"requestId,omitempty"`
	Code      *string `json:"code,omitempty"`
	Message   *string `json:"detailedMessage,omitempty"`
}

// Error returns the error message.
func (e *Error) Error() string {
	var b strings.Builder

	b.WriteString("requestId: ")
	if e.RequestID != nil {
		b.WriteString(*e.RequestID)
	}
	b.WriteString(", ")
	b.WriteString("code: ")
	if e.Code != nil {
		b.WriteString(*e.Code)
	}
	b.WriteString(", ")
	b.WriteString("message: ")
	if e.Message != nil {
		b.WriteString(*e.Message)
	}
	b.WriteRune('.')

	return b.String()
}
