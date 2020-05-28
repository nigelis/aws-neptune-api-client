package api

import "net/http"

const (
	contentType     = "Content-Type"
	applicationJSON = "application/json"
)

var (
	headerContentTypeJSON = []string{applicationJSON}
)

// Transport defines the interface for an API client.
type Transport interface {
	Perform(*http.Request) (*http.Response, error)
}

// API contains the AWS Neptune APIs.
type API struct {
}