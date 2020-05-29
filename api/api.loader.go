package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

func newCreateLoaderFunc(t Transport) CreateLoader {
	return func(input *CreateLoaderInput) (*CreateLoaderOutput, error) {
		return input.Do(context.Background(), t)
	}
}

// CreateLoader creates the loader task.
type CreateLoader func(*CreateLoaderInput) (*CreateLoaderOutput, error)

// CreateLoaderInput defines the parameters for the loader request.
type CreateLoaderInput struct {
	Source                            *string              `json:"source,omitempty"`
	Format                            *string              `json:"format,omitempty"`
	IAMRoleArn                        *string              `json:"iamRoleArn,omitempty"`
	Mode                              *string              `json:"mode,omitempty"`
	Region                            *string              `json:"region,omitempty"`
	FailOnError                       *string              `json:"failOnError,omitempty"`
	Parallelism                       *string              `json:"parallelism,omitempty"`
	ParserConfiguration               *ParserConfiguration `json:"parserConfiguration,omitempty"`
	UpdateSingleCardinalityProperties *string              `json:"updateSingleCardinalityProperties,omitempty"`
	QueueRequest                      *string              `json:"queueRequest,omitempty"`
	Dependencies                      []*string            `json:"dependencies,omitempty"`
}

// ParserConfiguration defines additional parser configuration values.
type ParserConfiguration struct {
	BaseURI       *string `json:"baseUri,omitempty"`
	NamedGraphURI *string `json:"namedGraphUri,omitempty"`
}

// Do CreateLoader request with context and transport.
func (r *CreateLoaderInput) Do(ctx context.Context, transport Transport) (*CreateLoaderOutput, error) {
	var (
		method string
		path   strings.Builder
		body   io.Reader
	)

	method = "POST"

	path.WriteRune('/')
	path.WriteString("loader")

	buf, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	body = bytes.NewReader(buf)

	req, err := http.NewRequest(method, path.String(), body)
	if err != nil {
		return nil, err
	}

	if req.Body != nil {
		req.Header[contentType] = headerContentTypeJSON
	}

	if ctx != nil {
		req = req.WithContext(ctx)
	}

	resp, err := transport.Perform(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	buf, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		var e Error
		if err = json.Unmarshal(buf, &e); err != nil {
			return nil, err
		}
		return nil, &e
	}

	var output CreateLoaderOutput
	if err = json.Unmarshal(buf, &r); err != nil {
		return nil, err
	}
	return &output, nil
}

// CreateLoaderOutput defines the output of loader request.
type CreateLoaderOutput struct {
	Status  *string                    `json:"status,omitempty"`
	Payload *CreateLoaderOutputPayload `json:"payload,omitempty"`
}

// CreateLoaderOutputPayload defines the output payload of loader request.
type CreateLoaderOutputPayload struct {
	LoadID *string `json:"loadId,omitempty"`
}
