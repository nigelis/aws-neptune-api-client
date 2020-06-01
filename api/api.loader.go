package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

func newCancelLoaderFunc(t Transport) CancelLoader {
	return func(input *CancelLoaderInput) error {
		return input.Do(context.Background(), t)
	}
}

// CancelLoader cancels the load job.
type CancelLoader func(*CancelLoaderInput) error

// CancelLoaderInput configures the loader cancel job request.
type CancelLoaderInput struct {
	LoadID *string
}

// Do executes the request and returns response or error.
func (input *CancelLoaderInput) Do(ctx context.Context, t Transport) error {
	var (
		method string
		path   strings.Builder
	)

	method = "DELETE"

	path.WriteRune('/')
	path.WriteString("loader")

	req, err := http.NewRequest(method, path.String(), http.NoBody)
	if err != nil {
		return err
	}

	u := req.URL
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return err
	}
	if input.LoadID == nil {
		return errors.New("loadId is required")
	}
	q.Set("loadId", *input.LoadID)
	u.RawQuery = q.Encode()

	if ctx != nil {
		req = req.WithContext(ctx)
	}

	resp, err := t.Perform(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var e Error
	if err = json.Unmarshal(buf, &e); err != nil {
		return err
	}
	return &e
}

func newCreateLoaderFunc(t Transport) CreateLoader {
	return func(input *CreateLoaderInput) (*CreateLoaderOutput, error) {
		return input.Do(context.Background(), t)
	}
}

// CreateLoader creates a load job.
type CreateLoader func(*CreateLoaderInput) (*CreateLoaderOutput, error)

// CreateLoaderInput configures the loader request.
type CreateLoaderInput struct {
	Source                            *string                          `json:"source,omitempty"`
	Format                            *string                          `json:"format,omitempty"`
	IAMRoleArn                        *string                          `json:"iamRoleArn,omitempty"`
	Mode                              *string                          `json:"mode,omitempty"`
	Region                            *string                          `json:"region,omitempty"`
	FailOnError                       *string                          `json:"failOnError,omitempty"`
	Parallelism                       *string                          `json:"parallelism,omitempty"`
	ParserConfiguration               *CreateLoaderParserConfiguration `json:"parserConfiguration,omitempty"`
	UpdateSingleCardinalityProperties *string                          `json:"updateSingleCardinalityProperties,omitempty"`
	QueueRequest                      *string                          `json:"queueRequest,omitempty"`
	Dependencies                      []*string                        `json:"dependencies,omitempty"`
}

// CreateLoaderParserConfiguration configures additional parser configuration values.
type CreateLoaderParserConfiguration struct {
	BaseURI       *string `json:"baseUri,omitempty"`
	NamedGraphURI *string `json:"namedGraphUri,omitempty"`
}

// Do executes the request and returns response or error.
func (input *CreateLoaderInput) Do(ctx context.Context, t Transport) (*CreateLoaderOutput, error) {
	var (
		method string
		path   strings.Builder
		body   io.Reader
	)

	method = "POST"

	path.WriteRune('/')
	path.WriteString("loader")

	buf, err := json.Marshal(input)
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

	resp, err := t.Perform(req)
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

	var o CreateLoaderOutput
	if err = json.Unmarshal(buf, &o); err != nil {
		return nil, err
	}
	return &o, nil
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

// GetLoaderInput configures the get status request.
type GetLoaderInput struct {
	LoadID             *string
	Details            *string
	Errors             *string
	Page               *int
	ErrorsPerPage      *int
	Limit              *int
	IncludeQueuedLoads *string
}

// Do executes the request and returns response or error.
func (i *GetLoaderInput) Do(ctx context.Context, t Transport) (*GetLoaderOutput, error) {
	var (
		method string
		path   strings.Builder
	)

	method = "GET"

	path.WriteRune('/')
	path.WriteString("loader")

	req, err := http.NewRequest(method, path.String(), http.NoBody)
	if err != nil {
		return nil, err
	}

	u := req.URL
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, err
	}
	if i.LoadID != nil {
		q.Add("loadId", *i.LoadID)
	}
	if i.Details != nil {
		q.Add("details", *i.Details)
	}
	if i.Errors != nil {
		q.Add("errors", *i.Errors)
	}
	if i.Page != nil {
		q.Add("page", string(*i.Page))
	}
	if i.ErrorsPerPage != nil {
		q.Add("errorsPerPage", string(*i.ErrorsPerPage))
	}
	if i.Limit != nil {
		q.Add("limit", string(*i.Limit))
	}
	if i.IncludeQueuedLoads != nil {
		q.Add("includeQueuedLoads", *i.IncludeQueuedLoads)
	}
	u.RawQuery = q.Encode()

	if ctx != nil {
		req = req.WithContext(ctx)
	}

	resp, err := t.Perform(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	buf, err := ioutil.ReadAll(resp.Body)
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

	var o GetLoaderOutput
	if err = json.Unmarshal(buf, &o); err != nil {
		return nil, err
	}
	return &o, nil
}

func newGetLoaderFunc(t Transport) GetLoader {
	return func(input *GetLoaderInput) (*GetLoaderOutput, error) {
		return input.Do(context.Background(), t)
	}
}

// GetLoader returns the load status.
type GetLoader func(*GetLoaderInput) (*GetLoaderOutput, error)

// GetLoaderOutput defines the output of the Neptune Loader Get-Status request.
type GetLoaderOutput struct {
	*string `json:"status,omitempty"`
	Payload *GetLoaderOutputPayload `json:"payload,omitempty"`
}

// GetLoaderOutputPayload defines the payload fields of GetLoaderOutput.
type GetLoaderOutputPayload struct {
	FeedCount     []*map[string]int64          `json:"feedCount,omitempty"`
	OverallStatus *GetLoaderOutputFeedStatus   `json:"overallStatus,omitempty"`
	FailedFeeds   []*GetLoaderOutputFeedStatus `json:"failedFeeds,omitempty"`
	Errors        *GetLoaderOutputErrors       `json:"errors,omitempty"`
	LoadIDs       []*string                    `json:"loadIds,omitempty"`
}

// GetLoaderOutputFeedStatus defines the feed status fields of GetLoaderOutput.
type GetLoaderOutputFeedStatus struct {
	FullURI                *string `json:"fullUri,omitempty"`
	RunNumber              *int64  `json:"runNumber,omitempty"`
	RetryNumber            *int64  `json:"retryNumber,omitempty"`
	Status                 *string `json:"status,omitempty"`
	TotalTimeSpent         *int64  `json:"totalTimeSpent,omitempty"`
	StartTime              *int64  `json:"startTime,omitempty"`
	TotalRecords           *int64  `json:"totalRecords,omitempty"`
	TotalDuplicates        *int64  `json:"totalDuplicates,omitempty"`
	ParsingErrors          *int64  `json:"parsingErrors,omitempty"`
	DatatypeMismatchErrors *int64  `json:"datatypeMismatchErrors,omitempty"`
	InsertErrors           *int64  `json:"insertErrors"`
}

// GetLoaderOutputErrors defines the errors fields of GetLoaderOutput.
type GetLoaderOutputErrors struct {
	StartIndex *int64                     `json:"startIndex,omitempty"`
	EndIndex   *int64                     `json:"endIndex,omitempty"`
	LoadID     *string                    `json:"loadId,omitempty"`
	ErrorLogs  []*GetLoaderOutputErrorLog `json:"errorLogs,omitempty"`
}

// GetLoaderOutputErrorLog defines the error log struct of GetLoaderOutput.
type GetLoaderOutputErrorLog struct {
	ErrorCode    *string `json:"errorCode,omitempty"`
	ErrorMessage *string `json:"errorMessage,omitempty"`
	FileName     *string `json:"fileName,omitempty"`
	RecordNum    *string `json:"recordNum,omitempty"`
}
