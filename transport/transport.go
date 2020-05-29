package transport

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"runtime"
	"strings"
	"time"

	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
)

var (
	userAgent string
)

func init() {
	userAgent = initUserAgent()
}

// Interface defines the interface of HTTP client.
type Interface interface {
	Perform(*http.Request) (*http.Response, error)
}

// Config represents the configuration of HTTP client.
type Config struct {
	// URL is the endpoint of AWS Neptune cluster.
	URL *url.URL

	// Region is the AWS region where AWS Neptune cluster is located in. Required only when Signer is not nil.
	Region string

	// Signer is to sign the request with AWS V4 signature.
	// When it is nil, the request won't be signed.
	Signer *v4.Signer

	// Transport represents the execution of a single HTTP transaction.
	// http.DefaultTransport will be used if not specified.
	Transport http.RoundTripper
}

// Client represents the HTTP client.
type Client struct {
	url *url.URL

	region string
	signer *v4.Signer

	transport http.RoundTripper
}

// New creates new transport client.
// http.DefaultTransport will be used if no transport is passed in the configuration.
func New(cfg Config) (*Client, error) {
	transport := cfg.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	client := Client{
		url:       cfg.URL,
		region:    cfg.Region,
		signer:    cfg.Signer,
		transport: transport,
	}

	return &client, nil
}

// Perform executes the request and returns a response or error.
func (c *Client) Perform(req *http.Request) (*http.Response, error) {
	// Update request
	c.setReqUserAgent(req)
	c.setReqURL(c.url, req)
	if c.signer != nil {
		_, err := c.setReqAuth(c.url, req)
		if err != nil {
			return nil, err
		}
	}

	res, err := c.transport.RoundTrip(req)
	return res, err
}

func (c *Client) setReqAuth(u *url.URL, req *http.Request) (*http.Request, error) {
	if h, ok := req.Header["Authorization"]; ok && len(h) > 0 && strings.HasPrefix(h[0], "AWS4") {
		return req, nil
	}

	now := time.Now().UTC()
	req.Header.Set("Date", now.Format(time.RFC3339))

	var body io.ReadSeeker
	if req.Body != nil && req.Body != http.NoBody {
		var buf bytes.Buffer
		_, err := buf.ReadFrom(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body = ioutil.NopCloser(&buf)
		body = bytes.NewReader(buf.Bytes())
	}
	_, err := c.signer.Sign(req, body, "neptune", c.region, now)
	return req, err
}

func (c *Client) setReqURL(u *url.URL, req *http.Request) *http.Request {
	req.URL.Scheme = u.Scheme
	req.URL.Host = u.Host

	if u.Path != "" {
		var b strings.Builder
		b.Grow(len(u.Path) + len(req.URL.Path))
		b.WriteString(u.Path)
		b.WriteString(req.URL.Path)
		req.URL.Path = b.String()
	}

	return req
}

func (c *Client) setReqUserAgent(req *http.Request) *http.Request {
	req.Header.Set("User-Agent", userAgent)
	return req
}

func initUserAgent() string {
	reGoVersion := regexp.MustCompile(`go(\d+\.\d+\..+)`)
	var b strings.Builder

	b.WriteString("aws-neptune-api-client (")
	b.WriteString(runtime.GOOS)
	b.WriteRune(' ')
	b.WriteString(runtime.GOARCH)
	b.WriteString("; Go ")
	if v := reGoVersion.ReplaceAllString(runtime.Version(), "$1"); v != "" {
		b.WriteString(v)
	} else {
		b.WriteString(runtime.Version())
	}
	b.WriteRune(')')

	return b.String()
}
