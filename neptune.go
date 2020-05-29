package neptune

import (
	"fmt"
	"net/url"
	"strings"

	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/nigelis/aws-neptune-api-client/api"
	"github.com/nigelis/aws-neptune-api-client/transport"
)

// Config represents the configuration of neptune client.
type Config struct {
	// Address is the Amazon Neptune endpoints to connect.
	// Use the cluster endpoint in most scenarios.
	Address string

	// Region is the AWS region. Required when Signer is not nil.
	Region string

	// Sign the HTTP request with AWS V4 signature.
	// Default: nil.
	Signer *v4.Signer
}

// Client represents the neptune client.
type Client struct {
	// Embeds the API methods
	*api.API

	Transport transport.Interface
}

// NewClient creates a new client with configuration from cfg.
func NewClient(cfg Config) (*Client, error) {
	u, err := url.Parse(strings.TrimRight(cfg.Address, "/"))
	if err != nil {
		return nil, err
	}

	tp, err := transport.New(transport.Config{
		URL:    u,
		Region: cfg.Region,
		Signer: cfg.Signer,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create transport: %s", err)
	}

	client := &Client{
		Transport: tp,
		API:       api.New(tp),
	}

	return client, nil
}
