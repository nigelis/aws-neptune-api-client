package transport

import (
	"bytes"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/stretchr/testify/assert"
)

func newDefaultSigner() *v4.Signer {
	return v4.NewSigner(credentials.NewStaticCredentials("ABC", "DEF", ""))
}

func TestSetReqAuthWithExistingHeader(t *testing.T) {
	test := assert.New(t)

	url := url.URL{Scheme: "https", Host: "hosts"}
	req, err := http.NewRequest("GET", url.String(), nil)
	test.Nil(err)

	c, err := New(Config{URL: &url, Region: "local", Signer: newDefaultSigner()})
	test.Nil(err)

	auth := "AWS4-ABCD:EFG"
	req.Header.Set("Authorization", auth)
	req, err = c.setReqAuth(&url, req)
	test.Nil(err)

	header := req.Header.Get("Authorization")
	test.Equal(auth, header)
}

func TestSetReqAuthWithNilBody(t *testing.T) {
	test := assert.New(t)

	url := url.URL{Scheme: "https", Host: "hosts"}
	req, err := http.NewRequest("GET", url.String(), nil)
	test.Nil(err)

	c, err := New(Config{URL: &url, Region: "local", Signer: newDefaultSigner()})
	test.Nil(err)

	req, err = c.setReqAuth(&url, req)
	test.Nil(err)

	header := req.Header.Get("Authorization")
	test.True(strings.HasPrefix(header, "AWS4"))
}

func TestSetReqAuthWithNoBody(t *testing.T) {
	test := assert.New(t)

	url := url.URL{Scheme: "https", Host: "hosts"}
	req, err := http.NewRequest("GET", url.String(), http.NoBody)
	test.Nil(err)

	c, err := New(Config{URL: &url, Region: "local", Signer: newDefaultSigner()})
	test.Nil(err)

	req, err = c.setReqAuth(&url, req)
	test.Nil(err)

	header := req.Header.Get("Authorization")
	test.True(strings.HasPrefix(header, "AWS4"))
}

func TestSetReqAuthWithNormalBody(t *testing.T) {
	test := assert.New(t)

	buf := []byte("1234")
	url := url.URL{Scheme: "https", Host: "hosts"}
	req, err := http.NewRequest("GET", url.String(), bytes.NewReader(buf))
	test.Nil(err)

	c, err := New(Config{URL: &url, Region: "local", Signer: newDefaultSigner()})
	test.Nil(err)

	req, err = c.setReqAuth(&url, req)
	test.Nil(err)

	header := req.Header.Get("Authorization")
	test.True(strings.HasPrefix(header, "AWS4"))
}
