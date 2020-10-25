package uhttp

import (
	"context"
	"io"
	"net/http"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/model"
)

// NewRequest creates new HTTP Request object and adds own User Agent
func NewRequest(method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return req, err
	}
	req.Header.Add("User-Agent", model.AppName+"/"+model.Version)
	return req, err
}

// RequireRequest ...
func RequireRequest(method, url string, body io.Reader) *http.Request {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		glog.Fatal(err)
	}
	req.Header.Add("User-Agent", model.AppName+"/"+model.Version)
	return req
}

// GetRequest ...
func GetRequest(url string) *http.Request {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		glog.Fatal(err)
	}
	req.Header.Add("User-Agent", model.AppName+"/"+model.Version)
	return req
}

// NewRequestWithContext ...
func NewRequestWithContext(ctx context.Context, method, url string, body io.Reader) *http.Request {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		glog.Fatal(err)
	}
	req.Header.Add("User-Agent", model.AppName+"/"+model.Version)
	return req
}
