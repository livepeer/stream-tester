// Package etcd API
package etcd

import (
	"context"
	"errors"
	"time"

	"github.com/golang/glog"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type (
	// API object incapsulating Livepeer's hosted API
	API struct {
		endpoints []string
		Client    *clientv3.Client
		timeout   time.Duration
	}
)

var ErrNotInitialized = errors.New("not initialized")

func NewEtcd(endpoints []string, timeout time.Duration) (*API, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379", "localhost:22379", "localhost:32379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		// handle error!
		glog.Errorf("Error connecting ETCD err=%v", err)
		return nil, err
	}
	return &API{
		endpoints: endpoints,
		Client:    cli,
		timeout:   timeout,
	}, nil
}

func (api *API) Put(ctx context.Context, key, value string) (*clientv3.PutResponse, error) {
	if api.Client == nil {
		return nil, ErrNotInitialized
	}
	ctx, cancel := context.WithTimeout(ctx, api.timeout)
	resp, err := api.Client.Put(ctx, key, value)
	cancel()
	return resp, err
}

func (api *API) Close() error {
	if api.Client != nil {
		return api.Client.Close()
	}
	return nil
}
