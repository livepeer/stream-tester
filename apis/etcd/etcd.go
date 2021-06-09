// Package etcd API
package etcd

import (
	"context"
	"crypto/tls"
	"errors"
	"time"

	"github.com/golang/glog"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type (
	// API object incapsulating Livepeer's hosted API
	API struct {
		Client  *clientv3.Client
		timeout time.Duration
		// endpoints []string
	}
)

var ErrNotInitialized = errors.New("not initialized")

func NewEtcd(endpoints []string, timeout time.Duration, etcdCaCert, etcdCert, etcdKey string) (*API, error) {
	enp := []string{"localhost:2379", "localhost:22379", "localhost:32379"}
	if len(endpoints) > 0 {
		enp = endpoints
	}
	var tcfg *tls.Config
	var err error
	if etcdCaCert != "" || etcdCert != "" || etcdKey != "" {
		tlsifo := transport.TLSInfo{
			CertFile:      etcdCert,
			KeyFile:       etcdKey,
			TrustedCAFile: etcdCaCert,
		}
		tcfg, err = tlsifo.ClientConfig()
		if err != nil {
			return nil, err
		}
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:        enp,
		DialTimeout:      5 * time.Second,
		AutoSyncInterval: 5 * time.Minute,
		TLS:              tcfg,
	})
	if err != nil {
		// handle error!
		glog.Errorf("Error connecting ETCD err=%v", err)
		return nil, err
	}
	return &API{
		// endpoints: endpoints,
		Client:  cli,
		timeout: timeout,
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
