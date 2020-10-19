package server

import (
	"context"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/internal/metrics"
)

type MetricsServer struct {
}

// NewStreamerServer creates new MetricsServer
func NewMetricsServer() *MetricsServer {
	return &MetricsServer{}
}

// Starts metric server
// blocks until exit
func (s *MetricsServer) Start(ctx context.Context, bindAddr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Exporter)

	srv := &http.Server{
		Addr:    bindAddr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		c, _ := context.WithTimeout(context.Background(), time.Second)
		glog.Infof("Shutting down metrics server")
		srv.Shutdown(c)
	}()

	glog.Info("Metrics server listening on ", bindAddr)
	srv.ListenAndServe()
}
