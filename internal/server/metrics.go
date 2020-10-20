package server

import (
	"context"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/internal/metrics"
)

// MetricsServer object
type MetricsServer struct {
}

// NewMetricsServer creates new MetricsServer
func NewMetricsServer() *MetricsServer {
	return &MetricsServer{}
}

// Start starts the metric server
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
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		glog.Infof("Shutting down metrics server")
		srv.Shutdown(c)
	}()

	glog.Info("Metrics server listening on ", bindAddr)
	srv.ListenAndServe()
}
