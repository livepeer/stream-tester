package metrics

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/golang/glog"

	"contrib.go.opencensus.io/exporter/prometheus"
	rprom "github.com/prometheus/client_golang/prometheus"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

const (
	logLevel = 6 // TODO move log levels definitions to separate package
	// importing `common` package here introduces import cycles
)

type (
	censusMetricsCounter struct {
		nodeID             string
		ctx                context.Context
		kNodeID            tag.Key
		mCurrentStreams    *stats.Int64Measure
		mSuccessfulStreams *stats.Int64Measure
		mTotalStreams      *stats.Int64Measure

		mStartupLatency   *stats.Float64Measure
		mTranscodeLatency *stats.Float64Measure

		activeStreams int64
		lock          sync.Mutex
	}
)

// Exporter Prometheus exporter that handles `/metrics` endpoint
var Exporter *prometheus.Exporter

var census censusMetricsCounter

// InitCensus init metrics
func InitCensus(nodeID, version string) {
	census = censusMetricsCounter{
		nodeID: nodeID,
	}
	var err error
	ctx := context.Background()
	census.kNodeID = tag.MustNewKey("node_id")
	census.ctx, err = tag.New(ctx, tag.Insert(census.kNodeID, nodeID))
	if err != nil {
		glog.Fatal("Error creating context", err)
	}
	census.mCurrentStreams = stats.Int64("current_streams", "Number of active streams", "tot")
	census.mSuccessfulStreams = stats.Int64("successful_streams", "Number of streams resulted in success", "tot")
	census.mTotalStreams = stats.Int64("total_streams", "Number of total streams", "tot")

	census.mStartupLatency = stats.Float64("startup_latency", "Startup latency", "sec")
	census.mTranscodeLatency = stats.Float64("transcode_latency", "Transcode latency", "sec")

	glog.Infof("Compiler: %s Arch %s OS %s Go version %s", runtime.Compiler, runtime.GOARCH, runtime.GOOS, runtime.Version())
	glog.Infof("Streamtester version: %s", version)
	glog.Infof("Node ID %s", nodeID)
	mVersions := stats.Int64("versions", "Version information.", "Num")
	compiler := tag.MustNewKey("compiler")
	goarch := tag.MustNewKey("goarch")
	goos := tag.MustNewKey("goos")
	goversion := tag.MustNewKey("goversion")
	streamtesterVersion := tag.MustNewKey("streamtesterversion")
	ctx, err = tag.New(ctx, tag.Insert(census.kNodeID, nodeID),
		tag.Insert(compiler, runtime.Compiler), tag.Insert(goarch, runtime.GOARCH), tag.Insert(goos, runtime.GOOS),
		tag.Insert(goversion, runtime.Version()), tag.Insert(streamtesterVersion, version))
	if err != nil {
		glog.Fatal("Error creating tagged context", err)
	}
	baseTags := []tag.Key{census.kNodeID}
	views := []*view.View{
		{
			Name:        "versions",
			Measure:     mVersions,
			Description: "Versions used by StreamTester.",
			TagKeys:     []tag.Key{compiler, goos, goversion, streamtesterVersion},
			Aggregation: view.LastValue(),
		},
		{
			Name:        "current_streams",
			Measure:     census.mCurrentStreams,
			Description: "Number of active streams",
			TagKeys:     baseTags,
			Aggregation: view.LastValue(),
		},
		{
			Name:        "successful_streams",
			Measure:     census.mSuccessfulStreams,
			Description: "Number of streams resulted in success",
			TagKeys:     baseTags,
			Aggregation: view.Count(),
		},
		{
			Name:        "total_streams",
			Measure:     census.mTotalStreams,
			Description: "Number of total streams",
			TagKeys:     baseTags,
			Aggregation: view.Count(),
		},
		{
			Name:        "startup_latency",
			Measure:     census.mStartupLatency,
			Description: "Startup latency",
			TagKeys:     baseTags,
			Aggregation: view.Distribution(0, 0.050, 0.100, .250, .500, .750, 1.000, 1.250, 1.500, 2.000, 2.500, 3.000, 3.500, 4.000, 4.500, 5.000, 10.000, 20.0, 30.0),
		},
		{
			Name:        "transcode_latency",
			Measure:     census.mTranscodeLatency,
			Description: "Transcode latency",
			TagKeys:     baseTags,
			Aggregation: view.Distribution(0, 0.050, 0.100, .250, .500, .750, 1.000, 1.250, 1.500, 2.000, 2.500, 3.000, 3.500, 4.000, 4.500, 5.000, 10.000, 20.0, 30.0, 60.0),
		},
	}

	// Register the views
	if err := view.Register(views...); err != nil {
		glog.Fatalf("Failed to register views: %v", err)
	}
	registry := rprom.NewRegistry()
	registry.MustRegister(rprom.NewProcessCollector(rprom.ProcessCollectorOpts{}))
	registry.MustRegister(rprom.NewGoCollector())
	pe, err := prometheus.NewExporter(prometheus.Options{
		Namespace: "streamtester",
		Registry:  registry,
	})
	if err != nil {
		glog.Fatalf("Failed to create the Prometheus stats exporter: %v", err)
	}

	// Register the Prometheus exporters as a stats exporter.
	view.RegisterExporter(pe)
	stats.Record(ctx, mVersions.M(1))
	Exporter = pe

	// init metrics values
	stats.Record(census.ctx, census.mCurrentStreams.M(0))
}

// CurrentStreams set number of active streams
// func CurrentStreams(cs int) {
// 	stats.Record(census.ctx, census.mCurrentStreams.M(int64(cs)))
// }

// StartStream ...
func StartStream() {
	census.lock.Lock()
	census.activeStreams++
	stats.Record(census.ctx, census.mCurrentStreams.M(census.activeStreams))
	census.lock.Unlock()
}

// StopStream ...
func StopStream(successful bool) {
	census.lock.Lock()
	census.activeStreams--
	stats.Record(census.ctx, census.mCurrentStreams.M(census.activeStreams))
	if successful {
		stats.Record(census.ctx, census.mSuccessfulStreams.M(1))
	}
	stats.Record(census.ctx, census.mTotalStreams.M(1))
	census.lock.Unlock()
}

// TotalStreams increase number of successful streams by one
func TotalStreams(successful bool) {
	if successful {
		stats.Record(census.ctx, census.mSuccessfulStreams.M(1))
	}
	stats.Record(census.ctx, census.mTotalStreams.M(1))
}

// StartupLatency ...
func StartupLatency(latency time.Duration) {
	stats.Record(census.ctx, census.mStartupLatency.M(latency.Seconds()))
}

// TranscodeLatency ...
func TranscodeLatency(latency time.Duration) {
	stats.Record(census.ctx, census.mTranscodeLatency.M(latency.Seconds()))
}
