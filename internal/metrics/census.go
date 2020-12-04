package metrics

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
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
		nodeID                string
		ctx                   context.Context
		kNodeID               tag.Key
		mCurrentStreams       *stats.Int64Measure
		mSuccessfulStreams    *stats.Int64Measure
		mTotalStreams         *stats.Int64Measure
		mSegmentsDownloading  *stats.Int64Measure
		mSegmentsToDownload   *stats.Int64Measure
		mSegmentsToDownloaded *stats.Int64Measure

		mStartupLatency   *stats.Float64Measure
		mTranscodeLatency *stats.Float64Measure

		activeStreams int64
		lock          sync.Mutex

		segmentsDownloading int64
	}
)

// Exporter Prometheus exporter that handles `/metrics` endpoint
var Exporter *prometheus.Exporter

// Census global object
var Census censusMetricsCounter

// InitCensus init metrics
func InitCensus(nodeID, version, namespace string) {
	Census = censusMetricsCounter{
		nodeID: nodeID,
	}
	var err error
	ctx := context.Background()
	Census.kNodeID = tag.MustNewKey("node_id")
	Census.ctx, err = tag.New(ctx, tag.Insert(Census.kNodeID, nodeID))
	if err != nil {
		glog.Fatal("Error creating context", err)
	}
	Census.mCurrentStreams = stats.Int64("current_streams", "Number of active streams", "tot")
	Census.mSuccessfulStreams = stats.Int64("successful_streams", "Number of streams resulted in success", "tot")
	Census.mTotalStreams = stats.Int64("total_streams", "Number of total streams", "tot")

	Census.mStartupLatency = stats.Float64("startup_latency", "Startup latency", "sec")
	Census.mTranscodeLatency = stats.Float64("transcode_latency", "Transcode latency", "sec")

	Census.mSegmentsDownloading = stats.Int64("segments_downloading", "", "tot")
	Census.mSegmentsToDownload = stats.Int64("segments_to_download", "Number of segments queued for download", "tot")
	Census.mSegmentsToDownloaded = stats.Int64("segments_downloaded", "Number of segments downloaded", "tot")

	glog.Infof("Compiler: %s Arch %s OS %s Go version %s", runtime.Compiler, runtime.GOARCH, runtime.GOOS, runtime.Version())
	glog.Infof("Streamtester version: %s", version)
	glog.Infof("Node ID %s", nodeID)
	mVersions := stats.Int64("versions", "Version information.", "Num")
	compiler := tag.MustNewKey("compiler")
	goarch := tag.MustNewKey("goarch")
	goos := tag.MustNewKey("goos")
	goversion := tag.MustNewKey("goversion")
	streamtesterVersion := tag.MustNewKey("streamtesterversion")
	ctx, err = tag.New(ctx, tag.Insert(Census.kNodeID, nodeID),
		tag.Insert(compiler, runtime.Compiler), tag.Insert(goarch, runtime.GOARCH), tag.Insert(goos, runtime.GOOS),
		tag.Insert(goversion, runtime.Version()), tag.Insert(streamtesterVersion, version))
	if err != nil {
		glog.Fatal("Error creating tagged context", err)
	}
	baseTags := []tag.Key{Census.kNodeID}
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
			Measure:     Census.mCurrentStreams,
			Description: "Number of active streams",
			TagKeys:     baseTags,
			Aggregation: view.LastValue(),
		},
		{
			Name:        "segments_downloading",
			Measure:     Census.mSegmentsDownloading,
			Description: "Number of active downloading segments",
			TagKeys:     baseTags,
			Aggregation: view.LastValue(),
		},
		{
			Name:        "segments_to_download",
			Measure:     Census.mSegmentsToDownload,
			Description: "Number of segments queued for download",
			TagKeys:     baseTags,
			Aggregation: view.Count(),
		},
		{
			Name:        "segments_downloaded",
			Measure:     Census.mSegmentsToDownloaded,
			Description: "Number of segments downloaded",
			TagKeys:     baseTags,
			Aggregation: view.Count(),
		},
		{
			Name:        "successful_streams",
			Measure:     Census.mSuccessfulStreams,
			Description: "Number of streams resulted in success",
			TagKeys:     baseTags,
			Aggregation: view.Count(),
		},
		{
			Name:        "total_streams",
			Measure:     Census.mTotalStreams,
			Description: "Number of total streams",
			TagKeys:     baseTags,
			Aggregation: view.Count(),
		},
		{
			Name:        "startup_latency",
			Measure:     Census.mStartupLatency,
			Description: "Startup latency",
			TagKeys:     baseTags,
			Aggregation: view.Distribution(0, 0.050, 0.100, .250, .500, .750, 1.000, 1.250, 1.500, 2.000, 2.500, 3.000, 3.500, 4.000, 4.500, 5.000, 10.000, 20.0, 30.0),
		},
		{
			Name:        "transcode_latency",
			Measure:     Census.mTranscodeLatency,
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
		Namespace: namespace,
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
	stats.Record(Census.ctx, Census.mCurrentStreams.M(0))
}

func (cs *censusMetricsCounter) IncSegmentsToDownload() {
	stats.Record(cs.ctx, cs.mSegmentsToDownload.M(1))
}

func (cs *censusMetricsCounter) IncSegmentsDownloading() int64 {
	std := atomic.AddInt64(&cs.segmentsDownloading, 1)
	stats.Record(cs.ctx, cs.mSegmentsDownloading.M(std))
	return std
}

func (cs *censusMetricsCounter) SegmentDownloaded() int64 {
	std := atomic.AddInt64(&cs.segmentsDownloading, -1)
	stats.Record(cs.ctx, cs.mSegmentsDownloading.M(std))
	stats.Record(cs.ctx, cs.mSegmentsToDownloaded.M(1))
	return std
}

// CurrentStreams set number of active streams
// func CurrentStreams(cs int) {
// 	stats.Record(census.ctx, census.mCurrentStreams.M(int64(cs)))
// }

// StartStream ...
func StartStream() {
	as := atomic.AddInt64(&Census.activeStreams, 1)
	stats.Record(Census.ctx, Census.mCurrentStreams.M(as))
}

// StopStream ...
func StopStream(successful bool) {
	as := atomic.AddInt64(&Census.activeStreams, -1)
	stats.Record(Census.ctx, Census.mCurrentStreams.M(as))
	if successful {
		stats.Record(Census.ctx, Census.mSuccessfulStreams.M(1))
	}
	stats.Record(Census.ctx, Census.mTotalStreams.M(1))
}

// TotalStreams increase number of successful streams by one
func TotalStreams(successful bool) {
	if successful {
		stats.Record(Census.ctx, Census.mSuccessfulStreams.M(1))
	}
	stats.Record(Census.ctx, Census.mTotalStreams.M(1))
}

// StartupLatency ...
func StartupLatency(latency time.Duration) {
	stats.Record(Census.ctx, Census.mStartupLatency.M(latency.Seconds()))
}

// TranscodeLatency ...
func TranscodeLatency(latency time.Duration) {
	stats.Record(Census.ctx, Census.mTranscodeLatency.M(latency.Seconds()))
}
