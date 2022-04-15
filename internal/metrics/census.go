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
		nodeID      string
		ctx         context.Context
		kNodeID     tag.Key
		kManifestID tag.Key
		kTrigger    tag.Key
		kType       tag.Key
		//kRegion               tag.Key
		//kOrchestrator         tag.Key
		mCurrentStreams       *stats.Int64Measure
		mSuccessfulStreams    *stats.Int64Measure
		mTotalStreams         *stats.Int64Measure
		mSegmentsDownloading  *stats.Int64Measure
		mSegmentsToDownload   *stats.Int64Measure
		mSegmentsToDownloaded *stats.Int64Measure

		mMultistreamUsageMb  *stats.Float64Measure
		mMultistreamUsageMin *stats.Float64Measure

		mStartupLatency   *stats.Float64Measure
		mTranscodeLatency *stats.Float64Measure

		mTriggerDuration *stats.Float64Measure

		mAPIErrors  *stats.Int64Measure
		mAPILatency *stats.Float64Measure

		mConsulErrors  *stats.Int64Measure
		mConsulLatency *stats.Float64Measure

		mSuccessRate *stats.Float64Measure
		//mRoundTripTime *stats.Float64Measure

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
	Census.kManifestID = tag.MustNewKey("manifest_id")
	Census.kTrigger = tag.MustNewKey("trigger")
	Census.kType = tag.MustNewKey("type")
	//Census.kRegion = tag.MustNewKey("region")
	//Census.kOrchestrator = tag.MustNewKey("orchestrator")
	Census.ctx, err = tag.New(ctx, tag.Insert(Census.kNodeID, nodeID))
	if err != nil {
		glog.Fatal("Error creating context", err)
	}
	Census.mCurrentStreams = stats.Int64("current_streams", "Number of active streams", "tot")
	Census.mSuccessfulStreams = stats.Int64("successful_streams", "Number of streams resulted in success", "tot")
	Census.mTotalStreams = stats.Int64("total_streams", "Number of total streams", "tot")

	Census.mStartupLatency = stats.Float64("startup_latency", "Startup latency", "sec")
	Census.mTranscodeLatency = stats.Float64("transcode_latency", "Transcode latency", "sec")

	Census.mTriggerDuration = stats.Float64("trigger_duration", "Trigger duration", "sec")

	Census.mAPIErrors = stats.Int64("api_errors", "Number of API errors", "tot")
	Census.mAPILatency = stats.Float64("api_latency", "API latency", "sec")

	Census.mConsulErrors = stats.Int64("consul_errors", "Number of Consul errors", "tot")
	Census.mConsulLatency = stats.Float64("consul_latency", "Consul latency", "sec")

	Census.mSegmentsDownloading = stats.Int64("segments_downloading", "", "tot")
	Census.mSegmentsToDownload = stats.Int64("segments_to_download", "Number of segments queued for download", "tot")
	Census.mSegmentsToDownloaded = stats.Int64("segments_downloaded", "Number of segments downloaded", "tot")

	Census.mMultistreamUsageMb = stats.Float64("multistream_usage_megabytes", "Total number of megabytes multistreamed, or pushed, to external services", "megabyte")
	Census.mMultistreamUsageMin = stats.Float64("multistream_usage_minutes", "Total minutes multistreamed, or pushed, to external services", "min")

	Census.mSuccessRate = stats.Float64("success_rate", "Success rate of orch test for the given orchestrator", "per")
	//Census.mRoundTripTime = stats.Float64("round_trip_time", "Round trip time of orch test for the given orchestrator", "sec")

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
			Name:        "multistream_usage_megabytes",
			Measure:     Census.mMultistreamUsageMb,
			Description: "Total number of megabytes multistreamed, or pushed, to external services",
			TagKeys:     append([]tag.Key{Census.kManifestID}, baseTags...),
			Aggregation: view.Sum(),
		},
		{
			Name:        "multistream_usage_minutes",
			Measure:     Census.mMultistreamUsageMin,
			Description: "Total minutes multistreamed, or pushed, to external services",
			TagKeys:     append([]tag.Key{Census.kManifestID}, baseTags...),
			Aggregation: view.Sum(),
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
		{
			Name:        "trigger_duration",
			Measure:     Census.mTriggerDuration,
			Description: "Trigger duration",
			TagKeys:     append([]tag.Key{Census.kTrigger}, baseTags...),
			Aggregation: view.Distribution(0, 0.050, 0.100, .250, .500, .750, 1.000, 1.250, 1.500, 2.000, 2.500, 3.000, 3.500, 4.000, 4.500, 5.000, 10.000, 20.0, 30.0, 60.0),
		},
		{
			Name:        "api_errors",
			Measure:     Census.mAPIErrors,
			Description: "Number of API errors",
			TagKeys:     append([]tag.Key{Census.kType}, baseTags...),
			Aggregation: view.Count(),
		},
		{
			Name:        "api_latency",
			Measure:     Census.mAPILatency,
			Description: "API latency",
			TagKeys:     append([]tag.Key{Census.kType}, baseTags...),
			Aggregation: view.Distribution(0, 0.050, 0.100, .250, .500, .750, 1.000, 1.250, 1.500, 2.000, 2.500, 3.000, 3.500, 4.000, 4.500, 5.000, 10.000, 20.0, 30.0, 60.0),
		},
		{
			Name:        "consul_errors",
			Measure:     Census.mConsulErrors,
			Description: "Number of Consul errors",
			TagKeys:     append([]tag.Key{Census.kType}, baseTags...),
			Aggregation: view.Count(),
		},
		{
			Name:        "consul_latency",
			Measure:     Census.mConsulLatency,
			Description: "Consul latency",
			TagKeys:     append([]tag.Key{Census.kType}, baseTags...),
			Aggregation: view.Distribution(0, 0.050, 0.100, .250, .500, .750, 1.000, 1.250, 1.500, 2.000, 2.500, 3.000, 3.500, 4.000, 4.500, 5.000, 10.000, 20.0, 30.0, 60.0),
		},
		{
			Name:        "success_rate",
			Measure:     Census.mSuccessRate,
			Description: "Success rate",
			//TagKeys:     append([]tag.Key{Census.kRegion, Census.kOrchestrator}, baseTags...),
			TagKeys:     baseTags,
			Aggregation: view.LastValue(),
		},
		//{
		//	Name:        "round_trip_time",
		//	Measure:     Census.mRoundTripTime,
		//	Description: "Round trip time",
		//	TagKeys:     append([]tag.Key{Census.kRegion, Census.kOrchestrator}, baseTags...),
		//	Aggregation: view.LastValue(),
		//},
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

func IncMultistreamBytes(bytes int64, manifestID string) {
	ctx, err := tag.New(Census.ctx, tag.Insert(Census.kManifestID, manifestID))
	if err != nil {
		glog.Error("Error creating context", err)
		return
	}
	stats.Record(ctx, Census.mMultistreamUsageMb.M(float64(bytes)/1024/1024))
}

func IncMultistreamTime(mediaTime time.Duration, manifestID string) {
	ctx, err := tag.New(Census.ctx, tag.Insert(Census.kManifestID, manifestID))
	if err != nil {
		glog.Error("Error creating context", err)
		return
	}
	stats.Record(ctx, Census.mMultistreamUsageMin.M(mediaTime.Minutes()))
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

// TriggerDuration ...
func TriggerDuration(trigger string, duration time.Duration) {
	ctx, err := tag.New(Census.ctx, tag.Insert(Census.kTrigger, trigger))
	if err != nil {
		glog.Error("Error creating context", err)
		return
	}
	stats.Record(ctx, Census.mTriggerDuration.M(duration.Seconds()))
}

// ConsulRequest ...
func ConsulRequest(rType string, duration time.Duration, err error) {
	ctx, err := tag.New(Census.ctx, tag.Insert(Census.kType, rType))
	if err != nil {
		glog.Error("Error creating context", err)
		return
	}
	if err != nil {
		stats.Record(ctx, Census.mConsulErrors.M(1))
	} else {
		stats.Record(ctx, Census.mConsulLatency.M(duration.Seconds()))
	}
}

// APIRequest ...
func APIRequest(rType string, duration time.Duration, err error) {
	ctx, err := tag.New(Census.ctx, tag.Insert(Census.kType, rType))
	if err != nil {
		glog.Error("Error creating context", err)
		return
	}
	if err != nil {
		stats.Record(ctx, Census.mAPIErrors.M(1))
	} else {
		stats.Record(ctx, Census.mAPILatency.M(duration.Seconds()))
	}
}

//func PostStats(s *apiModels.Stats) {
//	m := []tag.Mutator{tag.Insert(Census.kRegion, s.Region), tag.Insert(Census.kOrchestrator, s.Orchestrator)}
//	if err := stats.RecordWithTags(Census.ctx, m, Census.mSuccessRate.M(s.SuccessRate)); err != nil {
//		glog.Errorf("Error recording metrics region=%s orchestrator=%s err=%q", s.Region, s.Orchestrator, err)
//	}
//	if err := stats.RecordWithTags(Census.ctx, m, Census.mRoundTripTime.M(s.RoundTripTime)); err != nil {
//		glog.Errorf("Error recording metrics region=%s orchestrator=%s err=%q", s.Region, s.Orchestrator, err)
//	}
//}

func RecordSuccessRate(successRate float64) {
	stats.Record(Census.ctx, Census.mSuccessRate.M(successRate))
}
