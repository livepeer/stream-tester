package testers

import (
	"context"
	"fmt"
	"github.com/livepeer/stream-tester/internal/metrics"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/apis/livepeer"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
)

// HTTPLoadTester new version of streamer
// that uses HTTP ingest
// streams N streams simultaneously into B using HTTP ingest,
// repeats M times, calculates success rate and latecies
type HTTPLoadTester struct {
	ctx       context.Context
	cancel    func()
	streamers []*httpStreamer
	lapi      *livepeer.API
	skipFirst time.Duration
}

// NewHTTPLoadTester returns new HTTPLoadTester
func NewHTTPLoadTester(ctx context.Context, cancel context.CancelFunc, lapi *livepeer.API, skipFirst time.Duration) model.Streamer {
	return &HTTPLoadTester{ctx: ctx, cancel: cancel, lapi: lapi, skipFirst: skipFirst}
}

// Done returns channel that will be closed once streaming is done
func (hlt *HTTPLoadTester) Done() <-chan struct{} {
	return hlt.ctx.Done()
}

// Cancel closes done channel immidiately
func (hlt *HTTPLoadTester) Cancel() {
	hlt.cancel()
}

// Finished ...
func (hlt *HTTPLoadTester) Finished() bool {
	select {
	case <-hlt.ctx.Done():
		return true
	default:
		return false
	}
}

// Stop calls stop method on all the running streamers
func (hlt *HTTPLoadTester) Stop() {
	// sr.stopSignal = true
	// for _, str := range hlt.streamers {
	// 	str.Stop()
	// }
	hlt.Cancel()
}

// StartStreams start streaming
func (hlt *HTTPLoadTester) StartStreams(sourceFileName, bhost, rtmpPort, ohost, mediaPort string, simStreams, repeat uint, streamDuration time.Duration,
	notFinal, measureLatency, noBar bool, groupStartBy int, startDelayBetweenGroups, waitForTarget time.Duration) (string, error) {

	nMediaPort, err := strconv.Atoi(mediaPort)
	if err != nil {
		return "", err
	}
	showProgress := false
	baseManifestID := strings.ReplaceAll(path.Base(sourceFileName), ".", "") + "_" + randName()

	// httpIngestURLTemplate := fmt.Sprintf("http://%s:%d/live/%%s", bhost, nMediaPort)
	httpIngestURLTemplates := []string{fmt.Sprintf("http://%s:%d/live/%%s", bhost, nMediaPort)}
	if hlt.lapi != nil {
		broadcasters, err := hlt.lapi.Broadcasters()
		if err != nil {
			return "", err
		}
		if len(broadcasters) == 0 {
			return "", fmt.Errorf("empty list of broadcasters")
		}
		httpIngestURLTemplates = make([]string, 0, len(broadcasters))
		// httpIngestURLTemplate = fmt.Sprintf("%s/live/%%s", broadcasters[0])
		for _, b := range broadcasters {
			httpIngestURLTemplates = append(httpIngestURLTemplates, fmt.Sprintf("%s/live/%%s", b))
		}
	}

	go func() {
		for i := 0; i < int(repeat); i++ {
			if repeat > 1 {
				glog.Infof("Starting %d streaming session", i)
			}
			err := hlt.startStreams(baseManifestID, sourceFileName, i, httpIngestURLTemplates, simStreams, showProgress, measureLatency,
				streamDuration, groupStartBy, startDelayBetweenGroups, waitForTarget)
			if err != nil {
				glog.Fatal(err)
				return
			}
			select {
			case <-hlt.ctx.Done():
				return
			default:
			}
		}
		hlt.Cancel()
		// messenger.SendMessage(sr.AnalyzeFormatted(true))
		stats, _ := hlt.Stats("")
		messenger.SendCodeMessage(stats.FormatForConsole())
		messenger.SendCodeMessage(stats.FormatErrorsForConsole())
		// fmt.Printf(sr.AnalyzeFormatted(false))
		// if !notFinal {
		// 	hlt.Cancel()
		// }
	}()
	return baseManifestID, nil
}

func (hlt *HTTPLoadTester) startStreams(baseManifestID, sourceFileName string, repeatNum int, httpIngestURLTemplates []string, simStreams uint, showProgress,
	measureLatency bool, stopAfter time.Duration, groupStartBy int, startDelayBetweenGroups, waitForTarget time.Duration) error {

	// fmt.Printf("Starting streaming %s to %s:%d, number of streams is %d\n", sourceFileName, host, nRtmpPort, simStreams)
	pm := ""
	if hlt.lapi != nil {
		pm = fmt.Sprintf(" with profiles '%v'", hlt.lapi.DefaultPresets())
	}
	msg := fmt.Sprintf("Starting (%s) HTTP streaming %s to %v, number of streams is %d %s\n",
		model.Version, sourceFileName, httpIngestURLTemplates, simStreams, pm)
	messenger.SendMessage(msg)
	fmt.Println(msg)
	// httpIngestURLTemplate := "http://%s:%d/live/%s"
	var wg sync.WaitGroup
	for i := 0; i < int(simStreams); i++ {
		if groupStartBy > 0 && i%groupStartBy == 0 {
			startDelayBetweenGroups = 2*time.Second + time.Duration(rand.Intn(4000))*time.Millisecond
			glog.Infof("Waiting for %s before starting stream %d", startDelayBetweenGroups, i)
			time.Sleep(startDelayBetweenGroups)
		}
		manifestID := fmt.Sprintf("%s_%d_%d", baseManifestID, repeatNum, i)
		if hlt.lapi != nil {
			stream, err := hlt.lapi.CreateStream(livepeer.CreateStreamReq{Name: manifestID})
			if err != nil {
				glog.Errorf("Error creating stream using Livepeer API: %v", err)
				return err
			}
			manifestID = stream.ID
		}
		httpIngestURLTemplate := httpIngestURLTemplates[i%len(httpIngestURLTemplates)]
		httpIngestURL := fmt.Sprintf(httpIngestURLTemplate, manifestID)
		glog.Infof("HTTP ingest: %s", httpIngestURL)
		// var bar *uiprogress.Bar
		// if showProgress {
		// 	bar = uiprogress.AddBar(totalSegments).AppendCompleted().PrependElapsed()
		// }

		up := NewHTTPStreamer(hlt.ctx, measureLatency, baseManifestID)
		wg.Add(1)
		go func() {
			up.StartUpload(sourceFileName, httpIngestURL, manifestID, 0, waitForTarget, stopAfter, hlt.skipFirst)
			wg.Done()
		}()
		hlt.streamers = append(hlt.streamers, up)
	}
	glog.Info("Streams started, waiting.")
	wg.Wait()
	glog.Info("HTTP upload done.")
	return nil
}

/*
// StatsFormatted ...
func (hlt *HTTPLoadTester) StatsFormatted() string {
	return ""
}

// DownStatsFormatted ...
func (hlt *HTTPLoadTester) DownStatsFormatted() string {
	res := ""
	// for i, dl := range sr.downloaders {
	// 	if len(sr.downloaders) > 1 {
	// 		res += fmt.Sprintf("Downloads for stream %d\n", i)
	// 	}
	// 	res += dl.DownloadStatsFormatted()
	// }
	return res
}

// AnalyzeFormatted ...
func (hlt *HTTPLoadTester) AnalyzeFormatted(short bool) string {
	return ""
	// return sr.analyzeFormatted(short, true)
}
*/

// Stats ...
func (hlt *HTTPLoadTester) Stats(basedManifestID string) (*model.Stats, error) {
	stats := &model.Stats{
		RTMPstreams:         len(hlt.streamers),
		MediaStreams:        len(hlt.streamers),
		TotalSegmentsToSend: 0,
		Finished:            true,
		WowzaMode:           false,
	}
	transcodedLatencies := utils.LatenciesCalculator{}
	found := false
	for _, st := range hlt.streamers {
		if basedManifestID != "" && st.baseManifestID != basedManifestID {
			continue
		}
		found = true
		ds := st.stats()
		if stats.StartTime.IsZero() {
			stats.StartTime = ds.started
		} else if !ds.started.IsZero() && stats.StartTime.After(ds.started) {
			stats.StartTime = ds.started
		}
		stats.SentSegments += ds.triedToSend
		stats.DownloadedSegments += ds.success
		stats.FailedToDownloadSegments += ds.downloadFailures
		stats.BytesDownloaded += ds.bytes
		transcodedLatencies.Add(ds.latencies)
		if ds.errors != nil {
			if stats.Errors == nil {
				stats.Errors = make(map[string]int)
			}
			for e, i := range ds.errors {
				stats.Errors[e] = i
			}
		}
		if !ds.finished {
			stats.Finished = false
		}
	}
	if !found {
		return stats, model.ErroNotFound
	}
	transcodedLatencies.Prepare()
	avg, p50, p95, p99 := transcodedLatencies.Calc()
	stats.TranscodedLatencies = model.Latencies{Avg: avg, P50: p50, P95: p95, P99: p99}
	if stats.SentSegments > 0 {
		stats.SuccessRate = float64(stats.DownloadedSegments) / (float64(model.ProfilesNum) * float64(stats.SentSegments)) * 100
	}
	metrics.RecordSuccessRate(stats.SuccessRate)
	stats.ShouldHaveDownloadedSegments = model.ProfilesNum * stats.SentSegments
	stats.ProfilesNum = model.ProfilesNum
	stats.RawTranscodedLatencies = transcodedLatencies.Raw()
	return stats, nil
}
