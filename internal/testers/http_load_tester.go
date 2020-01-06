package testers

import (
	"context"
	"fmt"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/internal/messenger"
	"github.com/livepeer/stream-tester/internal/model"
	"github.com/livepeer/stream-tester/internal/utils"
)

// HTTPLoadTester new version of streamer
// that uses HTTP ingest
// streams N streams simultaneously into B using HTTP ingest,
// repeats M times, calculates success rate and latecies
type HTTPLoadTester struct {
	ctx       context.Context
	cancel    func()
	streamers []*httpStreamer
}

// NewHTTPLoadTester returns new HTTPLoadTester
func NewHTTPLoadTester() model.Streamer {
	ctx, cancel := context.WithCancel(context.Background())
	return &HTTPLoadTester{ctx: ctx, cancel: cancel}
}

// Done returns channel that will be closed once streaming is done
func (hlt *HTTPLoadTester) Done() <-chan struct{} {
	return hlt.ctx.Done()
}

// Cancel closes done channel immidiately
func (hlt *HTTPLoadTester) Cancel() {
	hlt.cancel()
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
	notFinal, measureLatency, noBar bool, groupStartBy int, startDelayBetweenGroups, waitForTarget time.Duration) error {

	nRtmpPort, err := strconv.Atoi(rtmpPort)
	if err != nil {
		return err
	}
	nMediaPort, err := strconv.Atoi(mediaPort)
	if err != nil {
		return err
	}
	showProgress := false

	go func() {
		for i := 0; i < int(repeat); i++ {
			if repeat > 1 {
				glog.Infof("Starting %d streaming session", i)
			}
			err := hlt.startStreams(sourceFileName, bhost, nRtmpPort, nMediaPort, simStreams, showProgress, measureLatency,
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
		// fmt.Printf(sr.AnalyzeFormatted(false))
		// if !notFinal {
		// 	hlt.Cancel()
		// }
	}()
	return nil
}

func (hlt *HTTPLoadTester) startStreams(sourceFileName, host string, nRtmpPort, nMediaPort int, simStreams uint, showProgress,
	measureLatency bool, stopAfter time.Duration, groupStartBy int, startDelayBetweenGroups, waitForTarget time.Duration) error {

	// fmt.Printf("Starting streaming %s to %s:%d, number of streams is %d\n", sourceFileName, host, nRtmpPort, simStreams)
	msg := fmt.Sprintf("Starting HTTP streaming %s to %s:%d, number of streams is %d\n", sourceFileName, host, nRtmpPort, simStreams)
	messenger.SendMessage(msg)
	fmt.Println(msg)
	httpIngestURLTemplate := "http://%s:%d/live/%s"
	baseManfistID := strings.ReplaceAll(path.Base(sourceFileName), ".", "") + "_" + randName()
	var wg sync.WaitGroup
	for i := 0; i < int(simStreams); i++ {
		if groupStartBy > 0 && i%groupStartBy == 0 {
			startDelayBetweenGroups = 2*time.Second + time.Duration(rand.Intn(4000))*time.Millisecond
			glog.Infof("Waiting for %s before starting stream %d", startDelayBetweenGroups, i)
			time.Sleep(startDelayBetweenGroups)
		}
		manifesID := fmt.Sprintf("%s_%d", baseManfistID, i)
		httpIngestURL := fmt.Sprintf(httpIngestURLTemplate, host, nMediaPort, manifesID)
		glog.Infof("HTTP ingest: %s", httpIngestURL)
		// var bar *uiprogress.Bar
		// if showProgress {
		// 	bar = uiprogress.AddBar(totalSegments).AppendCompleted().PrependElapsed()
		// }

		up := newHTTPtreamer(hlt.ctx, measureLatency)
		wg.Add(1)
		go func() {
			up.StartUpload(sourceFileName, httpIngestURL, manifesID, 0, waitForTarget, stopAfter)
			wg.Done()
		}()
		hlt.streamers = append(hlt.streamers, up)
	}
	glog.Info("Streams started, waiting.")
	wg.Wait()
	glog.Info("HTTP upload done.")
	return nil
}

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

// Stats ...
func (hlt *HTTPLoadTester) Stats() *model.Stats {
	stats := &model.Stats{
		RTMPstreams:         len(hlt.streamers),
		MediaStreams:        len(hlt.streamers),
		TotalSegmentsToSend: 0,
		Finished:            true,
		WowzaMode:           false,
	}
	transcodedLatencies := utils.LatenciesCalculator{}
	for _, st := range hlt.streamers {
		ds := st.stats()
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
	transcodedLatencies.Prepare()
	avg, p50, p95, p99 := transcodedLatencies.Calc()
	stats.TranscodedLatencies = model.Latencies{Avg: avg, P50: p50, P95: p95, P99: p99}
	if stats.SentSegments > 0 {
		stats.SuccessRate = float64(stats.DownloadedSegments) / (float64(model.ProfilesNum) * float64(stats.SentSegments)) * 100
	}
	return stats
}
