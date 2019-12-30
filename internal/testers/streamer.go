package testers

import (
	"fmt"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/gosuri/uiprogress"

	"github.com/livepeer/stream-tester/internal/messenger"
	"github.com/livepeer/stream-tester/internal/model"
	"github.com/livepeer/stream-tester/internal/utils"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// streamer streams multiple RTMP streams into broadcaster node,
// reads back source and transcoded segments and count them
// and calculates success rate from these numbers
type streamer struct {
	uploaders           map[string]map[string]*rtmpStreamer
	downloaders         map[string]map[string]*m3utester
	totalSegmentsToSend int
	stopSignal          bool
	eof                 chan struct{}
	wowzaMode           bool
}

// NewStreamer returns new streamer
func NewStreamer(wowzaMode bool) model.Streamer {
	return &streamer{
		eof:         make(chan struct{}),
		wowzaMode:   wowzaMode,
		uploaders:   make(map[string]map[string]*rtmpStreamer),
		downloaders: make(map[string]map[string]*m3utester),
	}
}

func (sr *streamer) Done() <-chan struct{} {
	return sr.eof
}

func (sr *streamer) Cancel() {
	close(sr.eof)
}

func (sr *streamer) Stop() {
	sr.stopSignal = true
	for _, ups := range sr.uploaders {
		for _, up := range ups {
			up.file.Close()
		}
	}
}

func (sr *streamer) StartStreams(sourceFileName, host, rtmpPort, mediaPort string, simStreams, repeat uint, streamDuration time.Duration,
	notFinal, measureLatency, noBar bool, groupStartBy int, startDelayBetweenGroups, waitForTarget time.Duration) (string, error) {

	showProgress := !noBar
	var segments int
	glog.Infof("Counting segments in %s", sourceFileName)
	segments = GetNumberOfSegments(sourceFileName, streamDuration)
	glog.Infof("Counted %d source segments (for %s duration)", segments, streamDuration)

	nRtmpPort, err := strconv.Atoi(rtmpPort)
	if err != nil {
		return "", err
	}
	nMediaPort, err := strconv.Atoi(mediaPort)
	if err != nil {
		return "", err
	}

	baseManifestID := strings.ReplaceAll(path.Base(sourceFileName), ".", "") + "_" + randName()

	var overallBar *uiprogress.Bar
	sr.totalSegmentsToSend = segments * int(simStreams) * int(repeat)
	if showProgress {
		uiprogress.Start()
		if repeat > 1 {
			overallBar = uiprogress.AddBar(sr.totalSegmentsToSend).AppendCompleted().PrependFunc(func(b *uiprogress.Bar) string {
				return "total: "
			})
			go func() {
				for {
					time.Sleep(5 * time.Second)
					st := sr.Stats(baseManifestID)
					overallBar.Set(st.SentSegments)
				}
			}()
		}
	}

	sr.stopSignal = false
	go func() {
		for i := 0; i < int(repeat); i++ {
			if repeat > 1 {
				glog.Infof("Starting %d streaming session", i)
			}
			err := sr.startStreams(baseManifestID, sourceFileName, host, nRtmpPort, nMediaPort, simStreams, showProgress, measureLatency,
				segments, groupStartBy, startDelayBetweenGroups, waitForTarget)
			if err != nil {
				glog.Fatal(err)
				return
			}
			if sr.stopSignal {
				break
			}
		}
		if !notFinal {
			close(sr.eof)
		}
	}()
	return baseManifestID, nil
}

func (sr *streamer) startStreams(baseManifestID string, sourceFileName, host string, nRtmpPort, nMediaPort int, simStreams uint, showProgress,
	measureLatency bool, totalSegments int, groupStartBy int, startDelayBetweenGroups, waitForTarget time.Duration) error {

	// fmt.Printf("Starting streaming %s to %s:%d, number of streams is %d\n", sourceFileName, host, nRtmpPort, simStreams)
	msg := fmt.Sprintf("Starting streaming %s to %s:%d, number of streams is %d\n", sourceFileName, host, nRtmpPort, simStreams)
	messenger.SendMessage(msg)
	fmt.Println(msg)
	rtmpURLTemplate := "rtmp://%s:%d/%s"
	mediaURLTemplate := "http://%s:%d/stream/%s.m3u8"
	if sr.wowzaMode {
		rtmpURLTemplate = "rtmp://%s:%d/live/%s"
		mediaURLTemplate = "http://%s:%d/live/ngrp:%s_all/playlist.m3u8"
	}

	if sr.uploaders[baseManifestID] == nil {
		sr.uploaders[baseManifestID] = make(map[string]*rtmpStreamer)
	}
	if sr.downloaders[baseManifestID] == nil {
		sr.downloaders[baseManifestID] = make(map[string]*m3utester)
	}

	var wg sync.WaitGroup
	started := make(chan interface{})
	go func() {
		for i := 0; i < int(simStreams); i++ {
			if groupStartBy > 0 && i%groupStartBy == 0 {
				startDelayBetweenGroups = 2*time.Second + time.Duration(rand.Intn(4000))*time.Millisecond
				glog.Infof("Waiting for %s before starting stream %d", startDelayBetweenGroups, i)
				time.Sleep(startDelayBetweenGroups)
			}
			manifestID := fmt.Sprintf("%s_%d", baseManifestID, i)
			rtmpURL := fmt.Sprintf(rtmpURLTemplate, host, nRtmpPort, manifestID)
			mediaURL := fmt.Sprintf(mediaURLTemplate, host, nMediaPort, manifestID)
			glog.Infof("RTMP: %s", rtmpURL)
			glog.Infof("MEDIA: %s", mediaURL)
			var bar *uiprogress.Bar
			if showProgress {
				bar = uiprogress.AddBar(totalSegments).AppendCompleted().PrependElapsed()
			}
			done := make(chan struct{})

			sentTimesMap := utils.NewSyncedTimesMap()

			up := newRtmpStreamer(rtmpURL, sourceFileName, sentTimesMap, bar, done, sr.wowzaMode, nil)
			wg.Add(1)
			go func() {
				up.startUpload(sourceFileName, rtmpURL, totalSegments, waitForTarget)
				wg.Done()
			}()
			sr.uploaders[baseManifestID][manifestID] = up
			down := newM3UTester(done, sentTimesMap, sr.wowzaMode, false, false)
			go findSkippedSegmentsNumber(up, down)
			sr.downloaders[baseManifestID][manifestID] = down
			down.Start(mediaURL)
			// put random delay before start of next stream
			// time.Sleep(time.Duration(rand.Intn(2)+2) * time.Second)
		}
		started <- nil
	}()
	<-started
	glog.Info("Streams started, waiting.")
	wg.Wait()
	glog.Info("RTMP upload done.")
	return nil
}

func findSkippedSegmentsNumber(rtmp *rtmpStreamer, mt *m3utester) {
	for {
		time.Sleep(3 * time.Second)
		if tm, ok := mt.GetFIrstSegmentTime(); ok {
			// check rtmp streamer
			// tm == 5
			// [2.5, 5,  7.5]
			for i, segTime := range rtmp.counter.segmentsStartTimes {
				if segTime >= tm {
					rtmp.skippedSegments = i + 1
					glog.V(model.VERBOSE).Infof("Found that %d segments was skipped, first segment time is %s, in rtmp %s",
						rtmp.skippedSegments, tm, segTime)
					return
				}
			}
			return
		}
	}
}

func (sr *streamer) AllStats() map[string]*model.Stats {
	all := make(map[string]*model.Stats)
	for mid, _ := range sr.uploaders {
		all[mid] = sr.Stats(mid)
	}
	return all
}

func (sr *streamer) Stats(baseManifestID string) *model.Stats {
	stats := &model.Stats{
		RTMPstreams:         len(sr.uploaders[baseManifestID]),
		MediaStreams:        len(sr.downloaders[baseManifestID]),
		TotalSegmentsToSend: sr.totalSegmentsToSend,
		Finished:            true,
		WowzaMode:           sr.wowzaMode,
		StartTime:           sr.uploaders[baseManifestID][baseManifestID+"_0"].started,
	}
	for _, rs := range sr.uploaders[baseManifestID] {
		// Broadcaster always skips at least first segment, and potentially more
		stats.SentSegments += rs.counter.segments - rs.skippedSegments
		if rs.connectionLost {
			stats.ConnectionLost++
		}
		if rs.active {
			stats.RTMPActiveStreams++
			stats.Finished = false
		}
	}
	sourceLatencies := utils.LatenciesCalculator{}
	transcodedLatencies := utils.LatenciesCalculator{}
	for _, mt := range sr.downloaders[baseManifestID] {
		ds := mt.stats()
		stats.DownloadedSegments += ds.success
		stats.FailedToDownloadSegments += ds.fail
		stats.BytesDownloaded += ds.bytes
		stats.Retries += ds.retries
		stats.Gaps += ds.gaps
		if mt.sentTimesMap != nil {
			for _, md := range mt.downloads {
				if md.source {
					sourceLatencies.Add(md.latencies)
				} else {
					transcodedLatencies.Add(md.latencies)
				}
			}
		}
	}
	sourceLatencies.Prepare()
	transcodedLatencies.Prepare()
	avg, p50, p95, p99 := sourceLatencies.Calc()
	stats.SourceLatencies = model.Latencies{Avg: avg, P50: p50, P95: p95, P99: p99}
	avg, p50, p95, p99 = transcodedLatencies.Calc()
	stats.TranscodedLatencies = model.Latencies{Avg: avg, P50: p50, P95: p95, P99: p99}
	if stats.SentSegments > 0 {
		stats.SuccessRate = float64(stats.DownloadedSegments) / (float64(model.ProfilesNum) * float64(stats.SentSegments)) * 100
	}
	stats.ShouldHaveDownloadedSegments = (model.ProfilesNum) * stats.SentSegments
	stats.ProfilesNum = model.ProfilesNum
	stats.RawSourceLatencies = sourceLatencies.Raw()
	stats.RawTranscodedLatencies = transcodedLatencies.Raw()
	// remove stats when finished
	if stats.Finished {
		defer sr.removeStats(baseManifestID)
	}
	return stats
}

func randName() string {
	x := make([]byte, 10, 10)
	for i := 0; i < len(x); i++ {
		x[i] = byte(rand.Uint32())
	}
	return fmt.Sprintf("%x", x)
}

func (sr *streamer) removeStats(baseManifestID string) {
	// keep the stats for an arbitrary set of time before removing for external availability purposes
	go func() {
		time.Sleep(1 * time.Minute)
		if _, ok := sr.downloaders[baseManifestID]; ok {
			delete(sr.downloaders, baseManifestID)
		}
		if _, ok := sr.uploaders[baseManifestID]; ok {
			delete(sr.uploaders, baseManifestID)
		}
	}()
}
