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
	uploaders           []*rtmpStreamer
	downloaders         []*m3utester
	totalSegmentsToSend int
	stopSignal          bool
	eof                 chan struct{}
	wowzaMode           bool
}

// NewStreamer returns new streamer
func NewStreamer(wowzaMode bool) model.Streamer {
	return &streamer{eof: make(chan struct{}), wowzaMode: wowzaMode}
}

func (sr *streamer) Done() <-chan struct{} {
	return sr.eof
}

func (sr *streamer) Cancel() {
	close(sr.eof)
}

func (sr *streamer) Stop() {
	sr.stopSignal = true
	for _, up := range sr.uploaders {
		up.file.Close()
	}
}

func (sr *streamer) StartStreams(sourceFileName, bhost, rtmpPort, ohost, mediaPort string, simStreams, repeat uint, streamDuration time.Duration,
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
					st := sr.Stats("")
					overallBar.Set(st.SentSegments)
				}
			}()
		}
	}

	sr.stopSignal = false
	baseManfistID := strings.ReplaceAll(path.Base(sourceFileName), ".", "") + "_" + randName()

	go func() {
		for i := 0; i < int(repeat); i++ {
			if repeat > 1 {
				glog.Infof("Starting %d streaming session", i)
			}
			err := sr.startStreams(baseManfistID, sourceFileName, i, bhost, ohost, nRtmpPort, nMediaPort, simStreams, showProgress, measureLatency,
				segments, groupStartBy, startDelayBetweenGroups, waitForTarget)
			if err != nil {
				glog.Fatal(err)
				return
			}
			if sr.stopSignal {
				break
			}
		}
		// messenger.SendMessage(sr.AnalyzeFormatted(true))
		// fmt.Printf(sr.AnalyzeFormatted(false))
		if !notFinal {
			close(sr.eof)
		}
	}()
	return baseManfistID, nil
}

func (sr *streamer) startStreams(baseManfistID, sourceFileName string, repeatNum int, bhost, mhost string, nRtmpPort, nMediaPort int, simStreams uint, showProgress,
	measureLatency bool, totalSegments int, groupStartBy int, startDelayBetweenGroups, waitForTarget time.Duration) error {

	// fmt.Printf("Starting streaming %s to %s:%d, number of streams is %d\n", sourceFileName, bhost, nRtmpPort, simStreams)
	msg := fmt.Sprintf("Starting streaming %s (repeat %d) to %s:%d, number of streams is %d, reading back from %s:%d\n", baseManfistID, repeatNum, bhost, nRtmpPort, simStreams,
		mhost, nMediaPort)
	messenger.SendMessage(msg)
	fmt.Println(msg)
	rtmpURLTemplate := "rtmp://%s:%d/%s"
	mediaURLTemplate := "http://%s:%d/stream/%s.m3u8"
	if sr.wowzaMode {
		rtmpURLTemplate = "rtmp://%s:%d/live/%s"
		mediaURLTemplate = "http://%s:%d/live/ngrp:%s_all/playlist.m3u8"
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
			manifesID := fmt.Sprintf("%s_%d_%d", baseManfistID, repeatNum, i)
			rtmpURL := fmt.Sprintf(rtmpURLTemplate, bhost, nRtmpPort, manifesID)
			mediaURL := fmt.Sprintf(mediaURLTemplate, mhost, nMediaPort, manifesID)
			glog.Infof("RTMP: %s", rtmpURL)
			glog.Infof("MEDIA: %s", mediaURL)
			var bar *uiprogress.Bar
			if showProgress {
				bar = uiprogress.AddBar(totalSegments).AppendCompleted().PrependElapsed()
			}
			done := make(chan struct{})
			var sentTimesMap *utils.SyncedTimesMap
			var segmentsMatcher *segmentsMatcher
			if measureLatency {
				// sentTimesMap = utils.NewSyncedTimesMap()
				segmentsMatcher = newsementsMatcher()
			}
			up := newRtmpStreamer(rtmpURL, sourceFileName, baseManfistID, sentTimesMap, bar, done, sr.wowzaMode, segmentsMatcher)
			wg.Add(1)
			go func() {
				up.StartUpload(sourceFileName, rtmpURL, totalSegments, waitForTarget)
				wg.Done()
			}()
			sr.uploaders = append(sr.uploaders, up)
			down := newM3UTester(done, sentTimesMap, sr.wowzaMode, false, false, segmentsMatcher)
			go findSkippedSegmentsNumber(up, down)
			sr.downloaders = append(sr.downloaders, down)
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

func (sr *streamer) StatsFormatted() string {
	r := ""
	for _, md := range sr.downloaders {
		r += md.StatsFormatted()
	}
	return r
}

func (sr *streamer) DownStatsFormatted() string {
	res := ""
	for i, dl := range sr.downloaders {
		if len(sr.downloaders) > 1 {
			res += fmt.Sprintf("Downloads for stream %d\n", i)
		}
		res += dl.DownloadStatsFormatted()
	}
	return res
}

func (sr *streamer) AnalyzeFormatted(short bool) string {
	return sr.analyzeFormatted(short, true)
}

func (sr *streamer) analyzeFormatted(short, streamEnded bool) string {
	res := ""
	for i, dl := range sr.downloaders {
		if len(sr.downloaders) > 1 {
			res += fmt.Sprintf("Analisys for stream %d\n", i)
		}
		res += dl.AnalyzeFormatted(short)
	}
	stats := sr.Stats("")
	res += "Latencies: " + stats.TranscodedLatencies.String() + "\n"
	return res
}

func (sr *streamer) Stats(basedManifestID string) *model.Stats {
	stats := &model.Stats{
		RTMPstreams:         len(sr.uploaders),
		MediaStreams:        len(sr.downloaders),
		TotalSegmentsToSend: sr.totalSegmentsToSend,
		Finished:            true,
		WowzaMode:           sr.wowzaMode,
	}
	sourceLatencies := utils.LatenciesCalculator{}
	transcodedLatencies := utils.LatenciesCalculator{}
	for i, rs := range sr.uploaders {
		if basedManifestID != "" && rs.baseManifestID != basedManifestID {
			continue
		}
		if stats.StartTime.IsZero() {
			stats.StartTime = rs.started
		} else if !rs.started.IsZero() && stats.StartTime.After(rs.started) {
			stats.StartTime = rs.started
		}
		// Broadcaster always skips at lest first segment, and potentially more
		stats.SentSegments += rs.counter.segments - rs.skippedSegments
		if rs.connectionLost {
			stats.ConnectionLost++
		}
		if rs.active {
			stats.RTMPActiveStreams++
			stats.Finished = false
		}
		mt := sr.downloaders[i]
		ds := mt.stats()
		stats.DownloadedSegments += ds.success
		stats.FailedToDownloadSegments += ds.fail
		stats.BytesDownloaded += ds.bytes
		stats.Retries += ds.retries
		stats.Gaps += ds.gaps
		if mt.segmentsMatcher != nil {
			for _, md := range mt.downloads {
				md.mu.Lock()
				lcp := make([]time.Duration, len(md.latencies), len(md.latencies))
				copy(lcp, md.latencies)
				if md.source {
					sourceLatencies.Add(lcp)
				} else {
					transcodedLatencies.Add(lcp)
					lcp := make([]time.Duration, len(md.latenciesPerStream))
					copy(lcp, md.latenciesPerStream)
					stats.RawTranscodeLatenciesPerStream = append(stats.RawTranscodeLatenciesPerStream, lcp)
				}
				md.mu.Unlock()
			}
		}
	}
	// glog.Infof("=== source latencies: %+v", sourceLatencies)
	// glog.Infof("=== transcoded latencies: %+v", transcodedLatencies)
	sourceLatencies.Prepare()
	transcodedLatencies.Prepare()
	avg, p50, p95, p99 := sourceLatencies.Calc()
	stats.SourceLatencies = model.Latencies{Avg: avg, P50: p50, P95: p95, P99: p99}
	avg, p50, p95, p99 = transcodedLatencies.Calc()
	stats.TranscodedLatencies = model.Latencies{Avg: avg, P50: p50, P95: p95, P99: p99}
	if stats.SentSegments > 0 {
		stats.SuccessRate = float64(stats.DownloadedSegments) / ((float64(model.ProfilesNum) + 1) * float64(stats.SentSegments)) * 100
	}
	stats.ShouldHaveDownloadedSegments = (model.ProfilesNum + 1) * stats.SentSegments
	stats.ProfilesNum = model.ProfilesNum
	stats.RawSourceLatencies = sourceLatencies.Raw()
	stats.RawTranscodedLatencies = transcodedLatencies.Raw()
	return stats
}

func randName() string {
	x := make([]byte, 10, 10)
	for i := 0; i < len(x); i++ {
		x[i] = byte(rand.Uint32())
	}
	return fmt.Sprintf("%x", x)
}
